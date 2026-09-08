"""Phase 3 S3 の unit テスト: Parent 終端時の Child 保護・追加返金・新 Child 禁止。

Done when の対応:
- 1（N03: Child を LEASED / SUBMITTED / DISPUTED にして Parent 失敗 → 状態と
  10 の拘束を保持、最初の返金は 90 だけ、各 Child は後続処理可能）
- 2（N13: Parent 先行返金 90 の後に Child 失敗 → 追加返金 10 のみで累計 100。
  refund:{root_id}:terminal と refund:{root_id}:child-return:{child_id} が
  別予約として 1 回ずつ。child_work → available → refund が 1 transaction）
- 3（Child DONE かつ送金障害中の失敗返却禁止: INVALID_STATE。計画書 第9節）
- 4（Parent 終端を Child へ伝播しない: Child が独立に終端し Child 側の資金
  だけが動く。Parent の Job 状態は復活しない）
- 5（Parent 状態別の新 Child 禁止: SUBMITTED / DISPUTED / DONE / FAILED /
  EXPIRED で create_child が拒否される。計画書 第8節の表）
- 7（X12: part-1 成功・B へ 10 支払済み、part-2 失敗返却、part-3 未提出の
  まま Parent 失敗 → 先行返金 80、part-3 の child_work=10 を保護。別 fixture
  で part-3 成功 B=20/返金80、失敗 B=10/返金90。A=0、最終 Escrow=0）
- 8（各 commit 後に assert_ledger_invariants が成立。返金累計が入金額を超えない）

補助: reserve_refundable_balance（計画書 第15節 Lifecycle）の状態別判定と、
reserve_parent_payout の Parent DONE ゲート（第8節の表 DONE 行）。
"""

from __future__ import annotations

import pytest

from ojp import clock, db, ledger, service
from ojp.domain import (
    ErrorCode,
    JobState,
    OjpError,
    PaymentStatus,
    SubcontractPolicy,
)
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    TEST_T0_US,
    default_subcontract_policy,
    force_job_state,
    insert_demo_participants,
    load_poc_catalog,
)

DEADLINE_US = TEST_T0_US + 86_400_000_000  # t0 + 1日
TEN = 10_000_000
REQUESTER_SEED_UNITS = ROOT_BUDGET_UNITS * 20  # 複数 Root を fund するための seed 額


@pytest.fixture
def demo_db(test_db):
    """固定 Actor + Wallet seed 済みの test mode DB（Worker も 0 で seed）。

    状態別に複数の Root を fund するテストがあるため、Requester の Wallet は
    複数 Root 分を seed する（Root の入金額 D は Root ごとの fund Journal
    から導出されるため、Wallet の seed 額は保存則の検証に影響しない）。
    """
    with db.transaction(test_db.conn, immediate=True):
        insert_demo_participants(test_db.conn)
        for participant_id, balance in (
            (REQUESTER_ID, REQUESTER_SEED_UNITS),
            (AGENT_A_ID, 0),
            (AGENT_B_ID, 0),
        ):
            ledger.seed_mock_wallet_for_demo(
                test_db.conn,
                participant_id=participant_id,
                asset="mock-USDC",
                balance_units=balance,
            )
    return test_db


def _leased_root(demo_db, *, policy=None, suffix="r1", deadline_us=DEADLINE_US):
    """create_root（第11節の固定カタログ）→ fund_root → A が Claim 済みの
    Root を返す（(root_id, root_version_id, parent_lease_id)）。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}",
        budget="100.000000",
        deadline_us=deadline_us,
        subcontract_policy=(
            policy if policy is not None else default_subcontract_policy()
        ),
        task_catalog=catalog,
        operation_id=f"create:{suffix}",
    )
    root_id = created.data["job_id"]
    service.fund_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        root_id=root_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id=f"fund:{suffix}",
    )
    claimed = service.claim(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        expected_version_id=created.data["version_id"],
        operation_id=f"claim:{suffix}",
    )
    return root_id, created.data["version_id"], claimed.data["lease_id"]


def _create_child(demo_db, root_id, lease_id, task_key, op_id, *, budget=TEN):
    return service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=lease_id,
        task_key=task_key,
        budget_units=budget,
        deadline_us=DEADLINE_US,
        operation_id=op_id,
    )


def _claim_child(demo_db, child_id, version_id, op_id, *, actor_id=AGENT_B_ID):
    return service.claim(
        demo_db.conn,
        actor_id=actor_id,
        job_id=child_id,
        expected_version_id=version_id,
        operation_id=op_id,
    )


def _assemble_submitted(demo_db, job_id, lease_id, version_id, suffix):
    """有効提出後（SUBMITTED）を直接組み立てる。

    ※ Phase 4 で submit / dispute が入るまでの暫定的な組み立てである。
    submissions 行と jobs.state の直接 INSERT / UPDATE で再現し、submit
    実装後は本物の経路へ置き換える。
    """
    with db.transaction(demo_db.conn, immediate=True):
        demo_db.conn.execute(
            "INSERT INTO submissions (id, job_id, lease_id, version_id,"
            " artifact_json, artifact_hash, verification_result,"
            " verification_evidence, submitted_at_us, valid_at_us,"
            " review_due_at_us)"
            " VALUES (?, ?, ?, ?, '{}', 'hash', 'PASS', '{}', ?, ?, ?)",
            (
                f"submission:{suffix}",
                job_id,
                lease_id,
                version_id,
                TEST_T0_US,
                TEST_T0_US,
                TEST_T0_US + 30_000_000,
            ),
        )
        demo_db.conn.execute(
            "UPDATE leases SET closed_reason = 'submitted' WHERE id = ?",
            (lease_id,),
        )
        demo_db.conn.execute(
            "UPDATE jobs SET state = 'SUBMITTED', active_lease_id = NULL,"
            " row_version = row_version + 1 WHERE id = ?",
            (job_id,),
        )


def _view(demo_db, root_id):
    return ledger.get_root_ledger_view(demo_db.conn, root_id)


def _wallet(demo_db, participant_id):
    row = demo_db.conn.execute(
        "SELECT balance_units FROM mock_wallets WHERE participant_id = ?",
        (participant_id,),
    ).fetchone()
    return int(row["balance_units"])


def _job_state(demo_db, job_id):
    return demo_db.conn.execute(
        "SELECT state FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()["state"]


def _payment_ops(demo_db, root_id):
    return demo_db.conn.execute(
        "SELECT * FROM payment_operations WHERE root_id = ?"
        " ORDER BY business_key",
        (root_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# reserve_refundable_balance（計画書 第15節 Lifecycle・第8節の表）
# ---------------------------------------------------------------------------


def test_rrb_on_failed_root_reuses_terminal_reservation(demo_db):
    """Parent 失敗（abandon）で既に確定した返金予約を、Lifecycle が再確定しない。

    終端化の transaction で作られた refund:{root_id}:terminal の
    PaymentOperation をそのまま返し（reused）、予約は 1 回だけ。
    """
    root_id, _v, lease_id = _leased_root(demo_db, suffix="rrb-f")
    _create_child(demo_db, root_id, lease_id, "part-1", "create:rrb-f-c1")
    abandoned = service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=lease_id,
        operation_id="abandon:rrb-f",
    )
    payment_op_id = abandoned.data["funds"]["payment_operation_id"]
    assert abandoned.data["funds"]["amount_units"] == ROOT_BUDGET_UNITS - TEN

    result = service.reserve_refundable_balance(
        demo_db.conn, actor_id=SYSTEM_ID, root_id=root_id,
        operation_id="reserve:rrb-f-1",
    )
    assert result.data["action"] == "refund_reserved"
    assert result.data["reused"] is True
    assert result.data["payment_operation_id"] == payment_op_id
    assert result.data["amount_units"] == ROOT_BUDGET_UNITS - TEN
    assert len(_payment_ops(demo_db, root_id)) == 1  # 予約は 1 回だけ

    # 同一 operation_id の再送は replay
    replay = service.reserve_refundable_balance(
        demo_db.conn, actor_id=SYSTEM_ID, root_id=root_id,
        operation_id="reserve:rrb-f-1",
    )
    assert replay.replayed is True
    assert len(_payment_ops(demo_db, root_id)) == 1
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_rrb_on_expired_root_reuses_terminal_reservation(demo_db):
    """Parent 失効（expire_due_leases）で確定した返金予約も同じく再利用される。"""
    root_id, _v, lease_id = _leased_root(demo_db, suffix="rrb-e")
    _create_child(demo_db, root_id, lease_id, "part-1", "create:rrb-e-c1")
    clock.set_test_now(demo_db.conn, TEST_T0_US + 61_000_000)
    results = service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)
    assert len(results) == 1
    payment_op_id = results[0].data["funds"]["payment_operation_id"]

    result = service.reserve_refundable_balance(
        demo_db.conn, actor_id=SYSTEM_ID, root_id=root_id,
        operation_id="reserve:rrb-e-1",
    )
    assert result.data["reused"] is True
    assert result.data["payment_operation_id"] == payment_op_id
    assert len(_payment_ops(demo_db, root_id)) == 1
    view = _view(demo_db, root_id)
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS - TEN
    assert view.locked_breakdown_units["child_work"] == TEN  # Child 保護
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_rrb_hold_states_do_not_move_funds(demo_db):
    """SUBMITTED / DISPUTED / DONE / OPEN / LEASED では返金予約を作らない。

    SUBMITTED / DISPUTED は Parent 判定のため保持（第8節の表）。DONE の
    A 向け支払い予約は Phase 4 の approve 経路（reserve_parent_payout）が
    確定する。OPEN / LEASED は available として保持する。
    """
    cases = {}
    for state_name in ("submitted", "disputed", "done", "leased"):
        suffix = f"rrb-hold-{state_name}"
        root_id, version_id, lease_id = _leased_root(demo_db, suffix=suffix)
        if state_name in ("submitted", "disputed", "done"):
            # ※ Phase 4 の submit / dispute / approve が入るまでの暫定的な
            #   組み立て（submissions 行と jobs.state の直接更新）
            _assemble_submitted(demo_db, root_id, lease_id, version_id, suffix)
            if state_name != "submitted":
                force_job_state(
                    demo_db.conn,
                    root_id,
                    JobState.DISPUTED
                    if state_name == "disputed"
                    else JobState.DONE,
                )
        cases[state_name] = root_id

    # OPEN（fund 済み未 Claim）の Root を組み立てる
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        title="root rrb-hold-open",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=catalog,
        operation_id="create:rrb-hold-open",
    )
    service.fund_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        root_id=created.data["job_id"],
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id="fund:rrb-hold-open",
    )
    cases["open"] = created.data["job_id"]

    for state_name, root_id in cases.items():
        result = service.reserve_refundable_balance(
            demo_db.conn, actor_id=SYSTEM_ID, root_id=root_id,
            operation_id=f"reserve:rrb-hold-{state_name}",
        )
        assert result.data["action"] == "hold", state_name
        assert _payment_ops(demo_db, root_id) == []
        view = _view(demo_db, root_id)
        assert view.available_units == ROOT_BUDGET_UNITS
        assert view.locked_breakdown_units["refund"] == 0
        ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_rrb_rejects_child_and_unknown_target(demo_db):
    """対象は Root のみ。Child や未知の Job は INVALID_TARGET。"""
    root_id, _v, lease_id = _leased_root(demo_db, suffix="rrb-t")
    child = _create_child(demo_db, root_id, lease_id, "part-1", "create:rrb-t-c1")
    with pytest.raises(OjpError) as exc_info:
        service.reserve_refundable_balance(
            demo_db.conn, actor_id=SYSTEM_ID, root_id=child.data["child_id"],
            operation_id="reserve:rrb-t-c1",
        )
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value
    with pytest.raises(OjpError) as exc_info:
        service.reserve_refundable_balance(
            demo_db.conn, actor_id=SYSTEM_ID, root_id="job:nonexistent",
            operation_id="reserve:rrb-t-x",
        )
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value


def test_rrb_creates_reservation_when_terminal_without_one(demo_db):
    """終端時に返金予約が無かった Root へ、Lifecycle が予約を確定する。

    ※ 実際の終端経路（失効・abandon）は終端化と同じ transaction で予約を
    確定するため、この「予約の無い終端 Root」は force_job_state による
    暫定的な組み立てで再現する。Lifecycle が backstop として同じ予約を
    作れることを確認する。
    """
    root_id, _v, _lease_id = _leased_root(demo_db, suffix="rrb-create")
    force_job_state(demo_db.conn, root_id, JobState.FAILED)
    result = service.reserve_refundable_balance(
        demo_db.conn, actor_id=SYSTEM_ID, root_id=root_id,
        operation_id="reserve:rrb-create-1",
    )
    assert result.data["action"] == "refund_reserved"
    assert "reused" not in result.data
    assert result.data["amount_units"] == ROOT_BUDGET_UNITS
    ops = _payment_ops(demo_db, root_id)
    assert len(ops) == 1
    assert ops[0]["business_key"] == f"refund:{root_id}:terminal"
    assert ops[0]["amount_units"] == ROOT_BUDGET_UNITS
    assert ops[0]["payee_id"] == REQUESTER_ID
    assert ops[0]["status"] == PaymentStatus.PENDING.value

    # 別 operation_id の再実行は既存の予約を返す（二重返金しない）
    again = service.reserve_refundable_balance(
        demo_db.conn, actor_id=SYSTEM_ID, root_id=root_id,
        operation_id="reserve:rrb-create-2",
    )
    assert again.data["reused"] is True
    assert len(_payment_ops(demo_db, root_id)) == 1
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_rrb_no_op_when_available_is_zero(demo_db):
    """終端時 available が 0 なら返金予約を作らない（正常な no-op。第16節）。"""
    root_id, _v, lease_id = _leased_root(demo_db, suffix="rrb-zero")
    # available を全額 parent_payout へ（Phase 2 の台帳プリミティブで組み立て。
    # locked は返金予約へ移せないため、終端時の返金は 0 の no-op になる）
    with db.transaction(demo_db.conn, immediate=True):
        ledger.parent_approval_in_tx(
            demo_db.conn,
            root_id=root_id,
            amount_units=ROOT_BUDGET_UNITS,
            operation_id="payout:rrb-zero-all",
            now_us=TEST_T0_US,
        )
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=lease_id,
        operation_id="abandon:rrb-zero",
    )
    result = service.reserve_refundable_balance(
        demo_db.conn, actor_id=SYSTEM_ID, root_id=root_id,
        operation_id="reserve:rrb-zero-1",
    )
    assert result.data["action"] == "no_op"
    assert result.data["no_op"] is True
    assert _payment_ops(demo_db, root_id) == []
    view = _view(demo_db, root_id)
    assert view.locked_breakdown_units["refund"] == 0
    assert view.locked_breakdown_units["parent_payout"] == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# reserve_refundable_balance の service 層認可: system 専用
# ---------------------------------------------------------------------------


def _rrb_audit_snapshot(conn):
    """available/refund・Journal・Operation・PaymentOperation の前後比較用
    スナップショット（認可拒否がいずれの表も変更しないことの検証）。"""

    def _rows(sql):
        return tuple(tuple(r) for r in conn.execute(sql).fetchall())

    return (
        _rows("SELECT * FROM budget_accounts ORDER BY id"),
        _rows("SELECT * FROM journal_transactions ORDER BY operation_id"),
        _rows("SELECT * FROM journal_entries ORDER BY operation_id, entry_no"),
        _rows("SELECT * FROM operations ORDER BY operation_id"),
        _rows("SELECT * FROM payment_operations ORDER BY operation_id"),
    )


def test_rrb_by_non_system_actor_is_forbidden(demo_db):
    """返金予約がまだない FAILED Root へ、登録済み非system Actor（human）の
    reserve_refundable_balance は FORBIDDEN。拒否後に available/refund・
    Journal・Operation・PaymentOperation は不変。system が同じ operation_id で
    後から正常に予約でき、Operation と派生 Payment Operation の監査 Actor が
    system になる。"""
    root_id, _v, _lease_id = _leased_root(demo_db, suffix="rrb-auth")
    # 返金予約がまだない FAILED Root（force_job_state による暫定的な組み立て。
    # 実際の終端経路は終端化 transaction で予約を確定するため）
    force_job_state(demo_db.conn, root_id, JobState.FAILED)
    before = _rrb_audit_snapshot(demo_db.conn)

    with pytest.raises(OjpError) as exc_info:
        service.reserve_refundable_balance(
            demo_db.conn, actor_id=REQUESTER_ID, root_id=root_id,
            operation_id="reserve:rrb-auth-1",
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    assert _rrb_audit_snapshot(demo_db.conn) == before  # 副副作用なし

    # 拒否に使った operation_id は DB に残らない。後から system が同じ ID で
    # 正常に予約できる
    result = service.reserve_refundable_balance(
        demo_db.conn, actor_id=SYSTEM_ID, root_id=root_id,
        operation_id="reserve:rrb-auth-1",
    )
    assert result.data["action"] == "refund_reserved"
    assert result.operation_id == "reserve:rrb-auth-1"
    assert result.replayed is False
    # 監査 Actor は system（外側 Operation と派生 Payment Operation 両方）
    outer = demo_db.conn.execute(
        "SELECT actor_id FROM operations WHERE operation_id = ?",
        ("reserve:rrb-auth-1",),
    ).fetchone()
    assert outer["actor_id"] == SYSTEM_ID
    payment_op_id = result.data["payment_operation_id"]
    payment_actor = demo_db.conn.execute(
        "SELECT actor_id FROM operations WHERE operation_id = ?",
        (payment_op_id,),
    ).fetchone()
    assert payment_actor["actor_id"] == SYSTEM_ID
    ops = _payment_ops(demo_db, root_id)
    assert [o["business_key"] for o in ops] == [f"refund:{root_id}:terminal"]
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_rrb_by_agent_actor_is_forbidden(demo_db):
    """登録済み agent でも FORBIDDEN（human だけでなく非system全般で拒否）。"""
    root_id, _v, _lease_id = _leased_root(demo_db, suffix="rrb-auth-agent")
    force_job_state(demo_db.conn, root_id, JobState.FAILED)
    before = _rrb_audit_snapshot(demo_db.conn)
    with pytest.raises(OjpError) as exc_info:
        service.reserve_refundable_balance(
            demo_db.conn, actor_id=AGENT_A_ID, root_id=root_id,
            operation_id="reserve:rrb-auth-agent-1",
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    assert _rrb_audit_snapshot(demo_db.conn) == before
    # 拒否後も system は同じ operation_id で予約できる
    result = service.reserve_refundable_balance(
        demo_db.conn, actor_id=SYSTEM_ID, root_id=root_id,
        operation_id="reserve:rrb-auth-agent-1",
    )
    assert result.data["action"] == "refund_reserved"


def test_rrb_by_unregistered_actor_is_forbidden(demo_db):
    """未登録 Actor でも FORBIDDEN（participants 行が無い場合も同様）。"""
    root_id, _v, _lease_id = _leased_root(demo_db, suffix="rrb-auth-unreg")
    force_job_state(demo_db.conn, root_id, JobState.FAILED)
    before = _rrb_audit_snapshot(demo_db.conn)
    with pytest.raises(OjpError) as exc_info:
        service.reserve_refundable_balance(
            demo_db.conn, actor_id="pt-unknown", root_id=root_id,
            operation_id="reserve:rrb-auth-unreg-1",
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    assert _rrb_audit_snapshot(demo_db.conn) == before


def test_rrb_non_system_rejected_even_for_hold_state(demo_db):
    """hold 状態（OPEN 等）の Root でも、非system Actor は予約・hold 判定の
    どちらも実行できない（system 専用 Lifecycle）。"""
    root_id, _v, _lease_id = _leased_root(demo_db, suffix="rrb-auth-open")
    before = _rrb_audit_snapshot(demo_db.conn)
    with pytest.raises(OjpError) as exc_info:
        service.reserve_refundable_balance(
            demo_db.conn, actor_id=REQUESTER_ID, root_id=root_id,
            operation_id="reserve:rrb-auth-open-1",
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    assert _rrb_audit_snapshot(demo_db.conn) == before


# ---------------------------------------------------------------------------
# Done when 1: N03。Child を LEASED / SUBMITTED / DISPUTED にして Parent 失敗
# ---------------------------------------------------------------------------


def test_n03_leased_child_preserved_when_parent_fails(demo_db):
    """N03（LEASED）: Parent 失敗しても Child の状態と 10 の拘束を保持し、
    最初の返金は 90 だけ。Child は後続処理（heartbeat / abandon）可能。"""
    root_id, _v, lease_id = _leased_root(demo_db, suffix="n03-l")
    child = _create_child(demo_db, root_id, lease_id, "part-1", "create:n03-l-c1")
    child_id = child.data["child_id"]
    b_claim = _claim_child(
        demo_db, child_id, child.data["version_id"], "claim:n03-l-c1"
    )

    service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=lease_id,
        operation_id="abandon:n03-l",
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # Child 状態と 10 の拘束を保持、最初の返金は 90 だけ
    assert _job_state(demo_db, child_id) == JobState.LEASED.value
    assert _job_state(demo_db, root_id) == JobState.FAILED.value
    view = _view(demo_db, root_id)
    assert view.locked_breakdown_units["child_work"] == TEN
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS - TEN
    assert view.available_units == 0
    ops = _payment_ops(demo_db, root_id)
    assert [o["business_key"] for o in ops] == [f"refund:{root_id}:terminal"]
    assert ops[0]["amount_units"] == ROOT_BUDGET_UNITS - TEN

    # Child は後続処理可能: heartbeat がそのまま使える（Parent 終端に影響されない）
    clock.set_test_now(demo_db.conn, TEST_T0_US + 20_000_000)
    beat = service.heartbeat(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=b_claim.data["lease_id"],
        generation=1,
        operation_id="heartbeat:n03-l-c1",
    )
    assert beat.data["expires_at"] == TEST_T0_US + 20_000_000 + 60_000_000

    # abandon も可能（追加返金は N13 のテストで検証）
    abandoned = service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=b_claim.data["lease_id"],
        operation_id="abandon:n03-l-c1",
    )
    assert abandoned.data["state"] == JobState.FAILED.value
    assert abandoned.data["funds"]["combined"] is True
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_n03_submitted_child_preserved_when_parent_fails(demo_db):
    """N03（SUBMITTED）: 提出済み Child の状態と 10 の拘束を保持。

    ※ SUBMITTED は Phase 4 の submit が入るまでの暫定的な組み立てである。
    """
    root_id, _v, lease_id = _leased_root(demo_db, suffix="n03-s")
    child = _create_child(demo_db, root_id, lease_id, "part-1", "create:n03-s-c1")
    child_id = child.data["child_id"]
    b_claim = _claim_child(
        demo_db, child_id, child.data["version_id"], "claim:n03-s-c1"
    )
    _assemble_submitted(
        demo_db, child_id, b_claim.data["lease_id"], child.data["version_id"],
        "n03-s-c1",
    )

    service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=lease_id,
        operation_id="abandon:n03-s",
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    assert _job_state(demo_db, child_id) == JobState.SUBMITTED.value
    view = _view(demo_db, root_id)
    assert view.locked_breakdown_units["child_work"] == TEN
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS - TEN
    ops = _payment_ops(demo_db, root_id)
    assert [o["business_key"] for o in ops] == [f"refund:{root_id}:terminal"]

    # 後続処理可能: 提出済み Child は支払いへ進める。
    # ※ approve は Phase 4 のため、その資金効果（child_work → child_payout と
    #   送金予約の確定）は Phase 2 の reserve_child_payout で構成する。
    service.reserve_child_payout(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        root_id=root_id,
        child_id=child_id,
        amount_units=TEN,
        payee_id=AGENT_B_ID,
        operation_id="payout:n03-s-c1",
    )
    view = _view(demo_db, root_id)
    assert view.locked_breakdown_units["child_work"] == 0
    assert view.locked_breakdown_units["child_payout"] == TEN
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS - TEN
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_n03_disputed_child_preserved_when_parent_fails(demo_db):
    """N03（DISPUTED）: 異議中の Child の状態と 10 の拘束を保持。

    ※ DISPUTED は Phase 4 の submit / dispute が入るまでの暫定的な組み立て
    である。後続処理（固定裁定）は Phase 4 の範囲であり、ここでは状態と
    拘束の保持を検証する。
    """
    root_id, _v, lease_id = _leased_root(demo_db, suffix="n03-d")
    child = _create_child(demo_db, root_id, lease_id, "part-1", "create:n03-d-c1")
    child_id = child.data["child_id"]
    b_claim = _claim_child(
        demo_db, child_id, child.data["version_id"], "claim:n03-d-c1"
    )
    _assemble_submitted(
        demo_db, child_id, b_claim.data["lease_id"], child.data["version_id"],
        "n03-d-c1",
    )
    force_job_state(demo_db.conn, child_id, JobState.DISPUTED)

    service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=lease_id,
        operation_id="abandon:n03-d",
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    assert _job_state(demo_db, child_id) == JobState.DISPUTED.value
    view = _view(demo_db, root_id)
    assert view.locked_breakdown_units["child_work"] == TEN
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS - TEN
    ops = _payment_ops(demo_db, root_id)
    assert [o["business_key"] for o in ops] == [f"refund:{root_id}:terminal"]
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 2: N13。Parent 先行返金の後に Child 失敗 → 追加返金
# ---------------------------------------------------------------------------


def test_n13_additional_refund_after_parent_refund(demo_db):
    """N13: Parent 先行返金 90 の後に Child 失敗 → 追加返金 10 のみで累計 100。

    - refund:{root_id}:terminal と refund:{root_id}:child-return:{child_id}
      が別予約として 1 回ずつだけ作られる
    - child_work → available → refund が 1 transaction（1 操作の Journal）
    - 中間の available は他の操作から使えない（Parent 終端なので新 Child 拒否）
    - 返金累計は入金額 100 と一致する（超えない）
    """
    root_id, _v, lease_id = _leased_root(demo_db, suffix="n13")
    child = _create_child(demo_db, root_id, lease_id, "part-1", "create:n13-c1")
    child_id = child.data["child_id"]
    b_claim = _claim_child(
        demo_db, child_id, child.data["version_id"], "claim:n13-c1"
    )

    # Parent 失敗 → 先行返金 90（PaymentOperation 付き）
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=lease_id,
        operation_id="abandon:n13",
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # Child 失敗が確定 → 追加返金 10（child_work → available → refund を
    # 1 transaction で記録）
    returned = service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=b_claim.data["lease_id"],
        operation_id="abandon:n13-c1",
    )
    assert returned.data["funds"]["combined"] is True
    assert returned.data["funds"]["amount_units"] == TEN
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # 別予約として 1 回ずつだけ
    ops = _payment_ops(demo_db, root_id)
    by_key = {o["business_key"]: o for o in ops}
    assert set(by_key) == {
        f"refund:{root_id}:terminal",
        f"refund:{root_id}:child-return:{child_id}",
    }
    assert by_key[f"refund:{root_id}:terminal"]["amount_units"] == (
        ROOT_BUDGET_UNITS - TEN
    )
    assert by_key[f"refund:{root_id}:child-return:{child_id}"]["amount_units"] == TEN
    assert by_key[f"refund:{root_id}:child-return:{child_id}"]["payee_id"] == (
        REQUESTER_ID
    )

    # child_work → available → refund は 1 操作の Journal（entry 順で検証）
    entries = demo_db.conn.execute(
        "SELECT e.entry_no, a.bucket, e.delta_units"
        " FROM journal_entries e JOIN budget_accounts a ON a.id = e.account_id"
        " WHERE e.operation_id = ? ORDER BY e.entry_no",
        ("abandon:n13-c1:funds",),
    ).fetchall()
    assert [(e["bucket"], e["delta_units"]) for e in entries] == [
        ("child_work", -TEN),
        ("available", TEN),
        ("available", -TEN),
        ("refund", TEN),
    ]
    txn = demo_db.conn.execute(
        "SELECT reason FROM journal_transactions WHERE operation_id = ?",
        ("abandon:n13-c1:funds",),
    ).fetchone()
    assert txn["reason"] == "child-return-after-parent-terminal"

    # 中間の available は他の操作から使えない: Parent 終端なので新 Child は拒否
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-2", "create:n13-c2")
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value

    # 返金済み Parent の Job 状態は復活しない
    assert _job_state(demo_db, root_id) == JobState.FAILED.value

    # abandon 再送は replay（追加返金は 1 回だけ）
    replay = service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=b_claim.data["lease_id"],
        operation_id="abandon:n13-c1",
    )
    assert replay.replayed is True
    assert len(_payment_ops(demo_db, root_id)) == 2

    # Parent 終端後の経路は return:{child_id} を消費しない
    # （追加返金は refund:{root_id}:child-return:{child_id} の PaymentOperation
    #   で一意にする。第16節の表）
    return_ops = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM operations WHERE business_key = ?",
        (f"return:{child_id}",),
    ).fetchone()["c"]
    assert return_ops == 0

    # settle: 返金累計は入金額 100 と一致する（超えない）
    results = service.process_payments(demo_db.conn, actor_id=SYSTEM_ID)
    assert len(results) == 2
    view = _view(demo_db, root_id)
    assert view.refunded_units == ROOT_BUDGET_UNITS
    assert view.paid_units == 0
    assert view.available_units == 0
    assert view.locked_units == 0
    # seed - 入金 100 + 返金 100 = seed（返金累計は入金額と一致）
    assert _wallet(demo_db, REQUESTER_ID) == REQUESTER_SEED_UNITS
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_n13_child_success_keeps_refund_at_90(demo_db):
    """N13 の別ケース: Parent 先行返金 90 の後に Child 成功なら累計 90 のまま。

    ※ approve は Phase 4 のため、Child 成功の資金効果（child_work →
    child_payout と送金予約の確定）は Phase 2 の reserve_child_payout で
    構成する。
    """
    root_id, _v, lease_id = _leased_root(demo_db, suffix="n13ok")
    child = _create_child(demo_db, root_id, lease_id, "part-1", "create:n13ok-c1")
    child_id = child.data["child_id"]
    _claim_child(demo_db, child_id, child.data["version_id"], "claim:n13ok-c1")

    service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=lease_id,
        operation_id="abandon:n13ok",
    )
    # Parent が終端でも Child の支払いは継続できる（child_work → child_payout）
    service.reserve_child_payout(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        root_id=root_id,
        child_id=child_id,
        amount_units=TEN,
        payee_id=AGENT_B_ID,
        operation_id="payout:n13ok-c1",
    )
    service.process_payments(demo_db.conn, actor_id=SYSTEM_ID)
    view = _view(demo_db, root_id)
    assert view.refunded_units == ROOT_BUDGET_UNITS - TEN  # 累計 90 のまま
    assert view.paid_units == TEN
    assert _wallet(demo_db, AGENT_B_ID) == TEN
    assert _wallet(demo_db, AGENT_A_ID) == 0
    assert view.locked_units == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 3: 送金障害中の Child の失敗返却禁止（計画書 第9節）
# ---------------------------------------------------------------------------


def test_failure_return_forbidden_while_done_child_payment_pending(demo_db):
    """Child が DONE で PaymentOperation が PENDING / RETRYABLE の間、
    失敗返却は INVALID_STATE で拒否される。

    ※ DONE は Phase 4 の approve が入るまでの暫定的な組み立て（jobs.state
    の直接更新）で再現する。送金障害は PENDING（未処理）と RETRYABLE
    （失敗記録済み）の両方で検証する。
    """
    root_id, _v, lease_id = _leased_root(demo_db, suffix="dw3")
    child = _create_child(demo_db, root_id, lease_id, "part-1", "create:dw3-c1")
    child_id = child.data["child_id"]
    _claim_child(demo_db, child_id, child.data["version_id"], "claim:dw3-c1")
    # 承認の資金効果（child_work → child_payout と送金予約）は Phase 2 の
    # reserve_child_payout で構成し、PaymentOperation は PENDING のままにする
    service.reserve_child_payout(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        root_id=root_id,
        child_id=child_id,
        amount_units=TEN,
        payee_id=AGENT_B_ID,
        operation_id="payout:dw3-c1",
    )
    force_job_state(demo_db.conn, child_id, JobState.DONE)

    # PENDING の間: 失敗返却は INVALID_STATE
    with pytest.raises(OjpError) as exc_info:
        service.return_child_work(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            root_id=root_id,
            child_id=child_id,
            amount_units=TEN,
            operation_id="return:dw3-c1-pending",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value

    # RETRYABLE にする（送金障害を記録）
    class _FailingEscrow(ledger.MockEscrow):
        def transfer(self, conn, operation_id, payload):
            raise ledger.PaymentFailedError("rail down")

    failed = service.process_single_payment(
        demo_db.conn, operation_id="payout:dw3-c1:payment", escrow=_FailingEscrow()
    )
    assert failed.data["payment_status"] == PaymentStatus.RETRYABLE.value

    # RETRYABLE の間も: 失敗返却は INVALID_STATE
    with pytest.raises(OjpError) as exc_info:
        service.return_child_work(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            root_id=root_id,
            child_id=child_id,
            amount_units=TEN,
            operation_id="return:dw3-c1-retryable",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value

    # 拒否の試行は資金・Job 状態・Operation を変えない（rollback）
    view = _view(demo_db, root_id)
    assert view.locked_breakdown_units["child_payout"] == TEN
    assert view.locked_breakdown_units["child_work"] == 0
    assert view.locked_breakdown_units["refund"] == 0
    assert _job_state(demo_db, child_id) == JobState.DONE.value
    assert (
        demo_db.conn.execute(
            "SELECT COUNT(*) AS c FROM operations"
            " WHERE operation_id LIKE 'return:dw3-c1-%'"
        ).fetchone()["c"]
        == 0
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 4: Parent 終端を Child へ伝播しない
# ---------------------------------------------------------------------------


def test_parent_expiry_does_not_propagate_and_child_expires_independently(demo_db):
    """Parent EXPIRED 後も Child の Lease・期限処理は独立に継続する。

    Child は独自の deadline・Lease を持ち、Parent 失効で自動 EXPIRED に
    ならない。Child が後で失効したときは Child 側の資金だけが動き、
    Parent の Job 状態は復活しない。
    """
    root_id, _v, lease_id = _leased_root(demo_db, suffix="dw4e")
    child = _create_child(demo_db, root_id, lease_id, "part-1", "create:dw4e-c1")
    child_id = child.data["child_id"]
    b_claim = _claim_child(
        demo_db, child_id, child.data["version_id"], "claim:dw4e-c1"
    )

    # Child の Lease だけ heartbeat で延ばし（t0+110秒）、Parent 側は延ばさない
    clock.set_test_now(demo_db.conn, TEST_T0_US + 50_000_000)
    service.heartbeat(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=b_claim.data["lease_id"],
        generation=1,
        operation_id="heartbeat:dw4e-c1",
    )

    # Parent だけ失効（t0+61秒: Parent Lease は t0+60秒で切れている）
    clock.set_test_now(demo_db.conn, TEST_T0_US + 61_000_000)
    results = service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)
    assert [r.data["job_id"] for r in results] == [root_id]
    assert _job_state(demo_db, root_id) == JobState.EXPIRED.value
    # Parent 終端を Child へ伝播しない
    assert _job_state(demo_db, child_id) == JobState.LEASED.value
    view = _view(demo_db, root_id)
    assert view.locked_breakdown_units["child_work"] == TEN
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS - TEN
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # Parent 失効後も Child の heartbeat は独立に使える
    beat = service.heartbeat(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=b_claim.data["lease_id"],
        generation=1,
        operation_id="heartbeat:dw4e-c1b",
    )
    assert beat.data["expires_at"] == TEST_T0_US + 61_000_000 + 60_000_000

    # Child の Lease が切れると Child は独立に終端し、Child 側の資金だけが動く
    clock.set_test_now(demo_db.conn, TEST_T0_US + 122_000_000)
    results = service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)
    assert [r.data["job_id"] for r in results] == [child_id]
    assert results[0].data["state"] == JobState.EXPIRED.value
    assert results[0].data["funds"]["combined"] is True
    assert _job_state(demo_db, child_id) == JobState.EXPIRED.value
    view = _view(demo_db, root_id)
    assert view.locked_breakdown_units["child_work"] == 0
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS
    # Parent の Job 状態は復活しない
    assert _job_state(demo_db, root_id) == JobState.EXPIRED.value
    # 再実行しても Parent は再処理されない（同じ期限を二重処理しない）
    assert service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID) == []
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_parent_expiry_then_child_abandon_moves_only_child_funds(demo_db):
    """Parent EXPIRED 後に Child が abandon で独立に終端し、Child 側の
    資金だけが追加返金へ動く。Parent の Job 状態は復活しない。"""
    root_id, _v, lease_id = _leased_root(demo_db, suffix="dw4a")
    child = _create_child(demo_db, root_id, lease_id, "part-1", "create:dw4a-c1")
    child_id = child.data["child_id"]
    b_claim = _claim_child(
        demo_db, child_id, child.data["version_id"], "claim:dw4a-c1"
    )
    clock.set_test_now(demo_db.conn, TEST_T0_US + 50_000_000)
    service.heartbeat(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=b_claim.data["lease_id"],
        generation=1,
        operation_id="heartbeat:dw4a-c1",
    )
    clock.set_test_now(demo_db.conn, TEST_T0_US + 61_000_000)
    service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)
    assert _job_state(demo_db, root_id) == JobState.EXPIRED.value
    assert _job_state(demo_db, child_id) == JobState.LEASED.value

    abandoned = service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=b_claim.data["lease_id"],
        operation_id="abandon:dw4a-c1",
    )
    assert abandoned.data["state"] == JobState.FAILED.value
    assert abandoned.data["funds"]["combined"] is True
    # Child 側の資金だけが動く（追加返金 10。Parent 側の予約は不変）
    view = _view(demo_db, root_id)
    assert view.locked_breakdown_units["child_work"] == 0
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS
    ops = _payment_ops(demo_db, root_id)
    assert {o["business_key"] for o in ops} == {
        f"refund:{root_id}:terminal",
        f"refund:{root_id}:child-return:{child_id}",
    }
    # Parent の Job 状態は復活しない
    assert _job_state(demo_db, root_id) == JobState.EXPIRED.value
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 5: Parent 状態別の新 Child 禁止（計画書 第8節の表）
# ---------------------------------------------------------------------------


def test_create_child_rejected_for_each_non_leased_parent_state(demo_db):
    """SUBMITTED / DISPUTED / DONE / FAILED / EXPIRED の Parent では
    create_child が拒否される（第8節の表「新 Child 禁止」）。

    SUBMITTED / DISPUTED / DONE は Phase 4 の submit / dispute / approve が
    入るまでの暫定的な組み立て（submissions 行と jobs.state の直接更新）で
    再現する。通常経路では提出・承認で Lease が閉じるため、拒否は
    LEASE_EXPIRED（閉じた Lease は復活しない）として現れる。
    """
    for state_name in ("submitted", "disputed", "done", "failed", "expired"):
        suffix = f"dw5-{state_name}"
        root_id, version_id, lease_id = _leased_root(demo_db, suffix=suffix)
        if state_name in ("submitted", "disputed", "done"):
            _assemble_submitted(demo_db, root_id, lease_id, version_id, suffix)
            if state_name != "submitted":
                force_job_state(
                    demo_db.conn,
                    root_id,
                    JobState.DISPUTED
                    if state_name == "disputed"
                    else JobState.DONE,
                )
        elif state_name == "failed":
            service.abandon(
                demo_db.conn,
                actor_id=AGENT_A_ID,
                job_id=root_id,
                lease_id=lease_id,
                operation_id=f"abandon:{suffix}",
            )
        else:  # expired
            clock.set_test_now(demo_db.conn, TEST_T0_US + 61_000_000)
            service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)
        with pytest.raises(OjpError) as exc_info:
            _create_child(demo_db, root_id, lease_id, "part-1", f"create:{suffix}-c1")
        assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value, state_name
        usage = ledger.get_subcontract_usage(demo_db.conn, root_id)
        assert usage.child_count == 0, state_name
        ledger.assert_ledger_invariants(demo_db.conn, root_id)
    # 注: expired ケースで test 時計は t0+61秒 へ進むが、後続ケースの
    # Root / Lease の期限は t0+1日 で十分未来のため影響しない（後退は拒否）。


def test_create_child_rejects_done_parent_with_open_lease(demo_db):
    """Parent が LEASED でなければ Lease が生きていても作成拒否（INVALID_STATE）。

    ※ 通常の DONE は Lease が閉じているため LEASE_EXPIRED になる。この
    テストは「Lease が開いたままの DONE」という人為的な組み立てで、
    第8節の表を直接判定する Parent 状態ガードを検証する。
    """
    root_id, _v, lease_id = _leased_root(demo_db, suffix="dw5-open")
    force_job_state(demo_db.conn, root_id, JobState.DONE)
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-1", "create:dw5-open-c1")
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    usage = ledger.get_subcontract_usage(demo_db.conn, root_id)
    assert usage.child_count == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# reserve_parent_payout の Parent DONE ゲート（第8節の表 DONE 行）
# ---------------------------------------------------------------------------


def test_reserve_parent_payout_requires_done_parent(demo_db):
    """available を parent_payout へ移すのは Parent DONE のときだけ許される。

    DONE 以外（OPEN / LEASED / SUBMITTED / FAILED / EXPIRED）は
    INVALID_STATE、Child を対象にすると INVALID_TARGET。
    """
    # DONE なら予約できる（※ DONE は暫定的な組み立て。approve は Phase 4）
    root_id, _v, _lease_id = _leased_root(demo_db, suffix="pp-done")
    force_job_state(demo_db.conn, root_id, JobState.DONE)
    ok = service.reserve_parent_payout(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        root_id=root_id,
        amount_units=TEN,
        payee_id=AGENT_A_ID,
        operation_id="payout:pp-done",
    )
    assert ok.data["amount_units"] == TEN
    assert _view(demo_db, root_id).locked_breakdown_units["parent_payout"] == TEN
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # DONE 以外は INVALID_STATE
    rejected = {}
    rejected["leased"] = _leased_root(demo_db, suffix="pp-leased")[0]
    rejected["submitted"] = None
    root_id_s, version_id_s, lease_id_s = _leased_root(demo_db, suffix="pp-sub")
    _assemble_submitted(demo_db, root_id_s, lease_id_s, version_id_s, "pp-sub")
    rejected["submitted"] = root_id_s
    root_id_f, _vf, lease_id_f = _leased_root(demo_db, suffix="pp-failed")
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id_f,
        lease_id=lease_id_f,
        operation_id="abandon:pp-failed",
    )
    rejected["failed"] = root_id_f
    rejected["expired"] = None
    root_id_e, _ve, _le = _leased_root(demo_db, suffix="pp-expired")
    clock.set_test_now(demo_db.conn, TEST_T0_US + 61_000_000)
    service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)
    rejected["expired"] = root_id_e
    # OPEN（fund 済み未 Claim）
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        title="root pp-open",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=catalog,
        operation_id="create:pp-open",
    )
    service.fund_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        root_id=created.data["job_id"],
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id="fund:pp-open",
    )
    rejected["open"] = created.data["job_id"]

    for state_name, target_root_id in rejected.items():
        with pytest.raises(OjpError) as exc_info:
            service.reserve_parent_payout(
                demo_db.conn,
                actor_id=REQUESTER_ID,
                root_id=target_root_id,
                amount_units=TEN,
                payee_id=AGENT_A_ID,
                operation_id=f"payout:pp-ng-{state_name}",
            )
        assert exc_info.value.code == ErrorCode.INVALID_STATE.value, state_name
        ledger.assert_ledger_invariants(demo_db.conn, target_root_id)

    # Child を対象にすると INVALID_TARGET
    root_id_c, _vc, lease_id_c = _leased_root(demo_db, suffix="pp-child")
    child = _create_child(demo_db, root_id_c, lease_id_c, "part-1", "create:pp-c1")
    with pytest.raises(OjpError) as exc_info:
        service.reserve_parent_payout(
            demo_db.conn,
            actor_id=REQUESTER_ID,
            root_id=child.data["child_id"],
            amount_units=TEN,
            payee_id=AGENT_A_ID,
            operation_id="payout:pp-ng-child",
        )
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value


# ---------------------------------------------------------------------------
# Done when 7: X12
# ---------------------------------------------------------------------------


def _x12_world(demo_db):
    """X12 の共通組み立て: part-1 成功・B へ 10 支払済み、part-2 失敗返却、
    part-3 は B が未提出 LEASED のまま Parent 失敗（先行返金 80）。

    ※ part-1 の「成功」は approve が Phase 4 のため、その資金効果
    （child_work → child_payout と送金予約の確定）は Phase 2 の
    reserve_child_payout + process_payments で構成する。
    """
    root_id, _v, lease_id = _leased_root(
        demo_db, policy=default_subcontract_policy(), suffix="x12"
    )
    children = {}
    for task_key in ("part-1", "part-2", "part-3"):
        created = _create_child(
            demo_db, root_id, lease_id, task_key, f"create:x12-{task_key}"
        )
        child_id = created.data["child_id"]
        b_claim = _claim_child(
            demo_db, child_id, created.data["version_id"], f"claim:x12-{task_key}"
        )
        children[task_key] = (child_id, b_claim.data["lease_id"])
        ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # part-1 成功・B へ 10 支払済み
    p1_id, _p1_lease = children["part-1"]
    service.reserve_child_payout(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        root_id=root_id,
        child_id=p1_id,
        amount_units=TEN,
        payee_id=AGENT_B_ID,
        operation_id="payout:x12-part-1",
    )
    settled = service.process_single_payment(
        demo_db.conn, operation_id="payout:x12-part-1:payment"
    )
    assert settled.data["payment_status"] == PaymentStatus.SUCCEEDED.value
    assert _wallet(demo_db, AGENT_B_ID) == TEN
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # part-2 失敗返却（Parent 生存中: child_work → available）
    p2_id, p2_lease = children["part-2"]
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=p2_id,
        lease_id=p2_lease,
        operation_id="abandon:x12-part-2",
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # Parent 失敗 → 先行返金 80（available = 100 - 30 + 10）
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=lease_id,
        operation_id="abandon:x12-parent",
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # 先行返金 80、part-3 の child_work=10 を保護
    p3_id, _p3_lease = children["part-3"]
    view = _view(demo_db, root_id)
    assert view.available_units == 0
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS - 2 * TEN
    assert view.locked_breakdown_units["child_work"] == TEN
    assert view.paid_units == TEN
    assert _job_state(demo_db, p3_id) == JobState.LEASED.value
    return root_id, children


def test_x12_part3_success(demo_db):
    """X12（part-3 成功）: B 累計 20・返金 80・A=0・最終 Escrow=0。

    ※ part-3 の「成功」も approve が Phase 4 のため Phase 2 の
    reserve_child_payout + process_payments で構成する。
    """
    root_id, children = _x12_world(demo_db)
    p3_id, _p3_lease = children["part-3"]

    # Parent が終端でも Child の支払いは継続できる（child_work → child_payout）
    service.reserve_child_payout(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        root_id=root_id,
        child_id=p3_id,
        amount_units=TEN,
        payee_id=AGENT_B_ID,
        operation_id="payout:x12-part-3",
    )
    service.process_payments(demo_db.conn, actor_id=SYSTEM_ID)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    view = _view(demo_db, root_id)
    assert view.paid_units == 2 * TEN  # B 累計 20
    assert view.refunded_units == ROOT_BUDGET_UNITS - 2 * TEN  # 返金 80
    assert view.available_units == 0
    assert view.locked_units == 0  # 最終 Escrow=0
    assert _wallet(demo_db, AGENT_B_ID) == 2 * TEN
    assert _wallet(demo_db, AGENT_A_ID) == 0  # A=0
    assert _job_state(demo_db, root_id) == JobState.FAILED.value


def test_x12_part3_failure(demo_db):
    """X12（part-3 失敗）: B 累計 10・返金 90・A=0・最終 Escrow=0。"""
    root_id, children = _x12_world(demo_db)
    p3_id, p3_lease = children["part-3"]

    # Parent 失敗後に Child 失敗が確定 → 追加返金 10
    returned = service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=p3_id,
        lease_id=p3_lease,
        operation_id="abandon:x12-part-3",
    )
    assert returned.data["funds"]["combined"] is True
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    service.process_payments(demo_db.conn, actor_id=SYSTEM_ID)
    view = _view(demo_db, root_id)
    assert view.paid_units == TEN  # B 累計 10
    assert view.refunded_units == ROOT_BUDGET_UNITS - TEN  # 返金 90
    assert view.available_units == 0
    assert view.locked_units == 0  # 最終 Escrow=0
    assert _wallet(demo_db, AGENT_B_ID) == TEN
    assert _wallet(demo_db, AGENT_A_ID) == 0  # A=0
    assert _job_state(demo_db, root_id) == JobState.FAILED.value
    assert view.refunded_units <= view.deposit_units  # 返金累計は入金額を超えない
    ledger.assert_ledger_invariants(demo_db.conn, root_id)
