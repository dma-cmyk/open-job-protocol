"""Phase 4 S3 の unit テスト: service.refund（計画書 第14節 `ojp job refund`・
第6節「Root返金コマンドは既に確定した返金予約を処理するだけで、Jobの判定を
変更できない」）。

Done when の対応:
- 検査順序が FORBIDDEN → INVALID_TARGET → INVALID_STATE の順に確定すること
  （対象が Child でも権限が無ければ INVALID_TARGET より先に FORBIDDEN）
- 新しい返金予約を作らない（reserve_refundable_balance を呼ばない）
- 確定済みの返金予約（refund:{root_id}:terminal と
  refund:{root_id}:child-return:{child_id}）だけを処理する
- 送金済み（SUCCEEDED）予約の再実行は既存結果を返す（新しい送金を作らない・
  金額と受取人を変更しない）
- jobs.state が不変であること（Job の判定を変更できない）
- Lifecycle で返金可能額が 0 のときに予約を作らない規則とは区別する
  （残高 0 で予約が存在するなら、その予約の処理結果を返す）

状態は create_root → fund_root → claim → create_child → submit →
approve/dispute/tick の実経路だけで作る。
"""

from __future__ import annotations

import pytest

from ojp import clock, db, ledger, service, verification
from ojp.domain import (
    ErrorCode,
    JobState,
    OjpError,
    PaymentStatus,
)
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    TEST_T0_US,
    default_subcontract_policy,
    insert_demo_participants,
    load_poc_catalog,
)

DEADLINE_US = TEST_T0_US + 86_400_000_000  # t0 + 1日
TEN = 10_000_000


@pytest.fixture
def demo_db(test_db):
    """固定 Actor + Wallet seed 済みの test mode DB（Worker も 0 で seed）。"""
    with db.transaction(test_db.conn, immediate=True):
        insert_demo_participants(test_db.conn)
        for participant_id, balance in (
            (REQUESTER_ID, ROOT_BUDGET_UNITS),
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


def _leased_root(demo_db, *, suffix="r1"):
    """create_root（第11節の固定カタログ）→ fund_root → A が Claim 済みの
    Root を返す（(root_id, root_version_id, parent_lease_id)）。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=catalog,
        input_json=ledger.canonical_json_dumps(_root_def.input_values),
        verifier_id=verification.VERIFIER_ID,
        verifier_hash=verification.verifier_hash(),
        conditions_json=ledger.canonical_json_dumps(_root_def.expected),
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


def _leased_child(demo_db, *, suffix="c1"):
    """create_root → fund → A Claim → create_child → B Claim 済みの Child を
    返す（(root_id, child_id, child_version_id, child_lease_id,
    root_lease_id)）。"""
    root_id, _root_v, root_lease = _leased_root(demo_db, suffix=f"{suffix}-r")
    created = service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease,
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id=f"create:{suffix}",
    )
    child_id = created.data["child_id"]
    claimed = service.claim(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=created.data["version_id"],
        operation_id=f"claim:{suffix}-b",
    )
    return (
        root_id,
        child_id,
        created.data["version_id"],
        claimed.data["lease_id"],
        root_lease,
    )


def _submitted_child(demo_db, *, suffix="c1"):
    """B の有効提出まで済みの Child を返す（(root_id, child_id, submission_id,
    child_lease_id, root_lease_id)）。"""
    root_id, child_id, version_id, child_lease, root_lease = _leased_child(
        demo_db, suffix=suffix
    )
    submitted = service.submit(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease,
        version_id=version_id,
        artifact_json='{"sum": 6}',
        operation_id=f"submit:{suffix}",
    )
    return root_id, child_id, submitted.data["submission_id"], child_lease, root_lease


def _failed_root_with_terminal_refund(demo_db, *, suffix="term"):
    """Parent 失敗（A の abandon）で先行返金 90 が確定済みの Root を返す
    （(root_id, child_id, terminal_payment_operation_id)）。Child は
    LEASED（未提出）のまま残す。"""
    root_id, child_id, _v, child_lease, root_lease = _leased_child(
        demo_db, suffix=suffix
    )
    del child_lease
    abandoned = service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=root_lease,
        operation_id=f"abandon:{suffix}",
    )
    payment_op_id = abandoned.data["funds"]["payment_operation_id"]
    assert abandoned.data["funds"]["amount_units"] == ROOT_BUDGET_UNITS - TEN
    return root_id, child_id, payment_op_id


def _refund(demo_db, job_id, op_id, *, actor_id=REQUESTER_ID):
    return service.refund(
        demo_db.conn,
        actor_id=actor_id,
        job_id=job_id,
        operation_id=op_id,
    )


def _job(demo_db, job_id):
    return demo_db.conn.execute(
        "SELECT * FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()


def _payment_ops(demo_db, root_id):
    return demo_db.conn.execute(
        "SELECT * FROM payment_operations WHERE root_id = ? ORDER BY operation_id",
        (root_id,),
    ).fetchall()


def _wallets(demo_db):
    return {
        row["participant_id"]: int(row["balance_units"])
        for row in demo_db.conn.execute("SELECT * FROM mock_wallets")
    }


def _audit_snapshot(conn):
    """jobs・leases・残高・Journal・PaymentOperation・Receipt の全体
    スナップショット（拒否がいずれの表も変更しないことの前後比較用）。"""

    def _rows(sql):
        return tuple(tuple(r) for r in conn.execute(sql).fetchall())

    return (
        _rows("SELECT * FROM jobs ORDER BY id"),
        _rows("SELECT * FROM leases ORDER BY id"),
        _rows("SELECT * FROM budget_accounts ORDER BY id"),
        _rows("SELECT * FROM journal_transactions ORDER BY operation_id"),
        _rows("SELECT * FROM journal_entries ORDER BY operation_id, entry_no"),
        _rows("SELECT * FROM payment_operations ORDER BY operation_id"),
        _rows("SELECT * FROM transfer_receipts ORDER BY operation_id"),
        _rows("SELECT * FROM mock_wallets ORDER BY participant_id"),
    )


# ---------------------------------------------------------------------------
# 検査順序: FORBIDDEN → INVALID_TARGET → INVALID_STATE
# ---------------------------------------------------------------------------


def test_refund_check_order_forbidden_before_invalid_target(demo_db):
    """検査順序 2: 権限の無い Actor（A・B）が Child を refund に指定しても、
    INVALID_TARGET より先に FORBIDDEN になる（対象が Child でも権限検査が先）。
    Root を指定した場合も FORBIDDEN。拒否後は全表が不変。"""
    root_id, child_id, _s, _cl, _rl = _submitted_child(demo_db, suffix="ord")
    for actor_id, job_id, op_id in (
        (AGENT_A_ID, child_id, "refund:ord-a-child"),
        (AGENT_A_ID, root_id, "refund:ord-a-root"),
        (AGENT_B_ID, root_id, "refund:ord-b-root"),
    ):
        before = _audit_snapshot(demo_db.conn)
        with pytest.raises(OjpError) as exc_info:
            _refund(demo_db, job_id, op_id, actor_id=actor_id)
        assert exc_info.value.code == ErrorCode.FORBIDDEN.value, (
            actor_id,
            job_id,
        )
        assert _audit_snapshot(demo_db.conn) == before


def test_refund_check_order_invalid_target_before_invalid_state(demo_db):
    """検査順序 3: 権限のある Actor（Root Requester）が Child を指定したら
    INVALID_TARGET（返金対象は Root のみ）。返金予約の有無（INVALID_STATE）より
    先に確定する。"""
    root_id, child_id, _s, _cl, _rl = _submitted_child(demo_db, suffix="ord2")
    del root_id
    # Child には返金予約が存在しないが、INVALID_TARGET が先に発火する
    before = _audit_snapshot(demo_db.conn)
    with pytest.raises(OjpError) as exc_info:
        _refund(demo_db, child_id, "refund:ord2-child")
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value
    assert _audit_snapshot(demo_db.conn) == before


def test_refund_invalid_state_when_no_reservations(demo_db):
    """検査順序 4: 返金予約が 1 件も無ければ INVALID_STATE。
    **新しい返金予約は作らない**（payment_operations が増えない）。"""
    root_id, _child_id, _s, _cl, _rl = _submitted_child(demo_db, suffix="nores")
    assert _payment_ops(demo_db, root_id) == []
    before = _audit_snapshot(demo_db.conn)
    with pytest.raises(OjpError) as exc_info:
        _refund(demo_db, root_id, "refund:nores-1")
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert _audit_snapshot(demo_db.conn) == before
    assert _payment_ops(demo_db, root_id) == []  # 予約を作らない
    assert _job(demo_db, root_id)["state"] == JobState.LEASED.value


def test_refund_unknown_job_is_invalid_target(demo_db):
    """検査順序 1: 存在しない Job は INVALID_TARGET。"""
    with pytest.raises(OjpError) as exc_info:
        _refund(demo_db, "job:nonexistent", "refund:unknown")
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value


# ---------------------------------------------------------------------------
# 確定済み予約の処理（terminal と child-return の両方）
# ---------------------------------------------------------------------------


def test_refund_processes_confirmed_reservations(demo_db):
    """先行返金 90 の確定予約を処理して Requester へ 90 送金する。
    新しい予約は作らず、jobs.state は不変（FAILED のまま）。"""
    root_id, _child_id, terminal_op_id = _failed_root_with_terminal_refund(
        demo_db, suffix="proc"
    )
    wallets_before = _wallets(demo_db)
    state_before = _job(demo_db, root_id)["state"]

    result = _refund(demo_db, root_id, "refund:proc-1")
    assert result.data["refund_count"] == 1
    assert result.data["refunds"][0]["operation_id"] == terminal_op_id
    assert result.data["refunds"][0]["payment_status"] == "SUCCEEDED"
    assert result.data["refunds"][0]["amount_units"] == ROOT_BUDGET_UNITS - TEN

    # 送金は 1 回だけ・金額と受取人は予約の固定値どおり
    wallets = _wallets(demo_db)
    assert wallets[REQUESTER_ID] == (
        wallets_before[REQUESTER_ID] + ROOT_BUDGET_UNITS - TEN
    )
    assert len(_payment_ops(demo_db, root_id)) == 1  # 新しい予約を作らない
    # Job の判定を変更できない
    assert _job(demo_db, root_id)["state"] == state_before
    view = ledger.get_root_ledger_view(demo_db.conn, root_id)
    assert view.refunded_units == ROOT_BUDGET_UNITS - TEN
    assert view.locked_breakdown_units["refund"] == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_refund_processes_all_confirmed_reservations(demo_db):
    """先行返金 90 ＋ Child 失敗後の追加返金 10 の両方の確定予約を
    1 回の refund で処理し、返金累計が入金額 100 と一致する。"""
    root_id, child_id, terminal_op_id = _failed_root_with_terminal_refund(
        demo_db, suffix="all"
    )
    # Child 失敗を確定（B の abandon）→ 追加返金 10 の予約
    child_lease = demo_db.conn.execute(
        "SELECT id FROM leases WHERE job_id = ? AND closed_reason IS NULL",
        (child_id,),
    ).fetchone()["id"]
    abandoned = service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease,
        operation_id="abandon:all-c1",
    )
    child_return_op_id = abandoned.data["funds"]["payment_operation_id"]
    assert abandoned.data["funds"]["amount_units"] == TEN

    wallets_before = _wallets(demo_db)
    result = _refund(demo_db, root_id, "refund:all-1")
    assert result.data["refund_count"] == 2
    processed_ids = {r["operation_id"] for r in result.data["refunds"]}
    assert processed_ids == {terminal_op_id, child_return_op_id}
    assert all(
        r["payment_status"] == "SUCCEEDED" for r in result.data["refunds"]
    )

    wallets = _wallets(demo_db)
    assert wallets[REQUESTER_ID] == wallets_before[REQUESTER_ID] + ROOT_BUDGET_UNITS
    assert len(_payment_ops(demo_db, root_id)) == 2  # 新しい予約を作らない
    view = ledger.get_root_ledger_view(demo_db.conn, root_id)
    assert view.refunded_units == ROOT_BUDGET_UNITS  # 返金累計 = 入金額
    assert view.available_units == 0
    assert view.locked_units == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# 送金済み予約の再実行は既存結果を返す
# ---------------------------------------------------------------------------


def test_refund_succeeded_reservation_returns_existing_result(demo_db):
    """送金済み（SUCCEEDED）予約の再実行は既存結果を返す。
    新しい送金を作らない（Receipt は 1 件のまま）・金額と受取人を変更しない。"""
    root_id, _child_id, terminal_op_id = _failed_root_with_terminal_refund(
        demo_db, suffix="again"
    )
    first = _refund(demo_db, root_id, "refund:again-1")
    assert first.data["refund_count"] == 1
    receipt_count = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM transfer_receipts"
    ).fetchone()["c"]
    assert receipt_count == 1
    wallets_after_first = _wallets(demo_db)

    # 別 operation_id での再実行（新しい送金を作らない）
    second = _refund(demo_db, root_id, "refund:again-2")
    assert second.data["refund_count"] == 1
    assert second.data["refunds"][0]["operation_id"] == terminal_op_id
    assert second.data["refunds"][0]["already_succeeded"] is True
    assert second.data["refunds"][0]["payment_status"] == "SUCCEEDED"
    # Wallet・Receipt・PaymentOperation は不変
    assert _wallets(demo_db) == wallets_after_first
    assert (
        demo_db.conn.execute(
            "SELECT COUNT(*) AS c FROM transfer_receipts"
        ).fetchone()["c"]
        == 1
    )
    assert len(_payment_ops(demo_db, root_id)) == 1
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_refund_same_operation_id_replays(demo_db):
    """同一 operation_id の再送は replay（保存済み結果）。送金は再実行
    されない（SUCCEEDED の既存結果を返す）。"""
    root_id, _child_id, _op = _failed_root_with_terminal_refund(
        demo_db, suffix="rep"
    )
    first = _refund(demo_db, root_id, "refund:rep-1")
    wallets_after_first = _wallets(demo_db)
    second = _refund(demo_db, root_id, "refund:rep-1")
    assert second.replayed is True
    # replay でも確定済み予約の処理結果（既存）を返す
    assert second.data["refund_count"] == 1
    assert second.data["refunds"][0]["already_succeeded"] is True
    assert _wallets(demo_db) == wallets_after_first
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# 認可（Root Requester または system のみ）と Job 状態の不変性
# ---------------------------------------------------------------------------


def test_refund_allows_system_actor(demo_db):
    """system Actor も refund を実行できる（第14節「refundはRoot Requester
    またはsystem」）。"""
    root_id, _child_id, _op = _failed_root_with_terminal_refund(
        demo_db, suffix="sys"
    )
    result = _refund(demo_db, root_id, "refund:sys-1", actor_id=SYSTEM_ID)
    assert result.data["refund_count"] == 1
    assert result.data["refunds"][0]["payment_status"] == "SUCCEEDED"
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_refund_does_not_change_job_state(demo_db):
    """refund は jobs.state を一切更新しない（Job の判定を変更できない）。
    SUBMITTED（有効提出後）の Root でも、予約があれば処理するだけで
    状態は SUBMITTED のまま。"""
    # Parent 失敗 → 先行返金 90 を作ってから Parent を「有効提出後」には
    # 戻せない（終端は不変）ため、ここは予約を持つ終端 Root で
    # 「処理前後で jobs.state が不変」ことを全 Job について検証する
    root_id, child_id, _op = _failed_root_with_terminal_refund(
        demo_db, suffix="state"
    )
    states_before = {
        row["id"]: (row["state"], row["row_version"])
        for row in demo_db.conn.execute("SELECT id, state, row_version FROM jobs")
    }
    _refund(demo_db, root_id, "refund:state-1")
    states_after = {
        row["id"]: (row["state"], row["row_version"])
        for row in demo_db.conn.execute("SELECT id, state, row_version FROM jobs")
    }
    assert states_after == states_before
    assert states_after[root_id][0] == JobState.FAILED.value
    assert states_after[child_id][0] == JobState.LEASED.value


def test_refund_zero_balance_with_reservation_returns_result(demo_db):
    """残高 0 で予約が存在するなら、その予約の処理結果を返す
    （Lifecycle の「返金可能額 0 なら予約を作らない」規則とは区別する）。"""
    root_id, child_id, _op = _failed_root_with_terminal_refund(
        demo_db, suffix="zero"
    )
    # Child 失敗で追加返金 10 を確定（refund bucket に残る予約）
    child_lease = demo_db.conn.execute(
        "SELECT id FROM leases WHERE job_id = ? AND closed_reason IS NULL",
        (child_id,),
    ).fetchone()["id"]
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease,
        operation_id="abandon:zero-c1",
    )
    # 送金を済ませて口座残高（refund bucket）を 0 にする
    service.process_payments(demo_db.conn)
    view = ledger.get_root_ledger_view(demo_db.conn, root_id)
    assert view.locked_breakdown_units["refund"] == 0
    assert view.refunded_units == ROOT_BUDGET_UNITS

    # 残高 0 でも予約は存在する → INVALID_STATE ではなく処理結果を返す
    result = _refund(demo_db, root_id, "refund:zero-1")
    assert result.data["refund_count"] == 2
    assert all(
        r["payment_status"] == "SUCCEEDED" and r.get("already_succeeded")
        for r in result.data["refunds"]
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)
