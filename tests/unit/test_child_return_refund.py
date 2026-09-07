"""Phase 4 S3 の unit テスト: Parent 終了後の Child 失敗による追加返金
（計画書 第8節「Parent失敗後にChild失敗が確定した場合」・N13・X08 の
非競合分・第9節）。

Done when の対応:
- N13: Parent 先行返金 90 → Child 失敗確定 → 追加返金 10 のみ発生し、
  返金累計が入金額 100 と一致する。返金済み Parent の Job 状態を復活させない
- Child 失敗の確定経路 3 つ（提出前 abandon / 失効 expire_due_leases /
  S2 の固定裁定 FAIL）すべてで追加返金が 1 件だけになること
- Child 成功なら返金累計は 90 のまま（追加返金なし）
- Child が DONE で送金障害中なら失敗返却を禁止する（第9節）
- 各 commit 後に ledger.assert_ledger_invariants と
  D = available + locked + paid + refunded を確認する

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
    """固定 Actor + Wallet seed 済みの test mode DB（Worker も 0 で seed）。

    裁定 FAIL 経路のテストが複数 Root を作るため、Requester は余裕を持って
    seed する（Root の入金額 D は Root ごとの fund Journal から導出される
    ため、Wallet の seed 額は保存則の検証に影響しない）。
    """
    with db.transaction(test_db.conn, immediate=True):
        insert_demo_participants(test_db.conn)
        for participant_id, balance in (
            (REQUESTER_ID, ROOT_BUDGET_UNITS * 4),
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
    Root を返す（(root_id, root_version_id, root_lease_id)）。"""
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
    """create_root → fund → A Claim → create_child(part-1, 10) → B Claim まで
    済みの Child を返す
    （(root_id, child_id, child_version_id, child_lease_id, root_lease_id)）。"""
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


def _fail_parent(demo_db, root_id, root_lease, *, suffix):
    """Parent（Root）を A の abandon で失敗させ、先行返金 90 を確定する。"""
    abandoned = service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=root_lease,
        operation_id=f"abandon:{suffix}",
    )
    assert abandoned.data["funds"]["amount_units"] == ROOT_BUDGET_UNITS - TEN
    ledger.assert_ledger_invariants(demo_db.conn, root_id)
    return abandoned


def _job_state(demo_db, job_id):
    return demo_db.conn.execute(
        "SELECT state FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()["state"]


def _payment_by_key(demo_db, business_key):
    return demo_db.conn.execute(
        "SELECT * FROM payment_operations WHERE business_key = ?",
        (business_key,),
    ).fetchone()


def _refund_payment_count(demo_db, root_id):
    return demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM payment_operations"
        " WHERE root_id = ? AND kind = 'refund'",
        (root_id,),
    ).fetchone()["c"]


def _assert_conservation(demo_db, root_id):
    """各 commit 後の保存則: D = available + locked + paid + refunded。
    assert_ledger_invariants が同値を含むが、S3 の完了条件として明示的に
    独立 assert する。"""
    ledger.assert_ledger_invariants(demo_db.conn, root_id)
    view = ledger.get_root_ledger_view(demo_db.conn, root_id)
    assert view.deposit_units == (
        view.available_units
        + view.locked_units
        + view.paid_units
        + view.refunded_units
    )


def _settle_and_assert_final(demo_db, root_id, *, expected_refunded, expected_paid):
    """送金まで完了させ、返金累計・支払い累計・最終 Escrow を照合する。"""
    service.process_payments(demo_db.conn)
    view = ledger.get_root_ledger_view(demo_db.conn, root_id)
    assert view.refunded_units == expected_refunded
    assert view.paid_units == expected_paid
    assert view.available_units == 0
    assert view.locked_units == 0  # 最終 Escrow=0
    # 返金累計は入金額を超えない
    assert view.refunded_units <= view.deposit_units
    _assert_conservation(demo_db, root_id)


# ---------------------------------------------------------------------------
# N13: 3 つの Child 失敗確定経路すべてで追加返金が 1 件だけ
# ---------------------------------------------------------------------------


def test_n13_route_abandon_single_additional_refund(demo_db):
    """N13（経路 1: 提出前 abandon）: Parent 先行返金 90 → B の abandon で
    Child 失敗確定 → 追加返金 10 のみ発生し、返金累計が入金額 100 と一致。
    返金済み Parent の Job 状態を復活させない。"""
    root_id, child_id, _v, child_lease, root_lease = _leased_child(
        demo_db, suffix="n13-ab"
    )
    _fail_parent(demo_db, root_id, root_lease, suffix="n13-ab-p")
    assert _payment_by_key(demo_db, f"refund:{root_id}:terminal") is not None

    returned = service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease,
        operation_id="abandon:n13-ab-c1",
    )
    assert returned.data["funds"]["combined"] is True
    assert returned.data["funds"]["amount_units"] == TEN
    _assert_conservation(demo_db, root_id)

    # 追加返金は 1 件だけ（terminal 1 件 + child-return 1 件の計 2 件）
    assert _refund_payment_count(demo_db, root_id) == 2
    child_return = _payment_by_key(
        demo_db, f"refund:{root_id}:child-return:{child_id}"
    )
    assert child_return is not None
    assert child_return["amount_units"] == TEN
    assert child_return["payee_id"] == REQUESTER_ID
    assert child_return["status"] == PaymentStatus.PENDING.value
    # 返金済み Parent の Job 状態を復活させない
    assert _job_state(demo_db, root_id) == JobState.FAILED.value
    assert _job_state(demo_db, child_id) == JobState.FAILED.value

    # abandon の再送（同一 operation_id）は replay。追加返金は増えない
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease,
        operation_id="abandon:n13-ab-c1",
    )
    assert _refund_payment_count(demo_db, root_id) == 2

    _settle_and_assert_final(
        demo_db, root_id, expected_refunded=ROOT_BUDGET_UNITS, expected_paid=0
    )


def test_n13_route_expiry_single_additional_refund(demo_db):
    """N13（経路 2: 失効 expire_due_leases）: Parent 先行返金 90 → Child の
    Lease 失効で Child 失敗確定 → 追加返金 10 のみ。Parent 状態は復活しない。"""
    root_id, child_id, _v, child_lease, root_lease = _leased_child(
        demo_db, suffix="n13-ex"
    )
    # Child の Lease だけ残して Parent を失敗させる
    _fail_parent(demo_db, root_id, root_lease, suffix="n13-ex-p")

    # Child の Lease 期限（t0+60秒）を越えて失効確定
    clock.set_test_now(demo_db.conn, TEST_T0_US + 61_000_000)
    results = service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)
    expired = [r for r in results if r.data["job_id"] == child_id]
    assert len(expired) == 1
    assert expired[0].data["funds"]["combined"] is True
    assert expired[0].data["funds"]["amount_units"] == TEN
    _assert_conservation(demo_db, root_id)

    # 追加返金は 1 件だけ
    assert _refund_payment_count(demo_db, root_id) == 2
    child_return = _payment_by_key(
        demo_db, f"refund:{root_id}:child-return:{child_id}"
    )
    assert child_return["amount_units"] == TEN
    # 失効処理の再実行（同じ期限を二重処理しない）
    assert service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID) == []
    assert _refund_payment_count(demo_db, root_id) == 2
    # Parent の Job 状態は復活しない
    assert _job_state(demo_db, root_id) == JobState.FAILED.value
    assert _job_state(demo_db, child_id) == JobState.EXPIRED.value
    del child_lease

    _settle_and_assert_final(
        demo_db, root_id, expected_refunded=ROOT_BUDGET_UNITS, expected_paid=0
    )


def test_n13_route_arbitration_fail_single_additional_refund(demo_db):
    """N13（経路 3: S2 の固定裁定 FAIL）: Parent 先行返金 90 → B の有効提出 →
    A の異議 → 裁定 FAIL で Child 失敗確定 → 追加返金 10 のみ。"""
    root_id, child_id, version_id, child_lease, root_lease = _leased_child(
        demo_db, suffix="n13-ar"
    )
    # B の有効提出
    submitted = service.submit(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease,
        version_id=version_id,
        artifact_json='{"sum": 6}',
        operation_id="submit:n13-ar-c1",
    )
    submission_id = submitted.data["submission_id"]
    # A の異議（DISPUTED）
    service.dispute(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=child_id,
        submission_id=submission_id,
        condition_id="sum",
        reason_code="CONDITION_MISMATCH",
        operation_id="dispute:n13-ar-c1",
    )
    # Parent 先行返金 90（Child は DISPUTED のため伝播しない）
    _fail_parent(demo_db, root_id, root_lease, suffix="n13-ar-p")
    assert _job_state(demo_db, child_id) == JobState.DISPUTED.value
    assert _refund_payment_count(demo_db, root_id) == 1

    # 裁定 FAIL を注入して確定
    verification.arbiter_result_override = lambda label: (
        verification.VerificationOutcome(
            result=verification.VerificationResult.FAIL,
            reason="ARTIFACT_VALUE_MISMATCH",
            canonical_artifact=None,
            artifact_hash=None,
            input_hash="0" * 64,
            evidence=ledger.canonical_json_dumps(
                {"injected": "arbiter_result_override", "result": "FAIL"}
            ),
        )
    )
    try:
        results = service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID)
    finally:
        verification.arbiter_result_override = None
    assert len(results) == 1
    assert results[0].data["resolution"] == "FAIL"
    assert _job_state(demo_db, child_id) == JobState.FAILED.value
    _assert_conservation(demo_db, root_id)

    # 追加返金は 1 件だけ
    assert _refund_payment_count(demo_db, root_id) == 2
    child_return = _payment_by_key(
        demo_db, f"refund:{root_id}:child-return:{child_id}"
    )
    assert child_return["amount_units"] == TEN
    # 裁定の再実行は冪等（追加返金は増えない）
    assert service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID) == []
    assert _refund_payment_count(demo_db, root_id) == 2
    # Parent の Job 状態は復活しない
    assert _job_state(demo_db, root_id) == JobState.FAILED.value

    _settle_and_assert_final(
        demo_db, root_id, expected_refunded=ROOT_BUDGET_UNITS, expected_paid=0
    )


# ---------------------------------------------------------------------------
# Child 成功なら追加返金なし（返金累計 90 のまま）
# ---------------------------------------------------------------------------


def test_child_success_after_parent_refund_keeps_refund_at_90(demo_db):
    """Child 成功（A の approve）なら返金累計は 90 のまま（追加返金なし）。
    B へ 10 が支払われ、最終 Escrow は 0。"""
    root_id, child_id, version_id, child_lease, root_lease = _leased_child(
        demo_db, suffix="ok"
    )
    submitted = service.submit(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease,
        version_id=version_id,
        artifact_json='{"sum": 6}',
        operation_id="submit:ok-c1",
    )
    # Parent 先行返金 90
    _fail_parent(demo_db, root_id, root_lease, suffix="ok-p")

    # Child 成功: A（Child の検収者）が approve
    service.approve(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=child_id,
        submission_id=submitted.data["submission_id"],
        operation_id="approve:ok-c1",
    )
    assert _job_state(demo_db, child_id) == JobState.DONE.value
    _assert_conservation(demo_db, root_id)

    # 追加返金なし（返金は terminal の 1 件だけ）
    assert _refund_payment_count(demo_db, root_id) == 1
    assert (
        _payment_by_key(demo_db, f"refund:{root_id}:child-return:{child_id}")
        is None
    )
    payout = _payment_by_key(demo_db, f"payout:{child_id}")
    assert payout is not None
    assert payout["amount_units"] == TEN

    _settle_and_assert_final(
        demo_db,
        root_id,
        expected_refunded=ROOT_BUDGET_UNITS - TEN,
        expected_paid=TEN,
    )


# ---------------------------------------------------------------------------
# Child DONE で送金障害中の失敗返却禁止（第9節）
# ---------------------------------------------------------------------------


def test_failure_return_forbidden_while_done_child_payment_pending(demo_db):
    """Child が DONE（A の approve）で PaymentOperation が PENDING の間、
    Parent 終端後の失敗返却（追加返金）も INVALID_STATE で拒否される。
    S3 の実経路（approve → abandon/expiry の追加返金経路）で検証する。"""
    root_id, child_id, version_id, child_lease, root_lease = _leased_child(
        demo_db, suffix="pend"
    )
    submitted = service.submit(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease,
        version_id=version_id,
        artifact_json='{"sum": 6}',
        operation_id="submit:pend-c1",
    )
    service.approve(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=child_id,
        submission_id=submitted.data["submission_id"],
        operation_id="approve:pend-c1",
    )
    assert _job_state(demo_db, child_id) == JobState.DONE.value
    payment = _payment_by_key(demo_db, f"payout:{child_id}")
    assert payment["status"] == PaymentStatus.PENDING.value
    del child_lease

    # Parent 先行返金 90
    _fail_parent(demo_db, root_id, root_lease, suffix="pend-p")

    # 送金を RETRYABLE にする（送金障害を記録）
    class _FailingEscrow(ledger.MockEscrow):
        def transfer(self, conn, operation_id, payload):
            raise ledger.PaymentFailedError("rail down")

    failed = service.process_single_payment(
        demo_db.conn,
        operation_id=payment["operation_id"],
        escrow=_FailingEscrow(),
    )
    assert failed.data["payment_status"] == PaymentStatus.RETRYABLE.value

    # Child は DONE かつ送金障害中。Child の Lease 期限を越えた失効処理は
    # 追加返金（失敗返却）を禁止される（Child は終端のため失効対象外だが、
    # 仮に終端化が試まっても資金移動は拒否されることを return 経路で確認）
    clock.set_test_now(demo_db.conn, TEST_T0_US + 61_000_000)
    assert service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID) == []
    # 失敗返却（追加返金）は INVALID_STATE
    with pytest.raises(OjpError) as exc_info:
        service.return_child_work(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            root_id=root_id,
            child_id=child_id,
            amount_units=TEN,
            operation_id="return:pend-c1",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    # 追加返金は存在しない
    assert (
        _payment_by_key(demo_db, f"refund:{root_id}:child-return:{child_id}")
        is None
    )
    _assert_conservation(demo_db, root_id)
    # Child は DONE のまま
    assert _job_state(demo_db, child_id) == JobState.DONE.value
