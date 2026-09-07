"""Phase 4 S2 の unit テスト: service.approve（計画書 第12節）。

Done when の対応:
- Requester無応答のとき承認・異議・自動承認で資金が正しく動く
- N05（同じChildを2回承認: Acceptance / PaymentOperation / Receipt は
  それぞれ1件だけ）
- 受取人は当該 Lease の Worker で、amount / payee_id を approve 引数に
  持たせない（渡すと TypeError）
- 非検収者・無関係Actor・Worker本人・Root Requester による Child approve は
  FORBIDDEN。Job・資金・Acceptance が不変
- LEASED / DISPUTED / 終端 Job の approve は INVALID_STATE。別 Job の
  submission_id は INVALID_TARGET
- 送金失敗でも jobs.state='DONE' は戻らない

状態は create_root → fund_root → claim → (create_child) → submit の
実経路だけで作る（force_job_state のような直接更新は使わない）。
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
    Requester は 200（Root 2 件分）持たせておき、複数 Root を作るテストに
    対応する。"""
    with db.transaction(test_db.conn, immediate=True):
        insert_demo_participants(test_db.conn)
        for participant_id, balance in (
            (REQUESTER_ID, ROOT_BUDGET_UNITS * 2),
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
    """create_root → fund_root → A が Claim 済みの Root を返す
    （(root_id, root_version_id, root_lease_id)）。"""
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


def _submitted_child(demo_db, *, suffix="c1"):
    """create_root → fund → A Claim → create_child → B Claim → B submit まで
    済みの Child を返す
    （(root_id, child_id, child_version_id, submission_id)）。"""
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
    submitted = service.submit(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=claimed.data["lease_id"],
        version_id=created.data["version_id"],
        artifact_json='{"sum": 6}',
        operation_id=f"submit:{suffix}",
    )
    return (
        root_id,
        child_id,
        created.data["version_id"],
        submitted.data["submission_id"],
    )


def _submitted_root(demo_db, *, suffix="root"):
    """Child なしで A が提出まで済みの Root を返す
    （(root_id, root_version_id, submission_id)）。"""
    root_id, root_v, root_lease = _leased_root(demo_db, suffix=suffix)
    submitted = service.submit(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=root_lease,
        version_id=root_v,
        artifact_json='{"sum": 15}',
        operation_id=f"submit:{suffix}",
    )
    return root_id, root_v, submitted.data["submission_id"]


def _approve(demo_db, job_id, submission_id, op_id, *, actor_id=AGENT_A_ID):
    return service.approve(
        demo_db.conn,
        actor_id=actor_id,
        job_id=job_id,
        submission_id=submission_id,
        operation_id=op_id,
    )


def _job(demo_db, job_id):
    return demo_db.conn.execute(
        "SELECT * FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()


def _buckets(demo_db, root_id):
    rows = demo_db.conn.execute(
        "SELECT owner_job_id, bucket, COALESCE(SUM(amount_units), 0) AS total"
        " FROM budget_accounts WHERE root_id = ?"
        " AND (source_key IS NULL OR source_key != ?)"
        " GROUP BY owner_job_id, bucket",
        (root_id, ledger.WALLET_LEDGER_SOURCE_KEY),
    ).fetchall()
    return {(r["owner_job_id"], r["bucket"]): int(r["total"]) for r in rows}


def _counts(demo_db, job_id):
    acceptances = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM acceptances WHERE job_id = ?", (job_id,)
    ).fetchone()["c"]
    payments = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM payment_operations WHERE job_id = ?",
        (job_id,),
    ).fetchone()["c"]
    receipts = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM transfer_receipts r JOIN payment_operations p"
        " ON p.operation_id = r.operation_id WHERE p.job_id = ?",
        (job_id,),
    ).fetchone()["c"]
    return acceptances, payments, receipts


def _wallets(demo_db):
    return {
        row["participant_id"]: int(row["balance_units"])
        for row in demo_db.conn.execute("SELECT * FROM mock_wallets")
    }


# ---------------------------------------------------------------------------
# 効果（Child / Parent 承認・N05・受取人）
# ---------------------------------------------------------------------------


def test_approve_child_moves_child_work_to_payout(demo_db):
    """Child 承認: DONE ＋ Acceptance APPROVED ＋ child_work -10 /
    child_payout +10 ＋ payout:{child_id} の PaymentOperation が PENDING。
    paid は増えない（APPROVED / DONE だけでは paid を増やさない）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    buckets_before = _buckets(demo_db, root_id)
    assert buckets_before[(child_id, "child_work")] == TEN
    assert (child_id, "child_payout") not in buckets_before

    result = _approve(demo_db, child_id, submission_id, "approve:c1")
    assert result.data["state"] == JobState.DONE.value
    assert result.data["decision"] == "APPROVED"
    assert result.data["amount_units"] == TEN
    assert result.data["payee_id"] == AGENT_B_ID

    job = _job(demo_db, child_id)
    assert job["state"] == JobState.DONE.value
    assert job["active_lease_id"] is None
    acceptance = demo_db.conn.execute(
        "SELECT * FROM acceptances WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert acceptance["decision"] == "APPROVED"
    assert acceptance["decided_by"] == AGENT_A_ID

    buckets = _buckets(demo_db, root_id)
    assert buckets[(child_id, "child_work")] == 0
    assert buckets[(child_id, "child_payout")] == TEN
    payment = demo_db.conn.execute(
        "SELECT * FROM payment_operations WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert payment["business_key"] == f"payout:{child_id}"
    assert payment["status"] == PaymentStatus.PENDING.value
    assert payment["payee_id"] == AGENT_B_ID
    assert payment["amount_units"] == TEN
    # APPROVED / DONE だけでは paid は増えない（Wallet も増えない）
    assert demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM transfer_receipts"
    ).fetchone()["c"] == 0
    assert _wallets(demo_db)[AGENT_B_ID] == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_approve_parent_moves_available_to_payout(demo_db):
    """Parent 承認（Child なし）: available -100 / parent_payout +100 ＋
    payout:{root_id}。Child がいる構成の残額 90 は N13/E01 側で扱う。"""
    root_id, _v, submission_id = _submitted_root(demo_db, suffix="p1")
    result = _approve(
        demo_db, root_id, submission_id, "approve:p1", actor_id=REQUESTER_ID
    )
    assert result.data["amount_units"] == ROOT_BUDGET_UNITS
    assert result.data["payee_id"] == AGENT_A_ID
    assert _job(demo_db, root_id)["state"] == JobState.DONE.value
    buckets = _buckets(demo_db, root_id)
    assert buckets[(root_id, "available")] == 0
    assert buckets[(root_id, "parent_payout")] == ROOT_BUDGET_UNITS
    payment = demo_db.conn.execute(
        "SELECT * FROM payment_operations WHERE job_id = ?", (root_id,)
    ).fetchone()
    assert payment["business_key"] == f"payout:{root_id}"
    assert payment["status"] == PaymentStatus.PENDING.value
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_approve_payee_is_the_lease_worker(demo_db):
    """受取人は submissions.lease_id の Worker（B）。A や Requester ではない。"""
    _root, child_id, _v, submission_id = _submitted_child(demo_db)
    result = _approve(demo_db, child_id, submission_id, "approve:payee")
    assert result.data["payee_id"] == AGENT_B_ID


@pytest.mark.parametrize(
    "override_kwarg",
    [
        {"amount": TEN},
        {"amount_units": TEN},
        {"payee_id": AGENT_B_ID},
    ],
)
def test_approve_rejects_amount_and_payee_arguments(demo_db, override_kwarg):
    """approve は amount / payee_id をシグネチャに持たず、渡すと TypeError
    （第12節「amount/payee を approve 引数に持たせない」）。"""
    _root, child_id, _v, submission_id = _submitted_child(demo_db)
    with pytest.raises(TypeError):
        service.approve(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            job_id=child_id,
            submission_id=submission_id,
            operation_id="approve:x",
            **override_kwarg,
        )
    assert _job(demo_db, child_id)["state"] == JobState.SUBMITTED.value


def test_double_approve_same_operation_id_replays(demo_db):
    """同一 operation_id の二重 approve は replay。Acceptance・
    PaymentOperation・Receipt は 1 件だけ（N05）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    first = _approve(demo_db, child_id, submission_id, "approve:duplicate")
    second = _approve(demo_db, child_id, submission_id, "approve:duplicate")
    assert second.replayed is True
    assert second.data == first.data
    acceptances, payments, receipts = _counts(demo_db, child_id)
    assert acceptances == 1
    assert payments == 1
    assert receipts == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_double_approve_different_operation_id_reuses_payment(demo_db):
    """別 operation_id の二重 approve は既存の業務効果を返して新しい送金を
    作らない（business_key=payout:{child_id}。N05「同時approve／自動approve
    で Acceptance と支払い予約が1組だけ」）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    first = _approve(demo_db, child_id, submission_id, "approve:first")
    buckets_before = _buckets(demo_db, root_id)
    wallets_before = _wallets(demo_db)
    second = _approve(demo_db, child_id, submission_id, "approve:second")
    assert second.replayed is True
    assert second.data["payment_operation_id"] == first.data["payment_operation_id"]
    acceptances, payments, receipts = _counts(demo_db, child_id)
    assert acceptances == 1
    assert payments == 1
    assert receipts == 0
    # 資金・Wallet は二度目で不変
    assert _buckets(demo_db, root_id) == buckets_before
    assert _wallets(demo_db) == wallets_before
    assert first.data["amount_units"] == TEN
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# 権限（非検収者・無関係 Actor・Worker 本人・Root Requester の Child approve）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "actor_id",
    [REQUESTER_ID, AGENT_B_ID, SYSTEM_ID],
)
def test_approve_child_rejects_non_requester(demo_db, actor_id):
    """非検収者（Root Requester・Worker 本人・無関係 Actor）が Child を
    approve すると FORBIDDEN。Job・資金・Acceptance が不変。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    buckets_before = _buckets(demo_db, root_id)
    with pytest.raises(OjpError) as exc_info:
        _approve(
            demo_db, child_id, submission_id, f"approve:forbid-{actor_id}",
            actor_id=actor_id,
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    assert _job(demo_db, child_id)["state"] == JobState.SUBMITTED.value
    assert _buckets(demo_db, root_id) == buckets_before
    acceptances, payments, _r = _counts(demo_db, child_id)
    assert acceptances == 0
    assert payments == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_root_requester_cannot_approve_child(demo_db):
    """Root Requester は Child の検収者ではない（Child の requester_id は A）。
    FORBIDDEN。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    with pytest.raises(OjpError) as exc_info:
        _approve(
            demo_db, child_id, submission_id, "approve:root-req",
            actor_id=REQUESTER_ID,
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    assert _job(demo_db, child_id)["state"] == JobState.SUBMITTED.value


# ---------------------------------------------------------------------------
# 状態検査（LEASED / DISPUTED / 終端 / 別 Job の submission_id）
# ---------------------------------------------------------------------------


def test_approve_leased_job_is_invalid_state(demo_db):
    """LEASED（提出前）の Job への approve は拒否される。R2 の前置検査
    順序（Job 存在 → Submission 所属 → 権限 → 状態）により、提出を持た
    ない Job への approve は INVALID_TARGET が先に確定する（存在しない
    Submission を渡しても同じ）。状態検査（INVALID_STATE）の到達は
    DISPUTED との競合テストで確認する。"""
    root_id, _root_v, _lease = _leased_root(demo_db, suffix="leased")
    with pytest.raises(OjpError) as exc_info:
        _approve(demo_db, root_id, "submission:unknown", "approve:leased",
                 actor_id=REQUESTER_ID)
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value


def test_approve_disputed_job_is_invalid_state(demo_db):
    """DISPUTED の approve は INVALID_STATE（approve と dispute の競合は
    先に commit した有効遷移が勝つ）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    service.dispute(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=child_id,
        submission_id=submission_id,
        condition_id="sum",
        reason_code="CONDITION_MISMATCH",
        operation_id="dispute:race",
    )
    with pytest.raises(OjpError) as exc_info:
        _approve(demo_db, child_id, submission_id, "approve:disputed")
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert _job(demo_db, child_id)["state"] == JobState.DISPUTED.value
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_approve_terminal_job_is_invalid_state(demo_db):
    """終端 Job（FAIL 系: FAILED）への approve は拒否される。
    （DONE への別 operation_id での再 approve は N05 の再利用経路が
    business_key=payout:{job_id} で返すため、ここは FAILED で検査する。
    R2 の前置検査順序により、提出を持たない終端 Job（提出前 abandon）へは
    INVALID_TARGET が先に確定する。提出済みで終端に至る Job の
    INVALID_STATE は DISPUTED との競合テストが担う）"""
    root_id, root_v, root_lease = _leased_root(demo_db, suffix="term")
    # A が abandon で Root を FAILED へ（提出前放棄の実経路）
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=root_lease,
        operation_id="abandon:term",
    )
    del root_v
    with pytest.raises(OjpError) as exc_info:
        _approve(demo_db, root_id, "submission:unknown", "approve:term-2",
                 actor_id=REQUESTER_ID)
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value


def test_approve_other_job_submission_is_invalid_target(demo_db):
    """別 Job の submission_id は INVALID_TARGET。
    両方とも SUBMITTED（状態検査を通る）にして所属検査だけを確かめる。"""
    root_a, _va, submission_a = _submitted_root(demo_db, suffix="other-a")
    root_b, _vb, _sb = _submitted_root(demo_db, suffix="other-b")
    del root_a
    with pytest.raises(OjpError) as exc_info:
        _approve(demo_db, root_b, submission_a, "approve:cross",
                 actor_id=REQUESTER_ID)
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value

# ---------------------------------------------------------------------------
# R2: 承認後の業務キー再利用経路も権限・対象・状態の前置検査を通る
# ---------------------------------------------------------------------------

def test_post_approval_other_actor_approve_is_forbidden(demo_db):
    """承認後に別 Actor が（新規 operation_id で）approve しても FORBIDDEN。
    業務キー再利用経路（reuse_existing_payment）が権限検査をすり抜けない。
    Acceptance・PaymentOperation・残高は不変。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    _approve(demo_db, child_id, submission_id, "approve:owner")
    buckets_before = _buckets(demo_db, root_id)
    wallets_before = _wallets(demo_db)
    with pytest.raises(OjpError) as exc_info:
        _approve(
            demo_db, child_id, submission_id, "approve:intruder",
            actor_id=REQUESTER_ID,
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    acceptances, payments, receipts = _counts(demo_db, child_id)
    assert acceptances == 1
    assert payments == 1
    assert receipts == 0
    assert _buckets(demo_db, root_id) == buckets_before
    assert _wallets(demo_db) == wallets_before
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

def test_post_approval_wrong_submission_id_is_invalid_target(demo_db):
    """承認後に同じ Requester が別 Job の submission_id を渡した新規
    operation_id の approve は INVALID_TARGET。Acceptance・PaymentOperation・
    残高は不変。"""
    root_a, child_a, _va, submission_a = _submitted_child(demo_db, suffix="wa-r")
    root_b, _vb, _sb, _submission_b = _submitted_child(demo_db, suffix="wb-r")
    _approve(demo_db, child_a, submission_a, "approve:wa")
    buckets_before = _buckets(demo_db, root_a)
    wallets_before = _wallets(demo_db)
    # child_a の Requester は A。別 Job（root_b 配下の Child）の submission
    # を渡しても INVALID_TARGET（状態検査は通る）
    other_child = demo_db.conn.execute(
        "SELECT id FROM jobs WHERE parent_id = ?", (root_b,)
    ).fetchone()["id"]
    other_submission = demo_db.conn.execute(
        "SELECT id FROM submissions WHERE job_id = ?",
        (other_child,),
    ).fetchone()["id"]
    with pytest.raises(OjpError) as exc_info:
        _approve(
            demo_db, child_a, other_submission, "approve:wa-wrong",
            actor_id=AGENT_A_ID,
        )
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value
    acceptances, payments, receipts = _counts(demo_db, child_a)
    assert acceptances == 1
    assert payments == 1
    assert receipts == 0
    assert _buckets(demo_db, root_a) == buckets_before
    assert _wallets(demo_db) == wallets_before
    ledger.assert_ledger_invariants(demo_db.conn, root_a)
    ledger.assert_ledger_invariants(demo_db.conn, root_b)

def test_post_approval_same_submission_new_operation_id_reuses(demo_db):
    """承認後に同じ Requester が同じ submission_id で別 operation_id の
    approve をすると既存 Acceptance / PaymentOperation / Receipt が 1 件の
    まま既存結果を返す（N05 の再利用経路は維持）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    first = _approve(demo_db, child_id, submission_id, "approve:n05-a")
    buckets_before = _buckets(demo_db, root_id)
    wallets_before = _wallets(demo_db)
    second = _approve(demo_db, child_id, submission_id, "approve:n05-b")
    assert second.replayed is True
    assert second.data["payment_operation_id"] == first.data["payment_operation_id"]
    acceptances, payments, receipts = _counts(demo_db, child_id)
    assert acceptances == 1
    assert payments == 1
    assert receipts == 0
    assert _buckets(demo_db, root_id) == buckets_before
    assert _wallets(demo_db) == wallets_before
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# R2: operation_id 照合は前置検査より最優先（IDEMPOTENCY_CONFLICT）
# ---------------------------------------------------------------------------


def test_operation_id_reuse_with_different_actor_is_idempotency_conflict(demo_db):
    """成功した approve と**同じ operation_id** を別 Actor で再利用すると、
    前置検査の FORBIDDEN ではなく IDEMPOTENCY_CONFLICT（計画書 第16節）。
    拒否後も Acceptance・PaymentOperation・残高は不変。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    _approve(demo_db, child_id, submission_id, "approve:reuse-a")
    buckets_before = _buckets(demo_db, root_id)
    wallets_before = _wallets(demo_db)
    with pytest.raises(OjpError) as exc_info:
        _approve(
            demo_db, child_id, submission_id, "approve:reuse-a",
            actor_id=REQUESTER_ID,
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    acceptances, payments, receipts = _counts(demo_db, child_id)
    assert acceptances == 1
    assert payments == 1
    assert receipts == 0
    assert _buckets(demo_db, root_id) == buckets_before
    assert _wallets(demo_db) == wallets_before
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_operation_id_reuse_with_different_submission_is_idempotency_conflict(
    demo_db,
):
    """同じ operation_id ・同じ Actor でも **別 submission_id**（別 payload）
    なら前置検査の INVALID_TARGET ではなく IDEMPOTENCY_CONFLICT。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    _root_b, _child_b, _vb, submission_b = _submitted_child(
        demo_db, suffix="reuse-b"
    )
    _approve(demo_db, child_id, submission_id, "approve:reuse-sub")
    with pytest.raises(OjpError) as exc_info:
        _approve(
            demo_db, child_id, submission_b, "approve:reuse-sub",
            actor_id=AGENT_A_ID,
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    acceptances, payments, receipts = _counts(demo_db, child_id)
    assert acceptances == 1
    assert payments == 1
    assert receipts == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_operation_id_reuse_with_different_job_is_idempotency_conflict(demo_db):
    """同じ operation_id ・同じ Actor でも **別 job_id**（別 payload）なら
    IDEMPOTENCY_CONFLICT（FORBIDDEN / INVALID_TARGET ではない）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    root_b, child_b, _vb, _sb = _submitted_child(demo_db, suffix="reuse-job")
    _approve(demo_db, child_id, submission_id, "approve:reuse-job")
    # child_b の submission（同じ Requester A）で payload だけが違う再利用
    other_submission = demo_db.conn.execute(
        "SELECT id FROM submissions WHERE job_id = ?", (child_b,)
    ).fetchone()["id"]
    with pytest.raises(OjpError) as exc_info:
        _approve(
            demo_db, child_b, other_submission, "approve:reuse-job",
            actor_id=AGENT_A_ID,
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    acceptances, payments, receipts = _counts(demo_db, child_id)
    assert acceptances == 1
    assert payments == 1
    assert receipts == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)
    ledger.assert_ledger_invariants(demo_db.conn, root_b)


def test_operation_id_reuse_same_payload_replays(demo_db):
    """同じ operation_id ＋ 同じ payload の再送は IDEMPOTENCY_CONFLICT では
    なく replay（既存結果。replayed=True）。前置検査の順序変更で replay
    経路が壊れていないことも固定する。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    first = _approve(demo_db, child_id, submission_id, "approve:reuse-same")
    buckets_before = _buckets(demo_db, root_id)
    wallets_before = _wallets(demo_db)
    second = _approve(demo_db, child_id, submission_id, "approve:reuse-same")
    assert second.replayed is True
    assert second.data == first.data
    acceptances, payments, receipts = _counts(demo_db, child_id)
    assert acceptances == 1
    assert payments == 1
    assert receipts == 0
    assert _buckets(demo_db, root_id) == buckets_before
    assert _wallets(demo_db) == wallets_before
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# 送金失敗（DONE は戻らない）
# ---------------------------------------------------------------------------


def test_payment_failure_keeps_done(demo_db):
    """送金失敗（MockEscrow transfer の失敗）でも jobs.state='DONE' のまま。
    DONE が FAILED 等へ戻らない。paid も増えない。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    _approve(demo_db, child_id, submission_id, "approve:pay-fail")
    assert _job(demo_db, child_id)["state"] == JobState.DONE.value

    class _FailingEscrow(ledger.EscrowPort):
        def fund(self, conn, operation_id, payload):
            raise AssertionError("must not call fund")

        def transfer(self, conn, operation_id, payload):
            raise ledger.PaymentFailedError("injected escrow outage")

        def lookup(self, conn, operation_id):
            return None

    result = service.process_single_payment(
        demo_db.conn,
        operation_id=f"approve:pay-fail:payment",
        escrow=_FailingEscrow(),
    )
    assert result.data["payment_status"] == PaymentStatus.RETRYABLE.value
    payment = demo_db.conn.execute(
        "SELECT * FROM payment_operations WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert payment["status"] == PaymentStatus.RETRYABLE.value
    # DONE は戻らない・paid は増えない
    assert _job(demo_db, child_id)["state"] == JobState.DONE.value
    assert _wallets(demo_db)[AGENT_B_ID] == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)
