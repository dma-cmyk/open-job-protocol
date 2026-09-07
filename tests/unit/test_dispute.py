"""Phase 4 S2 の unit テスト: service.dispute（計画書 第12節）。

Done when の対応:
- 期限前の異議で DISPUTED・due_at_us = opened_at + dispute_window。
  口座残高・PaymentOperation は異議の前後で完全に不変（予約前の対象資金を
  そのまま保持する）
- 異議は最大 1 件・再オープンなし（2 件目は INVALID_STATE）
- 固定 condition と固定理由コードのみ受理（価格交渉・主観的な不満は拒否）
- 権限: その Job の Requester または Root Requester のみ
- now >= review_due_at での異議は DISPUTE_WINDOW_CLOSED かつ自動承認済み
- approve と dispute の競合: 先に成立した側だけが有効

状態は create_root → fund_root → claim → (create_child) → submit の
実経路だけで作る。
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
DISPUTE_WINDOW_US = 30_000_000  # TimingPolicy 既定の異議判定待ち30秒
REVIEW_WINDOW_US = 30_000_000  # TimingPolicy 既定の検収待ち30秒


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


def _submitted_child(demo_db, *, suffix="c1"):
    """create_root → fund → A Claim → create_child → B Claim → B submit まで
    済みの Child を返す
    （(root_id, child_id, child_version_id, submission_id)）。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}-r",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=catalog,
        input_json=ledger.canonical_json_dumps(_root_def.input_values),
        verifier_id=verification.VERIFIER_ID,
        verifier_hash=verification.verifier_hash(),
        conditions_json=ledger.canonical_json_dumps(_root_def.expected),
        operation_id=f"create:{suffix}-r",
    )
    root_id = created.data["job_id"]
    service.fund_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        root_id=root_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id=f"fund:{suffix}-r",
    )
    claimed_root = service.claim(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        expected_version_id=created.data["version_id"],
        operation_id=f"claim:{suffix}-r",
    )
    created_child = service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=claimed_root.data["lease_id"],
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id=f"create:{suffix}",
    )
    child_id = created_child.data["child_id"]
    claimed_child = service.claim(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=created_child.data["version_id"],
        operation_id=f"claim:{suffix}-b",
    )
    submitted = service.submit(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=claimed_child.data["lease_id"],
        version_id=created_child.data["version_id"],
        artifact_json='{"sum": 6}',
        operation_id=f"submit:{suffix}",
    )
    return (
        root_id,
        child_id,
        created_child.data["version_id"],
        submitted.data["submission_id"],
    )


def _dispute(demo_db, job_id, submission_id, op_id, *, actor_id=AGENT_A_ID,
             condition_id="sum", reason_code="CONDITION_MISMATCH"):
    return service.dispute(
        demo_db.conn,
        actor_id=actor_id,
        job_id=job_id,
        submission_id=submission_id,
        condition_id=condition_id,
        reason_code=reason_code,
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


def _payment_ops(demo_db, root_id):
    return demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM payment_operations WHERE root_id = ?",
        (root_id,),
    ).fetchone()["c"]


def _wallets(demo_db):
    return {
        row["participant_id"]: int(row["balance_units"])
        for row in demo_db.conn.execute("SELECT * FROM mock_wallets")
    }


# ---------------------------------------------------------------------------
# 期限前の異議（DISPUTED・資金不変）
# ---------------------------------------------------------------------------


def test_dispute_before_deadline_transitions_to_disputed(demo_db):
    """期限前の異議: DISPUTED・due_at_us = opened_at + dispute_window・
    口座残高・PaymentOperation が異議の前後で完全に不変。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    buckets_before = _buckets(demo_db, root_id)
    wallets_before = _wallets(demo_db)
    payments_before = _payment_ops(demo_db, root_id)
    now = TEST_T0_US  # 提出と同時刻（review_due_at = t0+30秒 より前）

    result = _dispute(demo_db, child_id, submission_id, "dispute:open")
    assert result.data["state"] == JobState.DISPUTED.value
    assert result.data["due_at_us"] == now + DISPUTE_WINDOW_US

    dispute = demo_db.conn.execute(
        "SELECT * FROM disputes WHERE id = ?", (result.data["dispute_id"],)
    ).fetchone()
    assert dispute["status"] == "OPEN"
    assert dispute["opened_by"] == AGENT_A_ID
    assert dispute["reason_code"] == "CONDITION_MISMATCH"
    assert dispute["condition_id"] == "sum"
    assert dispute["due_at_us"] == dispute["opened_at_us"] + DISPUTE_WINDOW_US
    assert dispute["resolution"] is None

    assert _job(demo_db, child_id)["state"] == JobState.DISPUTED.value
    # 資金は予約前の対象資金をそのまま保持する
    assert _buckets(demo_db, root_id) == buckets_before
    assert _payment_ops(demo_db, root_id) == payments_before
    assert _wallets(demo_db) == wallets_before
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_second_dispute_is_invalid_state(demo_db):
    """2 件目の異議 → INVALID_STATE（最大 1 件・再オープンなし）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    _dispute(demo_db, child_id, submission_id, "dispute:first")
    with pytest.raises(OjpError) as exc_info:
        _dispute(demo_db, child_id, submission_id, "dispute:second")
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    count = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM disputes"
    ).fetchone()["c"]
    assert count == 1
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# 固定 condition と固定理由コード
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "reason_code",
    ["PRICE_NEGOTIATION", "WANT_NEW_CONDITION", "not_satisfied", "MISC"],
)
def test_dispute_rejects_arbitrary_reason_codes(demo_db, reason_code):
    """不正な reason_code（価格交渉・新条件・主観的な不満を表す任意文字列）は
    INVALID_ARGUMENT。Job は SUBMITTED のまま。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    with pytest.raises(OjpError) as exc_info:
        _dispute(
            demo_db, child_id, submission_id, f"dispute:bad-{reason_code}",
            reason_code=reason_code,
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert _job(demo_db, child_id)["state"] == JobState.SUBMITTED.value
    assert demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM disputes"
    ).fetchone()["c"] == 0


def test_dispute_rejects_unknown_condition_id(demo_db):
    """未知の condition_id → INVALID_ARGUMENT（公開 Version の固定 condition
    のキーだけ受理する）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    with pytest.raises(OjpError) as exc_info:
        _dispute(
            demo_db, child_id, submission_id, "dispute:bad-cond",
            condition_id="total",
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert _job(demo_db, child_id)["state"] == JobState.SUBMITTED.value


# ---------------------------------------------------------------------------
# 権限（Worker 本人・無関係 Actor・Root Requester）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("actor_id", [AGENT_B_ID, SYSTEM_ID])
def test_dispute_rejects_worker_and_unrelated_actor(demo_db, actor_id):
    """Worker 本人・無関係 Actor の異議は FORBIDDEN。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    with pytest.raises(OjpError) as exc_info:
        _dispute(
            demo_db, child_id, submission_id, f"dispute:forbid-{actor_id}",
            actor_id=actor_id,
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    assert _job(demo_db, child_id)["state"] == JobState.SUBMITTED.value
    assert demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM disputes"
    ).fetchone()["c"] == 0


def test_root_requester_can_dispute_child(demo_db):
    """Root Requester は Child の異議を出せる（第5節「異議はそのJobの
    RequesterまたはRoot Requesterが提出できる」）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    result = _dispute(
        demo_db, child_id, submission_id, "dispute:root-req",
        actor_id=REQUESTER_ID,
    )
    assert result.data["state"] == JobState.DISPUTED.value
    assert _job(demo_db, child_id)["state"] == JobState.DISPUTED.value
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# 期限後の異議（DISPUTE_WINDOW_CLOSED ＋ 自動承認）
# ---------------------------------------------------------------------------


def test_dispute_after_review_deadline_closes_window_and_auto_approves(demo_db):
    """now >= review_due_at での異議 → DISPUTE_WINDOW_CLOSED かつその
    Submission が自動承認されている（Acceptance 1 件・DONE・支払い予約 1 件）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    review_due_at = demo_db.conn.execute(
        "SELECT review_due_at_us FROM submissions WHERE id = ?",
        (submission_id,),
    ).fetchone()["review_due_at_us"]
    clock.set_test_now(demo_db.conn, review_due_at)
    with pytest.raises(OjpError) as exc_info:
        _dispute(demo_db, child_id, submission_id, "dispute:late")
    assert exc_info.value.code == ErrorCode.DISPUTE_WINDOW_CLOSED.value

    # その Submission は自動承認されている
    job = _job(demo_db, child_id)
    assert job["state"] == JobState.DONE.value
    acceptance = demo_db.conn.execute(
        "SELECT * FROM acceptances WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert acceptance["decision"] == "APPROVED"
    payment = demo_db.conn.execute(
        "SELECT * FROM payment_operations WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert payment["business_key"] == f"payout:{child_id}"
    assert payment["status"] == PaymentStatus.PENDING.value
    assert demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM disputes"
    ).fetchone()["c"] == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# approve と dispute の競合
# ---------------------------------------------------------------------------


def test_dispute_after_approve_is_invalid_state(demo_db):
    """approve が先に成立したら dispute は INVALID_STATE（先に commit した
    有効遷移が勝つ）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    service.approve(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=child_id,
        submission_id=submission_id,
        operation_id="approve:raced",
    )
    with pytest.raises(OjpError) as exc_info:
        _dispute(demo_db, child_id, submission_id, "dispute:late-race")
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert _job(demo_db, child_id)["state"] == JobState.DONE.value
    assert demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM disputes"
    ).fetchone()["c"] == 0
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_dispute_operation_id_reuse_with_different_payload_is_conflict(demo_db):
    """R2（dispute 経路の確認）: 成功した dispute と**同じ operation_id** を
    別 payload（別 condition_id）で再利用すると、冪等性契約どおり
    IDEMPOTENCY_CONFLICT になる（FORBIDDEN / INVALID_STATE ではない）。
    同じ operation_id ＋同じ payload の再送は replay（dispute_id が同じ）。
    dispute のドメイン検査は _run_idempotent 内のため、この契約は
    _run_idempotent が担保する（前置検査の追加は不要）。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    first = _dispute(demo_db, child_id, submission_id, "dispute:idem")
    dispute_id = first.data["dispute_id"]
    # 同じ operation_id ＋別 payload（別 condition_id）は IDEMPOTENCY_CONFLICT
    with pytest.raises(OjpError) as exc_info:
        _dispute(
            demo_db, child_id, submission_id, "dispute:idem",
            condition_id="total",
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    # 同じ operation_id ＋別 Actor も IDEMPOTENCY_CONFLICT
    with pytest.raises(OjpError) as exc_info:
        _dispute(
            demo_db, child_id, submission_id, "dispute:idem",
            actor_id=REQUESTER_ID,
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    # 同じ operation_id ＋同じ payload の再送は replay（同じ dispute_id）
    replay = _dispute(demo_db, child_id, submission_id, "dispute:idem")
    assert replay.replayed is True
    assert replay.data["dispute_id"] == dispute_id
    # 異議は 1 件だけ・Job・資金は不変
    assert demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM disputes"
    ).fetchone()["c"] == 1
    assert _job(demo_db, child_id)["state"] == JobState.DISPUTED.value
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_approve_after_dispute_is_invalid_state(demo_db):
    """dispute が先に成立したら approve は INVALID_STATE。"""
    root_id, child_id, _v, submission_id = _submitted_child(demo_db)
    _dispute(demo_db, child_id, submission_id, "dispute:first-race")
    with pytest.raises(OjpError) as exc_info:
        service.approve(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            job_id=child_id,
            submission_id=submission_id,
            operation_id="approve:raced",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert _job(demo_db, child_id)["state"] == JobState.DISPUTED.value
    ledger.assert_ledger_invariants(demo_db.conn, root_id)
