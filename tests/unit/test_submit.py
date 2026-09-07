"""Phase 4 S1 の unit テスト: service.submit（計画書 第11節「有効な提出の境界」）。

Done when の対応:
- 2（不正 JSON・誤答・古い Version・保存失敗のいずれでも、有効 Submission・
  review_due_at・支払い予約が作られず、Lease が開いたまま・Job が LEASED の
  まま残る）
- 3（有効提出は SUBMITTED へ遷移し、Lease が 'submitted' で閉じ、
  review_due_at_us = valid_at_us + review_window、支払い予約は作られない）
- 4（Parent の未解決 Child がある提出が CHILDREN_UNRESOLVED で拒否され、
  検収タイマーが作られない。Child を終端にすると Parent が提出できる。
  Child DONE の送金待ちでも Parent 提出できる）
- 6（検証器の検査と X06: expected / verifier_id / payee_id / amount を渡すと
  TypeError。提出は create_root → fund_root → claim（必要なら create_child）
  の実経路で状態を作る）
"""

from __future__ import annotations

import json

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
REVIEW_WINDOW_US = 30_000_000  # TimingPolicy 既定の検収待ち30秒


@pytest.fixture(autouse=True)
def _clear_failpoints():
    """failpoint seam はモジュール属性なので、テスト後は必ず元に戻す。"""
    yield
    verification.failpoint_before_submission_commit = None


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


def _leased_root(demo_db, *, suffix="r1", timing=None, deadline_us=DEADLINE_US):
    """create_root（第11節の固定カタログ）→ fund_root → A が Claim 済みの
    Root を返す（(root_id, root_version_id, parent_lease_id)）。
    検証器は VERIFIER_ID / verifier_hash() の実値を公開版へ保存する。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}",
        budget="100.000000",
        deadline_us=deadline_us,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=catalog,
        timing_policy=timing,
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


def _leased_child(demo_db, *, suffix="c1", task_key="part-1", budget=TEN):
    """create_root → fund → A Claim → create_child → B Claim 済みの Child を
    返す（(root_id, child_id, child_version_id, child_lease_id,
    root_lease_id)）。実経路だけで状態を作る。"""
    root_id, _root_v, root_lease = _leased_root(demo_db, suffix=f"{suffix}-r")
    created = service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease,
        task_key=task_key,
        budget_units=budget,
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


def _submit(demo_db, job_id, lease_id, version_id, artifact_json, op_id, *,
            actor_id=None):
    return service.submit(
        demo_db.conn,
        actor_id=actor_id if actor_id is not None else AGENT_B_ID,
        job_id=job_id,
        lease_id=lease_id,
        version_id=version_id,
        artifact_json=artifact_json,
        operation_id=op_id,
    )


def _job(demo_db, job_id):
    return demo_db.conn.execute(
        "SELECT * FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()


def _lease(demo_db, lease_id):
    return demo_db.conn.execute(
        "SELECT * FROM leases WHERE id = ?", (lease_id,)
    ).fetchone()


def _counts(demo_db, job_id):
    submissions = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM submissions WHERE job_id = ?", (job_id,)
    ).fetchone()["c"]
    attempts = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM submission_attempts WHERE job_id = ?",
        (job_id,),
    ).fetchone()["c"]
    fail_attempts = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM submission_attempts WHERE job_id = ?"
        " AND outcome = 'FAIL'",
        (job_id,),
    ).fetchone()["c"]
    return submissions, attempts, fail_attempts


def _wallets(demo_db):
    return {
        row["participant_id"]: int(row["balance_units"])
        for row in demo_db.conn.execute("SELECT * FROM mock_wallets")
    }


def _payment_ops(demo_db, root_id):
    return demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM payment_operations WHERE root_id = ?",
        (root_id,),
    ).fetchone()["c"]


# ---------------------------------------------------------------------------
# PASS 経路（Done when 3）
# ---------------------------------------------------------------------------


def test_submit_pass_transitions_to_submitted(demo_db):
    """有効提出: SUBMITTED へ遷移、review_due_at = valid_at + review_window、
    Lease が 'submitted' で閉じる、active_lease_id は NULL、submissions 1 件、
    支払い予約 0 件、口座残高は提出前後で不変。"""
    root_id, child_id, version_id, lease_id, _rl = _leased_child(demo_db)
    wallets_before = _wallets(demo_db)
    result = _submit(demo_db, child_id, lease_id, version_id, '{"sum": 6}', "submit:p1")
    assert result.data["state"] == JobState.SUBMITTED.value
    assert result.data["verification"] == "PASS"

    job = _job(demo_db, child_id)
    assert job["state"] == JobState.SUBMITTED.value
    assert job["active_lease_id"] is None
    lease = _lease(demo_db, lease_id)
    assert lease["closed_reason"] == "submitted"
    submissions, attempts, fail_attempts = _counts(demo_db, child_id)
    assert submissions == 1
    assert attempts == 1  # 監査用 PASS 行
    assert fail_attempts == 0
    submission = demo_db.conn.execute(
        "SELECT * FROM submissions WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert submission["verification_result"] == "PASS"
    assert submission["artifact_json"] == '{"sum":6}'  # canonical 形式
    assert submission["valid_at_us"] == TEST_T0_US
    assert submission["submitted_at_us"] == TEST_T0_US
    assert (
        submission["review_due_at_us"]
        == submission["valid_at_us"] + REVIEW_WINDOW_US
    )
    assert result.data["review_due_at"] == submission["review_due_at_us"]
    # 支払い予約は作られない・残高は不変（提出だけでは資金は動かない）
    assert _payment_ops(demo_db, root_id) == 0
    assert _wallets(demo_db) == wallets_before
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_submit_pass_closes_lease_against_heartbeat(demo_db):
    """提出後の heartbeat は LEASE_EXPIRED（閉じた Lease を復活させない）。"""
    _root, child_id, version_id, lease_id, _rl = _leased_child(demo_db)
    _submit(demo_db, child_id, lease_id, version_id, '{"sum": 6}', "submit:hb")
    with pytest.raises(OjpError) as exc_info:
        service.heartbeat(
            demo_db.conn,
            actor_id=AGENT_B_ID,
            job_id=child_id,
            lease_id=lease_id,
            generation=1,
            operation_id="heartbeat:after-submit",
        )
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value


def test_double_submit_is_invalid_state(demo_db):
    """SUBMITTED への二重提出は拒否される。検査順序 2-2 の require_active_lease
    （閉じた Lease → LEASE_EXPIRED）が 2-3 の LEASED 検査より先のため、
    提出済み Lease（closed_reason='submitted'）に対する再提出は
    LEASE_EXPIRED として拒否される（閉じた Lease を復活させない。第7節）。"""
    _root, child_id, version_id, lease_id, _rl = _leased_child(demo_db)
    _submit(demo_db, child_id, lease_id, version_id, '{"sum": 6}', "submit:d1")
    with pytest.raises(OjpError) as exc_info:
        _submit(
            demo_db, child_id, lease_id, version_id, '{"sum": 6}', "submit:d2"
        )
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value
    submissions, _a, _f = _counts(demo_db, child_id)
    assert submissions == 1


def test_submit_pass_on_root_without_children(demo_db):
    """Child を持たない Root の提出も通る（{"sum":15}）。"""
    root_id, version_id, lease_id = _leased_root(demo_db, suffix="root-only")
    result = _submit(
        demo_db, root_id, lease_id, version_id, '{"sum": 15}', "submit:root",
        actor_id=AGENT_A_ID,
    )
    assert result.data["state"] == JobState.SUBMITTED.value


# ---------------------------------------------------------------------------
# FAIL 経路（Done when 2。不正 JSON / 誤答 / bool / キー不一致）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "artifact_json, reason",
    [
        ("{not json", "ARTIFACT_NOT_JSON"),
        ('{"sum":5}', "ARTIFACT_VALUE_MISMATCH"),
        ('{"sum":true}', "ARTIFACT_TYPE_INVALID"),
        ('{"sum":6,"extra":1}', "ARTIFACT_KEY_MISMATCH"),
    ],
)
def test_submit_fail_keeps_leased_and_records_attempt(
    demo_db, artifact_json, reason
):
    """FAIL: VERIFICATION_FAILED、submissions 0 件、attempts に FAIL 1 件、
    Job は LEASED、Lease は開いたまま、支払い予約 0 件、残高不変。"""
    root_id, child_id, version_id, lease_id, _rl = _leased_child(demo_db)
    wallets_before = _wallets(demo_db)
    with pytest.raises(OjpError) as exc_info:
        _submit(
            demo_db, child_id, lease_id, version_id, artifact_json, "submit:f1"
        )
    assert exc_info.value.code == ErrorCode.VERIFICATION_FAILED.value
    assert exc_info.value.details["reason"] == reason
    assert exc_info.value.details["attempt_id"] is not None

    job = _job(demo_db, child_id)
    assert job["state"] == JobState.LEASED.value
    assert job["active_lease_id"] == lease_id
    lease = _lease(demo_db, lease_id)
    assert lease["closed_reason"] is None
    submissions, attempts, fail_attempts = _counts(demo_db, child_id)
    assert submissions == 0
    assert attempts == 1
    assert fail_attempts == 1
    attempt = demo_db.conn.execute(
        "SELECT * FROM submission_attempts WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert attempt["outcome"] == "FAIL"
    assert attempt["reason"] == reason
    assert attempt["lease_id"] == lease_id
    assert _payment_ops(demo_db, root_id) == 0
    assert _wallets(demo_db) == wallets_before
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_submit_fail_then_corrected_resubmission_succeeds(demo_db):
    """FAIL 後、同じ Lease・新しい operation_id で正解を再提出すると成功する。"""
    _root, child_id, version_id, lease_id, _rl = _leased_child(demo_db)
    with pytest.raises(OjpError) as exc_info:
        _submit(demo_db, child_id, lease_id, version_id, '{"sum": 5}', "submit:bad")
    assert exc_info.value.code == ErrorCode.VERIFICATION_FAILED.value
    result = _submit(
        demo_db, child_id, lease_id, version_id, '{"sum": 6}', "submit:good"
    )
    assert result.data["state"] == JobState.SUBMITTED.value
    submissions, attempts, _f = _counts(demo_db, child_id)
    assert submissions == 1
    assert attempts == 2  # FAIL 1 件 + PASS 1 件
    assert _lease(demo_db, lease_id)["closed_reason"] == "submitted"


def test_submit_fail_replay_with_same_operation_id(demo_db):
    """同じ operation_id で FAIL を再送すると同じ VERIFICATION_FAILED が返る
    （replay 経路で保存済み結果を再現。決定的）。"""
    _root, child_id, version_id, lease_id, _rl = _leased_child(demo_db)
    for op_id in ("submit:replay-fail",):
        errors = []
        for _ in range(2):
            try:
                _submit(demo_db, child_id, lease_id, version_id, '{"sum": 5}', op_id)
            except OjpError as exc:
                errors.append(exc)
    assert len(errors) == 2
    first, second = errors
    assert first.code == ErrorCode.VERIFICATION_FAILED.value
    assert second.code == ErrorCode.VERIFICATION_FAILED.value
    assert second.details == first.details
    # Attempt は 1 件だけ（replay では追加されない）
    _s, attempts, _f = _counts(demo_db, child_id)
    assert attempts == 1
    assert _job(demo_db, child_id)["state"] == JobState.LEASED.value


def test_submit_rejects_old_version(demo_db):
    """古い Version（version_id に別の版 ID）→ INVALID_TARGET。"""
    _root, child_id, _version_id, lease_id, _rl = _leased_child(demo_db)
    with pytest.raises(OjpError) as exc_info:
        _submit(
            demo_db,
            child_id,
            lease_id,
            "version:old:1",
            '{"sum": 6}',
            "submit:old-version",
        )
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value
    submissions, attempts, _f = _counts(demo_db, child_id)
    assert submissions == 0
    assert attempts == 0
    assert _job(demo_db, child_id)["state"] == JobState.LEASED.value


# ---------------------------------------------------------------------------
# 保存失敗（failpoint_before_submission_commit。Done when 2 の「取得不能と
# なる保存失敗」）
# ---------------------------------------------------------------------------


def test_submit_storage_failure_rolls_back_everything(demo_db):
    """PASS 判定後の commit 前失敗: submissions 0 件・submission_attempts にも
    行が残らない・Job は LEASED・Lease は開いたまま・review_due_at なし・
    支払い予約 0 件・残高不変。"""
    root_id, child_id, version_id, lease_id, _rl = _leased_child(demo_db)
    wallets_before = _wallets(demo_db)

    def _boom(name):
        raise RuntimeError(f"injected storage failure: {name}")

    verification.failpoint_before_submission_commit = _boom
    with pytest.raises(RuntimeError):
        _submit(
            demo_db, child_id, lease_id, version_id, '{"sum": 6}', "submit:boom"
        )
    verification.failpoint_before_submission_commit = None

    job = _job(demo_db, child_id)
    assert job["state"] == JobState.LEASED.value
    assert job["active_lease_id"] == lease_id
    lease = _lease(demo_db, lease_id)
    assert lease["closed_reason"] is None
    submissions, attempts, _f = _counts(demo_db, child_id)
    assert submissions == 0
    assert attempts == 0  # attempt 行も rollback される
    assert _payment_ops(demo_db, root_id) == 0
    assert _wallets(demo_db) == wallets_before
    # 同じ operation_id で再送すると（障害が無ければ）成功する
    result = _submit(
        demo_db, child_id, lease_id, version_id, '{"sum": 6}', "submit:boom"
    )
    assert result.data["state"] == JobState.SUBMITTED.value


def test_submit_failpoint_rejected_on_realtime_db(realtime_db):
    """failpoint seam を realtime DB で使うと拒否される（test mode 専用）。"""
    from ojp.domain import ClockMode

    conn = realtime_db.conn
    # 実経路で Lease まで作るには realtime で時刻制御ができないため、
    # seam の契約（何も動かす前に拒否）をassert_submission_failpoints_allowed
    # の直接呼び出しで検証する
    def _boom(name):
        raise AssertionError("must not fire")

    verification.failpoint_before_submission_commit = _boom
    try:
        with pytest.raises(OjpError) as exc_info:
            verification.assert_submission_failpoints_allowed(conn)
        assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    finally:
        verification.failpoint_before_submission_commit = None


# ---------------------------------------------------------------------------
# X06: 条件・検証器・受取人のすり替え引数は受け付けない
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "override_kwarg",
    [
        {"expected": {"sum": 6}},
        {"verifier_id": "forged-verifier"},
        {"payee_id": AGENT_B_ID},
        {"amount": 10_000_000},
    ],
)
def test_submit_rejects_condition_override_arguments(demo_db, override_kwarg):
    """submit は expected / verifier_id / payee_id / amount をシグネチャに
    持たず、渡すと TypeError になる（X06。create_child と同じ方針）。"""
    _root, child_id, version_id, lease_id, _rl = _leased_child(demo_db)
    with pytest.raises(TypeError):
        service.submit(
            demo_db.conn,
            actor_id=AGENT_B_ID,
            job_id=child_id,
            lease_id=lease_id,
            version_id=version_id,
            artifact_json='{"sum": 6}',
            operation_id="submit:x06",
            **override_kwarg,
        )
    submissions, attempts, _f = _counts(demo_db, child_id)
    assert submissions == 0
    assert attempts == 0


# ---------------------------------------------------------------------------
# CHILDREN_UNRESOLVED（Done when 4・X04）
# ---------------------------------------------------------------------------


def test_parent_submit_rejected_while_child_open(demo_db):
    """Child が OPEN の間に Parent が提出すると CHILDREN_UNRESOLVED、
    submissions 0 件・タイマーなし。Child を終端にすると Parent が提出できる。"""
    root_id, root_v, root_lease = _leased_root(demo_db, suffix="x04")
    service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease,
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id="create:x04-c1",
    )
    with pytest.raises(OjpError) as exc_info:
        _submit(
            demo_db,
            root_id,
            root_lease,
            root_v,
            '{"sum": 15}',
            "submit:x04-blocked",
            actor_id=AGENT_A_ID,
        )
    assert exc_info.value.code == ErrorCode.CHILDREN_UNRESOLVED.value
    submissions, attempts, _f = _counts(demo_db, root_id)
    assert submissions == 0
    assert attempts == 0
    assert _job(demo_db, root_id)["state"] == JobState.LEASED.value
    assert _lease(demo_db, root_lease)["closed_reason"] is None

    # Child を終端（B が abandon で FAILED）にすると Parent が提出できる
    child_id = demo_db.conn.execute(
        "SELECT id FROM jobs WHERE parent_id = ?", (root_id,)
    ).fetchone()["id"]
    child_lease = service.claim(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=_job(demo_db, child_id)["version_id"],
        operation_id="claim:x04-c1",
    ).data["lease_id"]
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease,
        operation_id="abandon:x04-c1",
    )
    result = _submit(
        demo_db,
        root_id,
        root_lease,
        root_v,
        '{"sum": 15}',
        "submit:x04-ok",
        actor_id=AGENT_A_ID,
    )
    assert result.data["state"] == JobState.SUBMITTED.value


@pytest.mark.parametrize(
    "child_state_op",
    ["lease_only", "submit_child"],
)
def test_parent_submit_rejected_while_child_leased_or_submitted(
    demo_db, child_state_op
):
    """Child が LEASED / SUBMITTED のときも Parent 提出は CHILDREN_UNRESOLVED。"""
    root_id, root_v, root_lease = _leased_root(demo_db, suffix=f"x04-{child_state_op}")
    created = service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease,
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id=f"create:{child_state_op}",
    )
    child_id = created.data["child_id"]
    child_lease = service.claim(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=created.data["version_id"],
        operation_id=f"claim:{child_state_op}",
    ).data["lease_id"]
    if child_state_op == "submit_child":
        _submit(
            demo_db, child_id, child_lease, created.data["version_id"],
            '{"sum": 6}', f"submit:{child_state_op}-c",
        )
    with pytest.raises(OjpError) as exc_info:
        _submit(
            demo_db,
            root_id,
            root_lease,
            root_v,
            '{"sum": 15}',
            f"submit:{child_state_op}-parent",
            actor_id=AGENT_A_ID,
        )
    assert exc_info.value.code == ErrorCode.CHILDREN_UNRESOLVED.value
    submissions, _a, _f = _counts(demo_db, root_id)
    assert submissions == 0


def test_parent_submit_allowed_when_child_done_payment_pending(demo_db):
    """Child が DONE で送金待ち（PaymentOperation が PENDING）でも Parent
    提出できる（第8節: Child 送金の完了までは待たない）。"""
    root_id, root_v, root_lease = _leased_root(demo_db, suffix="done-pending")
    created = service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease,
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id="create:done-pending-c1",
    )
    child_id = created.data["child_id"]
    child_lease = service.claim(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=created.data["version_id"],
        operation_id="claim:done-pending-c1",
    ).data["lease_id"]
    _submit(
        demo_db, child_id, child_lease, created.data["version_id"],
        '{"sum": 6}', "submit:done-pending-c1",
    )
    # Child を SUBMITTED → DONE へ。Phase 4 S2 の approve は未実装のため、
    # 既存の reserve_child_payout（Child approval の確定部分）と状態遷移で
    # 「DONE かつ PaymentOperation PENDING」を作る。approve 相当の実経路の
    # うち資金予約は service.reserve_child_payout が正本である
    service.reserve_child_payout(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        root_id=root_id,
        child_id=child_id,
        amount_units=TEN,
        payee_id=AGENT_B_ID,
        operation_id="payout:done-pending-c1",
    )
    with db.transaction(demo_db.conn, immediate=True):
        demo_db.conn.execute(
            "UPDATE jobs SET state = 'DONE', active_lease_id = NULL,"
            " row_version = row_version + 1 WHERE id = ?",
            (child_id,),
        )
    payment = demo_db.conn.execute(
        "SELECT status FROM payment_operations WHERE job_id = ?",
        (child_id,),
    ).fetchone()
    assert payment["status"] == PaymentStatus.PENDING.value

    result = _submit(
        demo_db,
        root_id,
        root_lease,
        root_v,
        '{"sum": 15}',
        "submit:done-pending-parent",
        actor_id=AGENT_A_ID,
    )
    assert result.data["state"] == JobState.SUBMITTED.value


# ---------------------------------------------------------------------------
# 権限系（X15 相当）
# ---------------------------------------------------------------------------


def test_submit_rejects_requester_and_unrelated_actor(demo_db):
    """別 Actor（Requester・無関係 Actor）が submit すると FORBIDDEN。
    存在しない lease_id は INVALID_TARGET。いずれも submissions /
    submission_attempts / 残高が不変。"""
    root_id, child_id, version_id, lease_id, _rl = _leased_child(demo_db)
    wallets_before = _wallets(demo_db)
    with pytest.raises(OjpError) as exc_info:
        _submit(
            demo_db, child_id, lease_id, version_id, '{"sum": 6}',
            "submit:req", actor_id=REQUESTER_ID,
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    with pytest.raises(OjpError) as exc_info:
        _submit(
            demo_db, child_id, lease_id, version_id, '{"sum": 6}',
            "submit:unrelated", actor_id="pt-system",
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    with pytest.raises(OjpError) as exc_info:
        _submit(
            demo_db, child_id, "lease:unknown", version_id, '{"sum": 6}',
            "submit:bad-lease",
        )
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value
    submissions, attempts, _f = _counts(demo_db, child_id)
    assert submissions == 0
    assert attempts == 0
    assert _wallets(demo_db) == wallets_before
    assert _job(demo_db, child_id)["state"] == JobState.LEASED.value
    assert _lease(demo_db, lease_id)["closed_reason"] is None
