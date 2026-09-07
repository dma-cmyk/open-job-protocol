"""Phase 4 S2 の unit テスト: 固定裁定（resolve_due_disputes。X14・第12節）。

Done when の対応:
- 裁定 PASS → DONE ＋ Acceptance APPROVED ＋ 支払い予約
- arbiter_result_override で FAIL を再現 → FAILED ＋ Acceptance REJECTED ＋
  Child なら child_work → available（Parent 生存中）、Parent 終端後なら
  追加返金 refund:{root_id}:child-return:{child_id} が 1 件だけ
- failpoint_arbiter_unresponsive で無応答 → due_at_us 前は Job DISPUTED・
  異議 OPEN・資金不変、due_at_us 到達後（期限ちょうどを含む）は必ず
  保存済み PASS を採用して DONE ＋ Acceptance APPROVED ＋ 支払い予約
  （TimingPolicy.unresponsive_arbiter_fallback='stored_pass' は全 Version
  が事前記録。資金を永久凍結しない）
- 裁定結果（PASS / FAIL / 無応答 fallback）の resolution は
  outcome / reason / evidence / condition_id / verifier_id / verifier_hash /
  input_hash を含む canonical JSON
- seam を realtime DB で使うと何も動かさず拒否
- 全経路の後で ledger.assert_ledger_invariants が成立

状態は create_root → fund_root → claim → create_child → submit → dispute の
実経路だけで作る。
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
DISPUTE_WINDOW_US = 30_000_000


@pytest.fixture(autouse=True)
def _clear_arbitration_seams():
    """裁定 seam はモジュール属性なので、テスト後は必ず元に戻す。"""
    yield
    verification.arbiter_result_override = None
    verification.failpoint_arbiter_unresponsive = None


@pytest.fixture
def demo_db(test_db):
    """固定 Actor + Wallet seed 済みの test mode DB（Worker も 0 で seed）。
    Requester は 300（Root 3 件分）持たせておき、期限境界テストなど複数
    Root を作るテストに対応する。"""
    with db.transaction(test_db.conn, immediate=True):
        insert_demo_participants(test_db.conn)
        for participant_id, balance in (
            (REQUESTER_ID, ROOT_BUDGET_UNITS * 3),
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


def _disputed_child(demo_db, *, suffix="c1", timing=None):
    """create_root → fund → A Claim → create_child → B Claim → B submit →
    A dispute まで済みの Child を返す
    （(root_id, child_id, submission_id, dispute_id)）。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}-r",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=catalog,
        timing_policy=timing,
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
    submission_id = submitted.data["submission_id"]
    disputed = service.dispute(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=child_id,
        submission_id=submission_id,
        condition_id="sum",
        reason_code="CONDITION_MISMATCH",
        operation_id=f"dispute:{suffix}",
    )
    return root_id, child_id, submission_id, disputed.data["dispute_id"]


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


def _payment_by_key(demo_db, business_key):
    return demo_db.conn.execute(
        "SELECT * FROM payment_operations WHERE business_key = ?",
        (business_key,),
    ).fetchone()


def _fail_outcome():
    return verification.VerificationOutcome(
        result=verification.VerificationResult.FAIL,
        reason="ARTIFACT_VALUE_MISMATCH",
        canonical_artifact=None,
        artifact_hash=None,
        input_hash="0" * 64,
        evidence=ledger.canonical_json_dumps(
            {"injected": "arbiter_result_override", "result": "FAIL"}
        ),
    )


def _dispute_row(demo_db, dispute_id):
    return demo_db.conn.execute(
        "SELECT * FROM disputes WHERE id = ?", (dispute_id,)
    ).fetchone()


def _resolution_record(demo_db, dispute_id):
    """disputes.resolution を canonical JSON として parse して返す。"""
    return json.loads(_dispute_row(demo_db, dispute_id)["resolution"])


def _expected_arbitrate_outcome(demo_db, dispute_id):
    """保存成果物を verification.arbitrate で再検証した証跡（テスト内再現）。"""
    dispute = _dispute_row(demo_db, dispute_id)
    submission = demo_db.conn.execute(
        "SELECT * FROM submissions WHERE id = ?", (dispute["submission_id"],)
    ).fetchone()
    version = demo_db.conn.execute(
        "SELECT * FROM job_versions WHERE id = ?",
        (submission["version_id"],),
    ).fetchone()
    return verification.arbitrate(
        stored_artifact_json=str(submission["artifact_json"]),
        input_values=json.loads(version["input_json"]),
        expected=json.loads(version["conditions_json"]),
        verifier_id=version["verifier_id"],
        verifier_hash_value=version["verifier_hash"],
        original_evidence=str(submission["verification_evidence"]),
    )


# ---------------------------------------------------------------------------
# 裁定 PASS（X14「決定的裁定PASS」）
# ---------------------------------------------------------------------------


def test_resolve_pass_approves_and_reserves_payout(demo_db):
    """裁定 PASS → DONE ＋ Acceptance APPROVED ＋ 支払い予約、
    disputes.status='RESOLVED'。"""
    root_id, child_id, submission_id, dispute_id = _disputed_child(demo_db)
    results = service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID)
    assert len(results) == 1
    data = results[0].data
    assert data["resolution"] == "PASS"
    assert data["job_id"] == child_id
    assert results[0].operation_id == f"resolve:{dispute_id}"

    job = _job(demo_db, child_id)
    assert job["state"] == JobState.DONE.value
    acceptance = demo_db.conn.execute(
        "SELECT * FROM acceptances WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert acceptance["decision"] == "APPROVED"
    assert acceptance["submission_id"] == submission_id
    payment = _payment_by_key(demo_db, f"payout:{child_id}")
    assert payment["status"] == PaymentStatus.PENDING.value
    assert payment["payee_id"] == AGENT_B_ID
    dispute = demo_db.conn.execute(
        "SELECT * FROM disputes WHERE id = ?", (dispute_id,)
    ).fetchone()
    assert dispute["status"] == "RESOLVED"
    # resolution は canonical JSON: outcome / reason / evidence /
    # condition_id / verifier_id / verifier_hash / input_hash を含む
    resolution = _resolution_record(demo_db, dispute_id)
    assert resolution["outcome"] == "PASS"
    assert resolution["condition_id"] == dispute["condition_id"]
    outcome = _expected_arbitrate_outcome(demo_db, dispute_id)
    assert resolution["reason"] == outcome.reason
    assert resolution["evidence"] == outcome.evidence
    assert resolution["input_hash"] == outcome.input_hash
    assert resolution["verifier_id"] == verification.VERIFIER_ID
    assert resolution["verifier_hash"] == verification.verifier_hash()
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

def test_resolve_is_idempotent(demo_db):
    """resolve:{dispute_id} は冪等（再実行は RESOLVED を選ばないため
    何もせず、Acceptance も PaymentOperation も 1 件のまま）。"""
    root_id, child_id, _s, dispute_id = _disputed_child(demo_db)
    first = service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID)
    assert len(first) == 1
    second = service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID)
    assert second == []
    assert demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM acceptances WHERE job_id = ?", (child_id,)
    ).fetchone()["c"] == 1
    assert demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM payment_operations WHERE job_id = ?",
        (child_id,),
    ).fetchone()["c"] == 1
    # resolve Operation は 1 件だけ
    assert (
        demo_db.conn.execute(
            "SELECT COUNT(*) AS c FROM operations WHERE operation_id = ?",
            (f"resolve:{dispute_id}",),
        ).fetchone()["c"]
        == 1
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# 裁定 FAIL（X14「証拠付きFAIL」）
# ---------------------------------------------------------------------------


def test_resolve_fail_returns_child_work_to_available(demo_db):
    """FAIL 再現（Parent 生存中）→ FAILED ＋ Acceptance REJECTED ＋
    child_work → available。返金 PaymentOperation は作らない。
    resolution は理由コード・証跡・condition_id を含む canonical JSON。"""
    root_id, child_id, _s, dispute_id = _disputed_child(demo_db)
    verification.arbiter_result_override = lambda label: _fail_outcome()
    results = service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID)
    assert results[0].data["resolution"] == "FAIL"

    job = _job(demo_db, child_id)
    assert job["state"] == JobState.FAILED.value
    acceptance = demo_db.conn.execute(
        "SELECT * FROM acceptances WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert acceptance["decision"] == "REJECTED"
    # FAIL の理由コードが acceptances.reason にも含まれる
    assert "ARTIFACT_VALUE_MISMATCH" in acceptance["reason"]
    buckets = _buckets(demo_db, root_id)
    assert buckets[(child_id, "child_work")] == 0
    assert buckets[(root_id, "available")] == ROOT_BUDGET_UNITS
    # Parent 生存中の返却は返金予約を作らない（child_work → available のみ）
    assert _payment_by_key(demo_db, f"payout:{child_id}") is None
    assert _payment_by_key(demo_db, f"refund:{root_id}:child-return:{child_id}") is None
    dispute = demo_db.conn.execute(
        "SELECT * FROM disputes WHERE id = ?", (dispute_id,)
    ).fetchone()
    assert dispute["status"] == "RESOLVED"
    # resolution は canonical JSON: 理由コード・証跡・異議の condition_id と
    # の一致・arbitrate の証跡との一致を検証する
    resolution = _resolution_record(demo_db, dispute_id)
    assert resolution["outcome"] == "FAIL"
    assert resolution["condition_id"] == dispute["condition_id"]
    assert resolution["reason"] == "ARTIFACT_VALUE_MISMATCH"
    assert resolution["evidence"] == _fail_outcome().evidence
    assert resolution["input_hash"] == "0" * 64
    assert resolution["verifier_id"] == verification.VERIFIER_ID
    assert resolution["verifier_hash"] == verification.verifier_hash()
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_resolve_fail_after_parent_terminal_reserves_child_return_refund(demo_db):
    """FAIL 再現（Parent 終端後）→ 追加返金 refund:{root_id}:child-return:{child_id}
    が 1 件だけ。Parent の先行返金（refund:{root_id}:terminal）と合わせて
    返金総額が入金総額に一致する。"""
    root_id, child_id, _s, _d = _disputed_child(demo_db, suffix="after-term")
    # Parent（Root）を失効させる（Child は DISPUTED のため伝播しない）
    clock.set_test_now(demo_db.conn, TEST_T0_US + 61_000_000)
    service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)
    assert _job(demo_db, root_id)["state"] == JobState.EXPIRED.value
    assert _job(demo_db, child_id)["state"] == JobState.DISPUTED.value
    terminal_refund = _payment_by_key(demo_db, f"refund:{root_id}:terminal")
    assert terminal_refund is not None
    assert terminal_refund["amount_units"] == ROOT_BUDGET_UNITS - TEN

    verification.arbiter_result_override = lambda label: _fail_outcome()
    results = service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID)
    assert results[0].data["resolution"] == "FAIL"
    assert _job(demo_db, child_id)["state"] == JobState.FAILED.value

    child_return = _payment_by_key(
        demo_db, f"refund:{root_id}:child-return:{child_id}"
    )
    assert child_return is not None
    assert child_return["amount_units"] == TEN
    assert child_return["status"] == PaymentStatus.PENDING.value
    # 追加返金は 1 件だけ（再実行しても増えない）
    service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID)
    assert (
        demo_db.conn.execute(
            "SELECT COUNT(*) AS c FROM payment_operations WHERE business_key = ?",
            (f"refund:{root_id}:child-return:{child_id}",),
        ).fetchone()["c"]
        == 1
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# 裁定無応答（X14「裁定応答なし」）
# ---------------------------------------------------------------------------


def test_unresponsive_arbiter_before_due_keeps_disputed(demo_db):
    """無応答 → due_at_us 前は Job DISPUTED・異議 OPEN・資金不変。"""
    root_id, child_id, _s, dispute_id = _disputed_child(demo_db, suffix="unresp")

    def _unresponsive(label):
        raise RuntimeError(f"injected arbiter outage: {label}")

    verification.failpoint_arbiter_unresponsive = _unresponsive
    results = service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID)
    assert len(results) == 1
    assert results[0].data["unresponsive"] is True
    assert results[0].data["skipped"] is True
    verification.failpoint_arbiter_unresponsive = None

    assert _job(demo_db, child_id)["state"] == JobState.DISPUTED.value
    dispute = demo_db.conn.execute(
        "SELECT * FROM disputes WHERE id = ?", (dispute_id,)
    ).fetchone()
    assert dispute["status"] == "OPEN"
    assert dispute["resolution"] is None
    buckets = _buckets(demo_db, root_id)
    assert buckets[(child_id, "child_work")] == TEN
    assert _payment_by_key(demo_db, f"payout:{child_id}") is None
    # resolve Operation は残らない（何も確定していない）
    assert (
        demo_db.conn.execute(
            "SELECT COUNT(*) AS c FROM operations WHERE operation_id = ?",
            (f"resolve:{dispute_id}",),
        ).fetchone()["c"]
        == 0
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_unresponsive_arbiter_after_due_adopts_stored_pass(demo_db):
    """無応答 → due_at_us 到達後は保存済み PASS を採用して DONE ＋
    Acceptance APPROVED ＋ 支払い予約（全 Version が
    unresponsive_arbiter_fallback='stored_pass' を事前記録。資金を永久
    凍結しない）。resolution は fallback であることを示す canonical JSON。"""
    root_id, child_id, _s, dispute_id = _disputed_child(demo_db, suffix="fb")
    due_at = demo_db.conn.execute(
        "SELECT due_at_us FROM disputes WHERE id = ?", (dispute_id,)
    ).fetchone()["due_at_us"]

    def _unresponsive(label):
        raise RuntimeError(f"injected arbiter outage: {label}")

    verification.failpoint_arbiter_unresponsive = _unresponsive
    # due_at_us 到達後まで進める
    clock.set_test_now(demo_db.conn, due_at)
    results = service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID)
    verification.failpoint_arbiter_unresponsive = None
    assert len(results) == 1
    assert (
        results[0].data["resolution"] == "UNRESPONSIVE_ARBITER_STORED_PASS"
    )

    job = _job(demo_db, child_id)
    assert job["state"] == JobState.DONE.value
    acceptance = demo_db.conn.execute(
        "SELECT * FROM acceptances WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert acceptance["decision"] == "APPROVED"
    payment = _payment_by_key(demo_db, f"payout:{child_id}")
    assert payment["status"] == PaymentStatus.PENDING.value
    assert payment["payee_id"] == AGENT_B_ID
    dispute = demo_db.conn.execute(
        "SELECT * FROM disputes WHERE id = ?", (dispute_id,)
    ).fetchone()
    assert dispute["status"] == "RESOLVED"
    # resolution は fallback であることを示す canonical JSON。証跡は保存済み
    # 検証証跡（submissions.verification_evidence）と一致する
    resolution = _resolution_record(demo_db, dispute_id)
    submission = demo_db.conn.execute(
        "SELECT * FROM submissions WHERE id = ?", (dispute["submission_id"],)
    ).fetchone()
    assert resolution["outcome"] == "UNRESPONSIVE_ARBITER_STORED_PASS"
    assert resolution["reason"] == "ARBITER_UNRESPONSIVE_STORED_PASS_FALLBACK"
    assert resolution["condition_id"] == dispute["condition_id"]
    assert resolution["evidence"] == str(submission["verification_evidence"])
    assert resolution["verifier_id"] == verification.VERIFIER_ID
    assert resolution["verifier_hash"] == verification.verifier_hash()
    assert resolution["input_hash"] == json.loads(
        submission["verification_evidence"]
    )["input_hash"]
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

def test_unresponsive_arbiter_past_due_converges_to_done(demo_db):
    """無応答のまま期限を越えたら必ず DONE ＋ Acceptance APPROVED ＋
    支払い予約へ収束し、資金が凍結されない（誤った「fallback しない」
    契約を固定していた旧テストの代替）。"""
    root_id, child_id, _s, dispute_id = _disputed_child(
        demo_db, suffix="conv"
    )
    due_at = demo_db.conn.execute(
        "SELECT due_at_us FROM disputes WHERE id = ?", (dispute_id,)
    ).fetchone()["due_at_us"]

    def _unresponsive(label):
        raise RuntimeError(f"injected arbiter outage: {label}")

    verification.failpoint_arbiter_unresponsive = _unresponsive
    # 期限を越えてもう一度実行しても必ず収束する
    clock.set_test_now(demo_db.conn, due_at + 1_000_000)
    results = service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID)
    verification.failpoint_arbiter_unresponsive = None
    assert len(results) == 1
    assert results[0].data["resolution"] == "UNRESPONSIVE_ARBITER_STORED_PASS"
    assert _job(demo_db, child_id)["state"] == JobState.DONE.value
    acceptance = demo_db.conn.execute(
        "SELECT * FROM acceptances WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert acceptance["decision"] == "APPROVED"
    payment = _payment_by_key(demo_db, f"payout:{child_id}")
    assert payment["status"] == PaymentStatus.PENDING.value
    assert payment["amount_units"] == TEN
    # 資金が凍結されていない（child_work は child_payout へ移動）
    buckets = _buckets(demo_db, root_id)
    assert buckets[(child_id, "child_work")] == 0
    assert buckets[(child_id, "child_payout")] == TEN
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

@pytest.mark.parametrize(
    ("offset", "expect_fallback"),
    [(-1, False), (0, True), (1, True)],
)
def test_unresponsive_arbiter_boundary_at_due(demo_db, offset, expect_fallback):
    """R4: 期限境界を固定 Clock で作り、due_at_us の直前・ちょうど・直後の
    3 点を確定させる。ちょうど（now == due_at_us）は fallback 適用側
    （失効側）を優先する。期限前は operations 行も残らない。"""
    root_id, child_id, _s, dispute_id = _disputed_child(
        demo_db, suffix=f"bnd{offset}"
    )
    due_at = demo_db.conn.execute(
        "SELECT due_at_us FROM disputes WHERE id = ?", (dispute_id,)
    ).fetchone()["due_at_us"]

    def _unresponsive(label):
        raise RuntimeError(f"injected arbiter outage: {label}")

    verification.failpoint_arbiter_unresponsive = _unresponsive
    clock.set_test_now(demo_db.conn, due_at + offset)
    results = service.resolve_due_disputes(demo_db.conn, actor_id=SYSTEM_ID)
    verification.failpoint_arbiter_unresponsive = None
    assert len(results) == 1
    if expect_fallback:
        assert (
            results[0].data["resolution"]
            == "UNRESPONSIVE_ARBITER_STORED_PASS"
        )
        assert _job(demo_db, child_id)["state"] == JobState.DONE.value
    else:
        assert results[0].data["unresponsive"] is True
        assert results[0].data["skipped"] is True
        assert _job(demo_db, child_id)["state"] == JobState.DISPUTED.value
        # 期限前は operations 行も残らない（transaction 全体が rollback）
        assert (
            demo_db.conn.execute(
                "SELECT COUNT(*) AS c FROM operations WHERE operation_id = ?",
                (f"resolve:{dispute_id}",),
            ).fetchone()["c"]
            == 0
        )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

def test_unresponsive_arbiter_fallback_is_always_stored_pass(demo_db):
    """R3: TimingPolicy.unresponsive_arbiter_fallback は Literal['stored_pass']
    （None 不可・既定 stored_pass）。キー欠落・None の timing_policy JSON
    （後方互換）は "stored_pass" として扱い、例外にしない。"""
    import json as _json

    from ojp.domain import TimingPolicy

    # 既定値
    assert TimingPolicy().unresponsive_arbiter_fallback == "stored_pass"
    # キー欠落の JSON（古い DB の timing_policy）→ stored_pass
    legacy_json = _json.dumps(
        {
            "lease_seconds": 60,
            "heartbeat_seconds": 20,
            "review_window_seconds": 30,
            "dispute_window_seconds": 30,
        }
    )
    assert (
        TimingPolicy.model_validate_json(legacy_json).unresponsive_arbiter_fallback
        == "stored_pass"
    )
    # None（旧バージョンで保存された値）→ stored_pass として扱う
    none_json = _json.dumps(
        {
            "lease_seconds": 60,
            "heartbeat_seconds": 20,
            "review_window_seconds": 30,
            "dispute_window_seconds": 30,
            "unresponsive_arbiter_fallback": None,
        }
    )
    assert (
        TimingPolicy.model_validate_json(none_json).unresponsive_arbiter_fallback
        == "stored_pass"
    )
    # その他の値は拒否
    import pytest as _pytest

    from pydantic import ValidationError

    with _pytest.raises(ValidationError):
        TimingPolicy.model_validate_json(
            _json.dumps(
                {
                    "lease_seconds": 60,
                    "heartbeat_seconds": 20,
                    "review_window_seconds": 30,
                    "dispute_window_seconds": 30,
                    "unresponsive_arbiter_fallback": "freeze_forever",
                }
            )
        )
    # （DB の job_versions 行は公開後 immutable なため、キー欠落の実データ
    # は作れない。後方互換の読み取りは上の model_validate_json で検証する）

# ---------------------------------------------------------------------------
# seam の realtime 拒否
# ---------------------------------------------------------------------------


def test_arbitration_seams_rejected_on_realtime_db(realtime_db):
    """裁定 seam を realtime DB で使うと、resolve_due_disputes が何も
    動かさずに拒否する（test mode 専用）。"""
    from ojp.domain import ParticipantKind
    from tests.conftest import insert_participant

    conn = realtime_db.conn
    with db.transaction(conn, immediate=True):
        insert_participant(conn, SYSTEM_ID, ParticipantKind.SYSTEM)
    verification.arbiter_result_override = lambda label: _fail_outcome()
    try:
        with pytest.raises(OjpError) as exc_info:
            service.resolve_due_disputes(conn, actor_id=SYSTEM_ID)
        assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    finally:
        verification.arbiter_result_override = None
    # 何も書いていない
    assert conn.execute("SELECT COUNT(*) AS c FROM operations").fetchone()["c"] == 0


# ---------------------------------------------------------------------------
# 認可（system 専用）
# ---------------------------------------------------------------------------


def test_resolve_requires_system_actor(demo_db):
    """非 system Actor からの裁定は FORBIDDEN（対象が無くても）。"""
    _root, _child, _s, _d = _disputed_child(demo_db)
    with pytest.raises(OjpError) as exc_info:
        service.resolve_due_disputes(demo_db.conn, actor_id=AGENT_A_ID)
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value


def test_approve_due_submissions_requires_system_actor(demo_db):
    """approve_due_submissions も system 専用。"""
    with pytest.raises(OjpError) as exc_info:
        service.approve_due_submissions(demo_db.conn, actor_id=AGENT_A_ID)
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
