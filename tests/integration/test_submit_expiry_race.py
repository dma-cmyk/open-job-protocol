"""Phase 4 S1 の integration テスト: submit と expire_due_leases の直列化
（計画書 第7節・N08・X09）。

Done when の対応:
- 5（submit と expiry の競合が計画書 第7節の境界どおりに直列化されている）:
  先に有効 submit が commit したら以後の expiry は何もしない /
  先に expiry が確定したら submit を拒否する

実ファイル SQLite（test mode の共有 Clock）で検証する。N08 と同じ考え方で、
有効提出後・検収期限前に旧 Lease 期限だけを越えた時刻へ Clock を進め、
expire_due_leases が SUBMITTED を OPEN / EXPIRED へ戻さないことを確認する。

競合の対象は「同じ Job の Lease に対する submit と expiry」であり、create_child
は timing を Root 公開版から継承するため、ここでは Root 単独（Child なし）で
Lease 期限と review_due_at の分離を作る（timing は Root 公開時に固定される）。
"""

from __future__ import annotations

import pytest

from ojp import clock, db, ledger, service, verification
from ojp.domain import (
    ErrorCode,
    JobState,
    OjpError,
    SubcontractPolicy,
    TimingPolicy,
)
from tests.conftest import (
    AGENT_A_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    TEST_T0_US,
    insert_demo_participants,
    load_poc_catalog,
)

DEADLINE_US = TEST_T0_US + 86_400_000_000  # t0 + 1日

# Lease 30秒・検収 120秒: 有効提出（t0+1秒）で review_due_at = t0+121秒と
# なり、旧 Lease 期限（t0+30秒）だけを先に越えられる時刻構成
RACE_TIMING = TimingPolicy(
    lease_seconds=30, heartbeat_seconds=20,
    review_window_seconds=120, dispute_window_seconds=30,
)


@pytest.fixture
def demo_db(test_db):
    """固定 Actor + Wallet seed 済みの test mode DB（Worker も 0 で seed）。"""
    with db.transaction(test_db.conn, immediate=True):
        insert_demo_participants(test_db.conn)
        for participant_id, balance in (
            (REQUESTER_ID, ROOT_BUDGET_UNITS),
            (AGENT_A_ID, 0),
        ):
            ledger.seed_mock_wallet_for_demo(
                test_db.conn,
                participant_id=participant_id,
                asset="mock-USDC",
                balance_units=balance,
            )
    return test_db


def _leased_root(demo_db, *, suffix):
    """create_root → fund_root → A が Claim 済みの Root を返す
    （(root_id, version_id, lease_id)）。timing は RACE_TIMING。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=SubcontractPolicy(
            enabled=True,
            max_amount_units=30_000_000,
            max_ratio_bps=3000,
            max_children=3,
            max_depth=1,
        ),
        task_catalog=catalog,
        timing_policy=RACE_TIMING,
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


def _submit(demo_db, job_id, lease_id, version_id, artifact_json, op_id):
    return service.submit(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        lease_id=lease_id,
        version_id=version_id,
        artifact_json=artifact_json,
        operation_id=op_id,
    )


def _wallets(conn):
    return {
        row["participant_id"]: int(row["balance_units"])
        for row in conn.execute("SELECT * FROM mock_wallets")
    }


# ---------------------------------------------------------------------------
# 先に有効 submit が commit した場合（N08・X09）
# ---------------------------------------------------------------------------


def test_expiry_after_valid_submit_does_nothing(demo_db):
    """有効 submit が先に commit した後、旧 Lease 期限より後・review_due_at
    より前へ Clock を進めて expire_due_leases を実行しても、Job は SUBMITTED
    を維持し OPEN / EXPIRED に戻らない。

    acceptances 0 件・payment_operations 0 件・全口座残高が提出直後から不変・
    leases.expires_at_us は監査値として元の値のまま・closed_reason は
    'submitted' のまま・expiry による新しい Lease や Event が作られない。
    """
    root_id, version_id, lease_id = _leased_root(demo_db, suffix="n08")
    lease_expires_at = demo_db.conn.execute(
        "SELECT expires_at_us FROM leases WHERE id = ?", (lease_id,)
    ).fetchone()["expires_at_us"]
    assert lease_expires_at == TEST_T0_US + 30_000_000

    # 共有 Clock を t0+1秒へ進めて有効提出（review_due_at = t0+1+120秒）
    clock.set_test_now(demo_db.conn, TEST_T0_US + 1_000_000)
    result = _submit(
        demo_db, root_id, lease_id, version_id, '{"sum": 15}', "submit:n08"
    )
    review_due_at = result.data["review_due_at"]
    assert review_due_at == TEST_T0_US + 1_000_000 + 120_000_000
    wallets_after_submit = _wallets(demo_db.conn)
    events_after_submit = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM events WHERE job_id = ?", (root_id,)
    ).fetchone()["c"]

    # 旧 Lease 期限（t0+30秒）より後・review_due_at（t0+121秒）より前へ進める
    now = TEST_T0_US + 31_000_000
    assert lease_expires_at < now < review_due_at
    clock.set_test_now(demo_db.conn, now)
    results = service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)

    # SUBMITTED の Job は expiry の対象外。何もしない
    assert all(r.data["job_id"] != root_id for r in results)
    fresh = demo_db.fresh_conn()
    try:
        job = fresh.execute(
            "SELECT state FROM jobs WHERE id = ?", (root_id,)
        ).fetchone()
        assert job["state"] == JobState.SUBMITTED.value
        # 提出で active_lease_id は外れている
        # acceptances・支払い予約は 0 件
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM acceptances WHERE job_id = ?",
                (root_id,),
            ).fetchone()["c"]
            == 0
        )
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM payment_operations WHERE root_id = ?",
                (root_id,),
            ).fetchone()["c"]
            == 0
        )
        # Lease は監査値のまま・'submitted' で閉じたまま
        lease = fresh.execute(
            "SELECT * FROM leases WHERE id = ?", (lease_id,)
        ).fetchone()
        assert lease["expires_at_us"] == lease_expires_at
        assert lease["closed_reason"] == "submitted"
        # 全口座残高が提出直後から不変
        assert _wallets(fresh) == wallets_after_submit
        # expiry による新しい Lease や Event は作られない
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM leases WHERE job_id = ?",
                (root_id,),
            ).fetchone()["c"]
            == 1
        )
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM events WHERE job_id = ?",
                (root_id,),
            ).fetchone()["c"]
            == events_after_submit
        )
        ledger.assert_ledger_invariants(fresh, root_id)
    finally:
        fresh.close()


# ---------------------------------------------------------------------------
# 先に expiry が確定した場合（X09）
# ---------------------------------------------------------------------------


def test_submit_rejected_after_expiry_committed(demo_db):
    """先に expiry が確定したら（Clock を Lease 期限後へ進めて
    expire_due_leases を実行してから submit）、submit は LEASE_EXPIRED で
    拒否され、submissions 0 件。"""
    root_id, version_id, lease_id = _leased_root(demo_db, suffix="x09")
    # Clock を Lease 期限（t0+30秒）ちょうどへ進めて失効を確定させる
    clock.set_test_now(demo_db.conn, TEST_T0_US + 30_000_000)
    results = service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)
    assert any(r.data["job_id"] == root_id for r in results)
    job = demo_db.conn.execute(
        "SELECT state FROM jobs WHERE id = ?", (root_id,)
    ).fetchone()
    assert job["state"] == JobState.EXPIRED.value

    with pytest.raises(OjpError) as exc_info:
        _submit(
            demo_db, root_id, lease_id, version_id, '{"sum": 15}', "submit:x09"
        )
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value
    fresh = demo_db.fresh_conn()
    try:
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM submissions WHERE job_id = ?",
                (root_id,),
            ).fetchone()["c"]
            == 0
        )
        # 失効確定後の Job 状態は変わらない（提出で復活しない）
        assert (
            fresh.execute(
                "SELECT state FROM jobs WHERE id = ?", (root_id,)
            ).fetchone()["state"]
            == JobState.EXPIRED.value
        )
        ledger.assert_ledger_invariants(fresh, root_id)
    finally:
        fresh.close()
