"""Phase 4 S3 の unit テスト: 有効提出後の一方的な失敗と直接返金の拒否
（計画書 第18節 N04 の具体的呼び出し 6 行 ＋ Root 自身の有効提出後の 2 行）。

N04 の前提となる固定状況: Parent は有効 LEASED、Child は part-1・予算 10・
B の有効提出による SUBMITTED、返金予約なし、検収期限前の固定時刻。
拒否確認中は tick を進めず、各呼び出しに新しい operation_id を使う。

各呼び出しの前後で、Job 状態・有効 Submission・Acceptance・全口座残高・
Journal・PaymentOperation・Receipt を比較して変化が無いことを確認する
（拒否の Operation 結果・監査記録の追加だけは許容）。

状態は create_root → fund_root → claim → create_child → submit の
実経路だけで作る。
"""

from __future__ import annotations

import pytest

from ojp import db, ledger, service, verification
from ojp.domain import (
    ErrorCode,
    JobState,
    OjpError,
)
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
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
    Requester は 200（Root 2 件分）を持たせておき、別 fixture 用の Root を
    作れるようにする。"""
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


def _n04_world(demo_db, *, suffix="n04"):
    """N04 の固定状況を作る（(root_id, child_id, child_lease_id, root_lease_id)）。

    - Parent（Root）は有効 LEASED（A の Lease が有効）
    - Child は part-1・予算 10・B の有効提出による SUBMITTED
    - 返金予約なし
    - 検収期限前の固定時刻（TEST_T0_US のまま tick を進めない）
    """
    root_id, _root_v, root_lease = _leased_root(demo_db, suffix=f"{suffix}-r")
    created = service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease,
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id=f"create:{suffix}-c1",
    )
    child_id = created.data["child_id"]
    claimed = service.claim(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=created.data["version_id"],
        operation_id=f"claim:{suffix}-c1-b",
    )
    child_lease = claimed.data["lease_id"]
    service.submit(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease,
        version_id=created.data["version_id"],
        artifact_json='{"sum": 6}',
        operation_id=f"submit:{suffix}-c1",
    )
    return root_id, child_id, child_lease, root_lease


def _submitted_root_world(demo_db, *, suffix="root-sub"):
    """Root 自身の有効提出後の固定状況を作る
    （(root_id, root_lease_id)）。全 Child 判定終端・Root SUBMITTED・返金予約なし。"""
    root_id, root_v, root_lease = _leased_root(demo_db, suffix=f"{suffix}-r")
    # Child を 1 件だけ作って B が abandon（判定終端 FAILED）にする
    created = service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease,
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id=f"create:{suffix}-c1",
    )
    child_id = created.data["child_id"]
    claimed = service.claim(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=created.data["version_id"],
        operation_id=f"claim:{suffix}-c1-b",
    )
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=claimed.data["lease_id"],
        operation_id=f"abandon:{suffix}-c1",
    )
    # 全 Child 判定終端（Child は abandon で available へ返却）→ Root 提出
    service.submit(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=root_lease,
        version_id=root_v,
        artifact_json='{"sum": 15}',
        operation_id=f"submit:{suffix}",
    )
    return root_id, root_lease


def _protected_state_snapshot(conn):
    """N04 で不変を確認する表のスナップショット。Job 状態・有効 Submission・
    Acceptance・全口座残高・Journal・PaymentOperation・Receipt。
    拒否の Operation 結果・監査記録（events）の追加は許容するため
    operations / events は含めない。"""

    def _rows(sql):
        return tuple(tuple(r) for r in conn.execute(sql).fetchall())

    return (
        _rows("SELECT * FROM jobs ORDER BY id"),
        _rows("SELECT * FROM leases ORDER BY id"),
        _rows("SELECT * FROM submissions ORDER BY id"),
        _rows("SELECT * FROM acceptances ORDER BY job_id, submission_id"),
        _rows("SELECT * FROM budget_accounts ORDER BY id"),
        _rows("SELECT * FROM journal_transactions ORDER BY operation_id"),
        _rows("SELECT * FROM journal_entries ORDER BY operation_id, entry_no"),
        _rows("SELECT * FROM payment_operations ORDER BY operation_id"),
        _rows("SELECT * FROM transfer_receipts ORDER BY operation_id"),
        _rows("SELECT * FROM mock_wallets ORDER BY participant_id"),
    )


# ---------------------------------------------------------------------------
# N04 の具体的呼び出し（6 行）
# ---------------------------------------------------------------------------


def test_n04_six_rejected_calls_keep_state_unchanged(demo_db):
    """N04 の 6 行: いずれも拒否され、Job 状態・有効 Submission・Acceptance・
    全口座残高・Journal・PaymentOperation・Receipt が前後で不変。

    | 呼び出し                    | 期待コード      |
    | A が Child を refund         | FORBIDDEN      |
    | A が Root を refund          | FORBIDDEN      |
    | A が Child を abandon（B の Lease 指定） | FORBIDDEN |
    | B が Child を abandon（B の Lease 指定） | INVALID_STATE |
    | R が Child を refund         | INVALID_TARGET |
    | R が Root を refund          | INVALID_STATE  |
    """
    root_id, child_id, b_lease, _root_lease = _n04_world(demo_db, suffix="n04")
    # 前提の固定状況を確認
    assert (
        demo_db.conn.execute(
            "SELECT state FROM jobs WHERE id = ?", (root_id,)
        ).fetchone()["state"]
        == JobState.LEASED.value
    )
    child = demo_db.conn.execute(
        "SELECT state FROM jobs WHERE id = ?", (child_id,)
    ).fetchone()
    assert child["state"] == JobState.SUBMITTED.value
    assert (
        demo_db.conn.execute(
            "SELECT COUNT(*) AS c FROM payment_operations WHERE root_id = ?",
            (root_id,),
        ).fetchone()["c"]
        == 0
    )

    calls = [
        # (actor_id, kind, job_id, lease_id, operation_id, expected_code)
        (AGENT_A_ID, "refund", child_id, None, "refund:n04-a-child",
         ErrorCode.FORBIDDEN.value),
        (AGENT_A_ID, "refund", root_id, None, "refund:n04-a-root",
         ErrorCode.FORBIDDEN.value),
        (AGENT_A_ID, "abandon", child_id, b_lease, "abandon:n04-a-child",
         ErrorCode.FORBIDDEN.value),
        (AGENT_B_ID, "abandon", child_id, b_lease, "abandon:n04-b-child",
         ErrorCode.INVALID_STATE.value),
        (REQUESTER_ID, "refund", child_id, None, "refund:n04-r-child",
         ErrorCode.INVALID_TARGET.value),
        (REQUESTER_ID, "refund", root_id, None, "refund:n04-r-root",
         ErrorCode.INVALID_STATE.value),
    ]
    for actor_id, kind, job_id, lease_id, op_id, expected_code in calls:
        before = _protected_state_snapshot(demo_db.conn)
        with pytest.raises(OjpError) as exc_info:
            if kind == "refund":
                service.refund(
                    demo_db.conn,
                    actor_id=actor_id,
                    job_id=job_id,
                    operation_id=op_id,
                )
            else:
                service.abandon(
                    demo_db.conn,
                    actor_id=actor_id,
                    job_id=job_id,
                    lease_id=lease_id,
                    operation_id=op_id,
                )
        assert exc_info.value.code == expected_code, (
            f"{actor_id} {kind} {job_id}: {exc_info.value.code}"
            f" (expected {expected_code})"
        )
        after = _protected_state_snapshot(demo_db.conn)
        assert after == before, (
            f"{actor_id} {kind} {job_id} changed protected state"
        )
        ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Root 自身の有効提出後（別 fixture の 2 行）
# ---------------------------------------------------------------------------


def test_n04_root_submission_rejects_refund_and_abandon(demo_db):
    """Root 自身の有効提出後（全 Child 判定終端・Root SUBMITTED・返金予約なし）:
    R の refund Root と A の abandon Root（A の Lease 指定）がどちらも
    INVALID_STATE。保護対象の全表が前後で不変。"""
    root_id, a_lease = _submitted_root_world(demo_db, suffix="n04root")
    assert (
        demo_db.conn.execute(
            "SELECT state FROM jobs WHERE id = ?", (root_id,)
        ).fetchone()["state"]
        == JobState.SUBMITTED.value
    )
    assert (
        demo_db.conn.execute(
            "SELECT COUNT(*) AS c FROM payment_operations WHERE root_id = ?",
            (root_id,),
        ).fetchone()["c"]
        == 0
    )

    calls = [
        (REQUESTER_ID, "refund", root_id, None, "refund:n04root-r"),
        (AGENT_A_ID, "abandon", root_id, a_lease, "abandon:n04root-a"),
    ]
    for actor_id, kind, job_id, lease_id, op_id in calls:
        before = _protected_state_snapshot(demo_db.conn)
        with pytest.raises(OjpError) as exc_info:
            if kind == "refund":
                service.refund(
                    demo_db.conn,
                    actor_id=actor_id,
                    job_id=job_id,
                    operation_id=op_id,
                )
            else:
                service.abandon(
                    demo_db.conn,
                    actor_id=actor_id,
                    job_id=job_id,
                    lease_id=lease_id,
                    operation_id=op_id,
                )
        assert exc_info.value.code == ErrorCode.INVALID_STATE.value, (
            f"{actor_id} {kind} {job_id}: {exc_info.value.code}"
        )
        assert _protected_state_snapshot(demo_db.conn) == before
        ledger.assert_ledger_invariants(demo_db.conn, root_id)
