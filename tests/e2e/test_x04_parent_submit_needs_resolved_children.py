"""X04: Child が未判定の間の Parent 提出（計画書 第18節 追加検証 X04）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X04 | part-1がLEASEDの間にParent提出 | CHILDREN_UNRESOLVED、タイマーなし。Child判定後に提出できる |

第8節「Parent 提出は全 Child が判定上の終端状態になってから」／第11節 手順
2-5 の境界を実プロセスで観測する:

1. part-1 が **LEASED**（B が Claim 済み・未提出）の間に A が Root へ提出する
   と `CHILDREN_UNRESOLVED`。Root は LEASED のままで、有効 Submission・
   Attempt・Acceptance・支払い予約はどれも作られない
2. **タイマーなし**: 共有 Clock を検収窓（30 秒）より後へ進めて独立 tick を
   回しても、Root の自動承認・支払い予約は起きない（拒否された提出が
   review_due_at を作っていない）
3. part-1 が **SUBMITTED**（有効提出済み・未判定）でも同じく
   `CHILDREN_UNRESOLVED`。終端条件は「判定上の終端状態」であり提出完了では
   ない
4. part-1 が **DONE**（A が承認）になった後は Root へ提出できる。以後は
   Requester 承認・決済まで通常経路で進む

拒否の前後で Job 状態・Submission・Acceptance・口座残高・Journal・
PaymentOperation・Receipt を比較し、変化を認めない。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "X04"

CHILD_BUDGET_UNITS = 10_000_000
PARENT_PAYOUT_UNITS = 90_000_000
# Root カードの review_window_seconds=30 より後へ進めるための増分
PAST_REVIEW_WINDOW_US = 31_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x04_parent_submit_rejected_until_children_are_resolved(
    tmp_path: Path,
) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_x04(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_x04(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    agent_a_wallet_baseline = after_fund.wallet_units[AGENT_A_ID]

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

            # ----------------------------------------------------------
            # 1. part-1 が LEASED の間の Parent 提出は CHILDREN_UNRESOLVED
            # ----------------------------------------------------------
            before_leased = world.observe("before parent submit (child LEASED)")
            assert before_leased.job_states[child_id] == "LEASED"
            assert before_leased.active_lease_worker(child_id) == AGENT_B_ID
            assert before_leased.job_states[root_id] == "LEASED"

            rejected = await agent_a.call(
                "ojp_submit",
                {
                    "job_id": root_id,
                    "lease_id": start.root_lease_id,
                    "version_id": start.root_version_id,
                    "artifact_json": json.dumps({"sum": 15}),
                    "operation_id": world.next_operation_id(
                        "submit", "a-root-child-leased"
                    ),
                },
                expect_ok=False,
            )
            assert rejected.error_code == "CHILDREN_UNRESOLVED", rejected.payload

            after_leased_rejection = world.observe(
                "after rejected parent submit (child LEASED)"
            )
            _assert_unchanged(before_leased, after_leased_rejection)
            _assert_no_parent_review_timer(after_leased_rejection, root_id)
            assert after_leased_rejection.active_lease_worker(root_id) == AGENT_A_ID

            # ----------------------------------------------------------
            # 2. タイマーなし: 検収窓より後へ進めて tick しても何も起きない
            # ----------------------------------------------------------
            world.advance_clock_to(
                world.current_clock_us() + PAST_REVIEW_WINDOW_US
            )
            world.tick_once(label="tick --once (past review window)")
            after_tick = world.observe("after tick past the review window")
            _assert_unchanged(before_leased, after_tick)
            _assert_no_parent_review_timer(after_tick, root_id)

            # ----------------------------------------------------------
            # 3. part-1 が SUBMITTED（未判定）でも CHILDREN_UNRESOLVED
            # ----------------------------------------------------------
            submit_child = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": child_id,
                    "lease_id": start.child_lease_id,
                    "version_id": start.child_version_id,
                    "artifact_json": json.dumps({"sum": 6}),
                    "operation_id": world.next_operation_id("submit", "b-child"),
                },
            )
            child_submission_id = str(submit_child.data["submission_id"])
            before_submitted = world.observe("before parent submit (child SUBMITTED)")
            assert before_submitted.job_states[child_id] == "SUBMITTED"

            rejected_again = await agent_a.call(
                "ojp_submit",
                {
                    "job_id": root_id,
                    "lease_id": start.root_lease_id,
                    "version_id": start.root_version_id,
                    "artifact_json": json.dumps({"sum": 15}),
                    "operation_id": world.next_operation_id(
                        "submit", "a-root-child-submitted"
                    ),
                },
                expect_ok=False,
            )
            assert rejected_again.error_code == "CHILDREN_UNRESOLVED", (
                rejected_again.payload
            )
            after_submitted_rejection = world.observe(
                "after rejected parent submit (child SUBMITTED)"
            )
            _assert_unchanged(before_submitted, after_submitted_rejection)
            _assert_no_parent_review_timer(after_submitted_rejection, root_id)

            # ----------------------------------------------------------
            # 4. Child 判定後（DONE）は Parent 提出できる
            # ----------------------------------------------------------
            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": child_id,
                    "submission_id": child_submission_id,
                    "operation_id": world.next_operation_id("approve", "a-child"),
                },
            )
            after_child_done = world.observe("after child approve by A")
            assert after_child_done.job_states[child_id] == "DONE"

            submit_root = await agent_a.call(
                "ojp_submit",
                {
                    "job_id": root_id,
                    "lease_id": start.root_lease_id,
                    "version_id": start.root_version_id,
                    "artifact_json": json.dumps({"sum": 15}),
                    "operation_id": world.next_operation_id("submit", "a-root"),
                },
            )
            root_submission_id = str(submit_root.data["submission_id"])
            after_root_submit = world.observe("after root submit by A")
            assert after_root_submit.job_states[root_id] == "SUBMITTED"
            root_submissions = [
                s for s in after_root_submit.submissions if s["job_id"] == root_id
            ]
            assert len(root_submissions) == 1
            # 判定後の提出で初めて検収タイマーが作られる
            assert int(root_submissions[0]["review_due_at_us"]) > world.current_clock_us()

    # ------------------------------------------------------------------
    # 通常経路で Requester 承認・決済まで進む
    # ------------------------------------------------------------------
    world.run_cli(
        ["job", "approve", root_id, "--submission", root_submission_id],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("approve", "r-root"),
        action="job approve (root)",
    )
    world.observe("after root approve by Requester")
    world.settle()
    final = world.observe("after settlement")

    assert final.job_states[root_id] == "DONE"
    assert final.job_states[child_id] == "DONE"
    assert final.paid_to(AGENT_A_ID) == PARENT_PAYOUT_UNITS
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.refunded_units == 0
    assert final.escrow_units == 0
    assert (
        final.wallet_units[AGENT_A_ID]
        == agent_a_wallet_baseline + PARENT_PAYOUT_UNITS
    )
    assert final.submission_worker(root_id) == AGENT_A_ID
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert harness.check_payee_entitlement(final) == []

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label

    world.report.note(
        "X04: part-1 が LEASED / SUBMITTED（未判定）の間の Parent 提出は"
        " CHILDREN_UNRESOLVED で、有効 Submission も検収タイマーも作られない。"
        "検収窓より後へ進めて tick しても Root の自動承認は起きず、Child が"
        " DONE になった後に初めて Parent 提出が成立する"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {root_id: "DONE", child_id: "DONE"},
        "paid": {AGENT_A_ID: "90.000000", AGENT_B_ID: "10.000000"},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


def _assert_no_parent_review_timer(
    snapshot: harness.Snapshot, root_id: str
) -> None:
    """Root に有効 Submission・Attempt・Acceptance・支払い予約が無い（タイマーなし）。"""
    assert [s for s in snapshot.submissions if s["job_id"] == root_id] == []
    assert snapshot.attempts_for(root_id) == []
    assert [a for a in snapshot.acceptances if a["job_id"] == root_id] == []
    assert snapshot.operations_for(root_id, "payout") == []
    assert snapshot.operations_for(root_id, "refund") == []
    assert snapshot.locked_for("parent_payout") == 0
    assert snapshot.job_states[root_id] == "LEASED"


def _assert_unchanged(
    before: harness.Snapshot, after: harness.Snapshot
) -> None:
    """Job 状態・Submission・Acceptance・Lease・口座・Journal・予約・Receipt が不変。"""
    assert after.job_states == before.job_states
    assert after.submissions == before.submissions
    assert after.acceptances == before.acceptances
    assert after.leases == before.leases
    assert after.accounts == before.accounts
    assert after.journal == before.journal
    assert after.operations == before.operations
    assert after.receipts == before.receipts
    assert after.wallet_units == before.wallet_units
    assert after.submission_attempts == before.submission_attempts
