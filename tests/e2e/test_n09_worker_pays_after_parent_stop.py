"""N09: A プロセス停止・Parent 失効後も B が受領する（計画書 第18節 異常系 9 / N09）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 9 / N09 | Aプロセス停止、Parent失効、B提出 | Bは固定検証と期限承認で10受領。Requesterは成果物取得可 |

手順:

1. A（MCP stdio サーバープロセス）が Root を Claim し、part-1 の Child を
   予算 10 で作成する。B が Child を Claim する
2. **A のプロセスを実際に停止する**（`agent_session` の context を抜けて
   stdio サーバープロセスを終了させ、終了を確認する）
3. 共有 Clock を Root Lease 期限の直後へ進め、独立 tick で **Parent を失効**
   させる（Child Lease はカタログ事前許可の長い Lease でまだ有効）
4. A が停止したまま B が Child へ有効提出する（固定検証 sum-v1 の PASS）
5. A は停止しているので手動承認は起きない。共有 Clock を `review_due_at`
   へ進め、独立 tick の**期限承認**で Child を DONE にして B へ 10 を送る
6. Root Requester が CLI で Child の成果物を取得できる
   （`artifact_access_policy.requester_can_read` の事前明記による）

最終: B=10 / A=0 / Requester 返金=90 / Escrow=0。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "N09"

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000
REFUND_UNITS = 90_000_000

ROOT_LEASE_SECONDS = 60
CHILD_LEASE_SECONDS = 600
CHILD_REVIEW_WINDOW_SECONDS = 30

ROOT_TIMING_POLICY = {
    "lease_seconds": ROOT_LEASE_SECONDS,
    "heartbeat_seconds": 20,
    "review_window_seconds": 30,
    "dispute_window_seconds": 30,
}
# Child だけ長い Lease を持たせる（Root Requester の事前許可。第8節）。
# これで「Parent Lease だけが失効し、Child Lease は生存する」状態を作れる。
CHILD_TIMING_POLICY = {
    "lease_seconds": CHILD_LEASE_SECONDS,
    "heartbeat_seconds": 20,
    "review_window_seconds": CHILD_REVIEW_WINDOW_SECONDS,
    "dispute_window_seconds": 30,
}
FAR_DEADLINE = "2027-01-16T00:00:00.000000Z"

CHILD_ARTIFACT = {"sum": 6}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n09_worker_paid_after_parent_stop_and_expiry(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_n09(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_n09(world: harness.E2EWorld) -> dict:
    card_path = harness.write_root_card(
        world,
        "n09",
        timing_policy=ROOT_TIMING_POLICY,
        catalog_timing_policy=CHILD_TIMING_POLICY,
        deadline=FAR_DEADLINE,
    )
    root_id, _ = harness.create_and_fund_root(world, card_path=card_path)
    after_fund = world.observe("after root fund")
    agent_a_wallet_baseline = after_fund.wallet_units[AGENT_A_ID]
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]
    requester_wallet_baseline = after_fund.wallet_units[REQUESTER_ID]

    t0_us = world.current_clock_us()

    async with harness.agent_session(world, AGENT_B_ID) as agent_b:
        # A のセッションは Child 作成までで終える（このあと停止させる）
        async with harness.agent_session(world, AGENT_A_ID) as agent_a:
            start = await harness.start_root_child_part1(
                world, agent_a, agent_b, child_deadline=FAR_DEADLINE
            )
            child_id = start.child_id
            root_lease_expires_us = world.lease_expires_at_us(start.root_lease_id)
            child_lease_expires_us = world.lease_expires_at_us(start.child_lease_id)
            assert root_lease_expires_us == t0_us + ROOT_LEASE_SECONDS * 1_000_000
            assert child_lease_expires_us == t0_us + CHILD_LEASE_SECONDS * 1_000_000
        # ここで A の MCP stdio サーバープロセスは終了している（context 退出）

        # --------------------------------------------------------------
        # Parent（Root）を Lease 失効させる。Child Lease はまだ有効
        # --------------------------------------------------------------
        world.advance_clock_to(root_lease_expires_us + 1_000_000)
        now_us = world.current_clock_us()
        assert root_lease_expires_us < now_us < child_lease_expires_us

        world.run_tick_until(
            lambda conn: conn.execute(
                "SELECT state FROM jobs WHERE id = ?", (root_id,)
            ).fetchone()["state"]
            in harness.TERMINAL_JOB_STATES,
            label="tick --watch (root lease expiry after A stopped)",
        )
        after_parent_fail = world.observe("after root expiry (A stopped)")
        assert after_parent_fail.job_states[root_id] == "EXPIRED"
        # Parent 終端を Child へ伝播しない: Child は LEASED のまま 10 を保持
        assert after_parent_fail.job_states[child_id] == "LEASED"
        assert after_parent_fail.active_lease_worker(child_id) == AGENT_B_ID
        assert after_parent_fail.locked_for("child_work") == CHILD_BUDGET_UNITS
        # 先行返金は未拘束の 90 だけ
        refund_ops = after_parent_fail.operations_for(root_id, "refund")
        assert len(refund_ops) == 1
        assert refund_ops[0]["amount"] == "90.000000"

        # --------------------------------------------------------------
        # A が停止したまま B が Child へ有効提出する（固定検証 sum-v1）
        # --------------------------------------------------------------
        submit = await agent_b.call(
            "ojp_submit",
            {
                "job_id": child_id,
                "lease_id": start.child_lease_id,
                "version_id": start.child_version_id,
                "artifact_json": json.dumps(CHILD_ARTIFACT),
                "operation_id": world.next_operation_id("submit", "b-child"),
            },
        )
        submission_id = str(submit.data["submission_id"])
        assert submit.data["verification"] == "PASS"

    after_submit = world.observe("after child submit by B (A stopped)")
    assert after_submit.job_states[child_id] == "SUBMITTED"
    assert after_submit.acceptances == []
    child_submission = _only(after_submit.submissions, child_id)
    review_due_at_us = int(child_submission["review_due_at_us"])

    # ------------------------------------------------------------------
    # 期限承認（A は停止しているので手動承認は起きない）
    # ------------------------------------------------------------------
    world.advance_clock_to(review_due_at_us)
    world.run_tick_until(
        lambda conn: conn.execute(
            "SELECT state FROM jobs WHERE id = ?", (child_id,)
        ).fetchone()["state"]
        == "DONE",
        label="tick --watch (child deadline approval)",
    )
    after_auto_approve = world.observe("after child deadline approval")
    child_acceptances = [
        a for a in after_auto_approve.acceptances if a["job_id"] == child_id
    ]
    assert len(child_acceptances) == 1
    assert child_acceptances[0]["decision"] == "APPROVED"
    # A ではなく system（期限承認）が決定している
    assert child_acceptances[0]["decided_by"] == harness.SYSTEM_ID
    assert child_acceptances[0]["submission_id"] == submission_id

    world.settle()
    final = world.observe("after settlement")

    # -- B は 10 を受領、A は 0、Requester は 90 --------------------------
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.refunded_to(REQUESTER_ID) == REFUND_UNITS
    assert final.job_states[root_id] == "EXPIRED"
    assert final.job_states[child_id] == "DONE"
    assert final.escrow_units == 0
    assert final.locked_units == 0
    assert final.available_units == 0
    assert (
        final.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    assert final.wallet_units[AGENT_A_ID] == agent_a_wallet_baseline
    assert (
        final.wallet_units[REQUESTER_ID] == requester_wallet_baseline + REFUND_UNITS
    )
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert harness.check_payee_entitlement(final) == []

    # ------------------------------------------------------------------
    # Requester は成果物を取得できる（A 停止・Parent 終了後も）
    # ------------------------------------------------------------------
    fetched = world.run_cli(
        ["job", "get", child_id],
        actor=REQUESTER_ID,
        action="job get (child artifact by Requester)",
    )
    submission_view = fetched.data["submission"]
    assert submission_view["artifact_readable"] is True
    assert json.loads(submission_view["artifact_json"]) == CHILD_ARTIFACT
    assert submission_view["artifact_hash"] == child_submission["artifact_hash"]
    assert submission_view["verification_result"] == "PASS"

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label

    world.report.note(
        "N09: A の MCP プロセス停止・Root Lease 失効後も、B は固定検証 PASS と"
        "期限承認（decided_by=system）で 10 を受領。Root Requester は Child の"
        "成果物を取得できる。A=0 / B=10 / 返金=90 / Escrow=0"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {root_id: "EXPIRED", child_id: "DONE"},
        "paid": {AGENT_B_ID: "10.000000"},
        "refunded": {REQUESTER_ID: "90.000000"},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


def _only(rows: list[dict], job_id: str) -> dict:
    matching = [row for row in rows if row["job_id"] == job_id]
    assert len(matching) == 1, matching
    return matching[0]
