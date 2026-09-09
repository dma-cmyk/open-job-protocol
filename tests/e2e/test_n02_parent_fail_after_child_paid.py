"""N02: B へ 10 支払い後に Parent 失敗（計画書 第18節 異常系 2 / N02）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 2 / N02 | Bへ10支払い後、Parent失敗 | B=10維持、A=0、Requester=90 |

「Parent 失敗」は第18節どおり**提出前 abandon**で再現する（E03 が expiry
経路を扱うので、N02 は abandon 経路を通す。不正な一方的返金では代替しない）。

確定済みの B への 10 は取り戻されない（返金原資は Root の未拘束 available
90 だけ）。A は Parent 提出前に失敗しているので受取 0。

実プロセス構成: Requester は CLI 子プロセス、A / B は別々の MCP stdio
サーバープロセス、決済は独立 tick プロセス。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "N02"

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000
REFUND_UNITS = 90_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n02_parent_fail_after_child_paid(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_n02(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_n02(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    agent_a_wallet_baseline = after_fund.wallet_units[AGENT_A_ID]
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]
    requester_wallet_baseline = after_fund.wallet_units[REQUESTER_ID]

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

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
            world.observe("after child submit by B")

            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": child_id,
                    "submission_id": child_submission_id,
                    "operation_id": world.next_operation_id("approve", "a-child"),
                },
            )
            world.observe("after child approve by A")

    # ------------------------------------------------------------------
    # B への 10 を確定させる（独立 tick）。ここまでが「B へ 10 支払い後」
    # ------------------------------------------------------------------
    world.settle()
    after_child_payment = world.observe("after child payment")
    assert after_child_payment.job_states[child_id] == "DONE"
    assert after_child_payment.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert after_child_payment.locked_units == 0
    assert after_child_payment.available_units == REFUND_UNITS
    assert (
        after_child_payment.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    assert after_child_payment.job_states[root_id] == "LEASED"

    # ------------------------------------------------------------------
    # Parent（Root）を提出前に失敗させる: A が CLI で abandon
    # ------------------------------------------------------------------
    world.run_cli(
        ["job", "abandon", root_id, "--lease", start.root_lease_id],
        actor=AGENT_A_ID,
        operation_id=world.next_operation_id("abandon", "a-root"),
        action="job abandon (root)",
    )
    after_parent_fail = world.observe("after root abandon by A")
    assert after_parent_fail.job_states[root_id] == "FAILED"
    # 第9節「Parent failure: available -90 / refund +90」。確定済みの
    # B への 10 は返金原資に入らない
    assert after_parent_fail.journal_total("refund", reason="reserve") == REFUND_UNITS
    assert (
        after_parent_fail.journal_total("available", reason="reserve") == -REFUND_UNITS
    )
    refund_ops = after_parent_fail.operations_for(root_id, "refund")
    assert len(refund_ops) == 1
    assert refund_ops[0]["payee_id"] == REQUESTER_ID
    assert refund_ops[0]["amount"] == "90.000000"
    # 確定済みの支払いは取り戻されない
    assert after_parent_fail.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert (
        after_parent_fail.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )

    # 返金予約を独立 tick で決済する
    world.settle()
    final = world.observe("after refund settlement")

    # -- N02 の期待結果: B=10 維持 / A=0 / Requester=90 ------------------
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.refunded_to(REQUESTER_ID) == REFUND_UNITS
    assert final.paid_units == CHILD_BUDGET_UNITS
    assert final.refunded_units == REFUND_UNITS
    assert final.escrow_units == 0
    assert final.available_units == 0
    assert final.locked_units == 0
    assert final.job_states[root_id] == "FAILED"
    assert final.job_states[child_id] == "DONE"

    # Wallet でも同じ結論（B は +10 のまま、A は不変、Requester は +90）
    assert (
        final.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    assert final.wallet_units[AGENT_A_ID] == agent_a_wallet_baseline
    assert (
        final.wallet_units[REQUESTER_ID] == requester_wallet_baseline + REFUND_UNITS
    )

    # 受取権者との一致（保存則とは独立に導出する）
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert final.submission_worker(root_id) is None  # A は提出していない
    assert final.paid_by_payee_units == {AGENT_B_ID: CHILD_BUDGET_UNITS}
    assert final.refunded_by_payee_units == {REQUESTER_ID: REFUND_UNITS}
    assert harness.check_payee_entitlement(final) == []

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label

    world.report.note(
        "N02: B へ 10 支払い後に A が Root を提出前 abandon。"
        "B=10 維持 / A=0 / Requester 返金=90 / Escrow=0"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {root_id: "FAILED", child_id: "DONE"},
        "paid": {AGENT_B_ID: "10.000000"},
        "refunded": {REQUESTER_ID: "90.000000"},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }
