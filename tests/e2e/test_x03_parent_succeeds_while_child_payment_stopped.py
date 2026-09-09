"""X03: Child 送金停止中の Parent 成功（計画書 第18節 追加検証 X03）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X03 | Child送金停止中にParent成功 | Parent判定・90支払いは可能。Bの10はlockedに残り復旧後1回支払う |

Child（part-1・予算 10）の送金を **commit 前で停止**させたまま Parent（Root）を
成功させる。実プロセスの流れ:

1. B の有効提出を A が承認し、`child_payout` に 10 の送金予約ができる
2. `failpoint_before_commit` を立てた独立プロセスで送金を試み、exit 3 で
   異常終了させる。Receipt も台帳変化も残らず 10 は `child_payout` に留まる
3. **Parent 提出は妨げられない**（第11節 手順 2-5「Child が DONE で送金待ち
   でも Parent 提出は妨げない」）。A が Root へ提出し Requester が承認する
4. Root 承認は未拘束 available の 90 を `parent_payout` へ予約する。送金停止
   中の 10 を巻き込まないことを locked 内訳で確認する
5. A への 90 だけを `ojp payment retry`（実 CLI プロセス）で確定させる。
   この時点でも B の 10 は `child_payout` に残り、Receipt も 1 件もない
6. 復旧（独立 tick プロセス）で B の 10 が **1 回だけ**支払われ、Escrow は 0
   になる。再度 tick を回しても Receipt は増えない

停止中の 10 を Parent 側の支払い原資へ流用していないことを、locked 内訳・
Journal・`check_no_double_counting` の各観測点で照合する。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import failpoints, harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "X03"

CHILD_BUDGET_UNITS = 10_000_000
PARENT_PAYOUT_UNITS = 90_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x03_parent_succeeds_while_child_payment_is_stopped(
    tmp_path: Path,
) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_x03(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_x03(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    agent_a_wallet_baseline = after_fund.wallet_units[AGENT_A_ID]
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

            # ----------------------------------------------------------
            # 1. B の有効提出 → A の承認で child_payout 10 の予約ができる
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
            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": child_id,
                    "submission_id": str(submit_child.data["submission_id"]),
                    "operation_id": world.next_operation_id("approve", "a-child"),
                },
            )
            reserved = world.observe("after child approve by A")
            assert reserved.job_states[child_id] == "DONE"
            child_payouts = reserved.operations_for(child_id, "payout")
            assert len(child_payouts) == 1
            child_payment_id = str(child_payouts[0]["operation_id"])
            assert child_payouts[0]["status"] == "PENDING"
            assert reserved.locked_for("child_payout") == CHILD_BUDGET_UNITS

            # ----------------------------------------------------------
            # 2. Child の送金を commit 前で停止させる
            # ----------------------------------------------------------
            failpoints.crash_payment(
                world,
                seam="failpoint_before_commit",
                payment_operation_id=child_payment_id,
                label="child settlement crash before commit",
            )
            stopped = world.observe("after child settlement crash before commit")
            assert stopped.receipts == []
            assert stopped.paid_units == 0
            assert stopped.locked_for("child_payout") == CHILD_BUDGET_UNITS
            assert stopped.journal == reserved.journal
            assert stopped.wallet_units == reserved.wallet_units

            # ----------------------------------------------------------
            # 3. 送金停止中でも Parent 提出は妨げられない
            # ----------------------------------------------------------
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
            assert after_root_submit.locked_for("child_payout") == CHILD_BUDGET_UNITS

    # ------------------------------------------------------------------
    # 4. Requester が Root を承認 → 未拘束 available の 90 だけを予約する
    # ------------------------------------------------------------------
    world.run_cli(
        ["job", "approve", root_id, "--submission", root_submission_id],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("approve", "r-root"),
        action="job approve (root)",
    )
    after_root_approve = world.observe("after root approve by Requester")
    assert after_root_approve.job_states[root_id] == "DONE"
    assert after_root_approve.locked_for("parent_payout") == PARENT_PAYOUT_UNITS
    # 送金停止中の 10 は巻き込まれない
    assert after_root_approve.locked_for("child_payout") == CHILD_BUDGET_UNITS
    assert after_root_approve.available_units == 0
    assert after_root_approve.paid_units == 0
    root_payouts = after_root_approve.operations_for(root_id, "payout")
    assert len(root_payouts) == 1
    root_payment_id = str(root_payouts[0]["operation_id"])
    assert root_payouts[0]["payee_id"] == AGENT_A_ID
    assert root_payouts[0]["amount"] == "90.000000"

    # ------------------------------------------------------------------
    # 5. A への 90 だけを確定させる（B の 10 は locked に残る）
    # ------------------------------------------------------------------
    retry_root = world.run_cli(
        ["payment", "retry", root_payment_id],
        actor=REQUESTER_ID,
        action="payment retry (root payout)",
    )
    assert retry_root.data["payment_status"] == "SUCCEEDED"
    after_parent_paid = world.observe("after root payout while child payment stopped")
    assert after_parent_paid.paid_to(AGENT_A_ID) == PARENT_PAYOUT_UNITS
    assert (
        after_parent_paid.wallet_units[AGENT_A_ID]
        == agent_a_wallet_baseline + PARENT_PAYOUT_UNITS
    )
    # B の 10 は locked（child_payout）に残り、Receipt も Wallet も動かない
    assert after_parent_paid.locked_for("child_payout") == CHILD_BUDGET_UNITS
    assert after_parent_paid.paid_to(AGENT_B_ID) == 0
    assert after_parent_paid.wallet_units[AGENT_B_ID] == agent_b_wallet_baseline
    assert [r["job_id"] for r in after_parent_paid.receipts] == [root_id]
    assert after_parent_paid.escrow_units == CHILD_BUDGET_UNITS
    assert after_parent_paid.available_units == 0
    stopped_payment = after_parent_paid.operations_for(child_id, "payout")[0]
    assert stopped_payment["status"] != "SUCCEEDED", stopped_payment
    assert stopped_payment["receipt_id"] is None

    # ------------------------------------------------------------------
    # 6. 復旧: B の 10 は 1 回だけ支払われる
    # ------------------------------------------------------------------
    world.settle()
    recovered = world.observe("after child payment recovery")
    assert recovered.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert len([r for r in recovered.receipts if r["job_id"] == child_id]) == 1
    assert (
        recovered.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    assert recovered.locked_units == 0
    assert recovered.escrow_units == 0

    # 追加の tick でも二重送金しない
    world.tick_once(label="tick --once (after recovery)")
    final = world.observe("after tick following recovery")
    assert final.receipts == recovered.receipts
    assert final.wallet_units == recovered.wallet_units

    assert final.job_states[root_id] == "DONE"
    assert final.job_states[child_id] == "DONE"
    assert final.paid_to(AGENT_A_ID) == PARENT_PAYOUT_UNITS
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.refunded_units == 0
    assert final.available_units == 0
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert final.submission_worker(root_id) == AGENT_A_ID
    assert final.paid_by_payee_units == {
        AGENT_A_ID: PARENT_PAYOUT_UNITS,
        AGENT_B_ID: CHILD_BUDGET_UNITS,
    }
    assert harness.check_payee_entitlement(final) == []

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label

    world.report.note(
        "X03: Child の送金を commit 前で停止させても Parent の提出・承認・"
        "90 の支払いは進む。B の 10 は child_payout に残り、復旧後に 1 回だけ"
        "支払われて Escrow は 0 になる"
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
