"""E03: Child 成功・Parent 失敗（計画書 第18節「指定4結果」）。

| ID | Child | Parent | A支払い | B支払い | Requester返金 | 最終Escrow |
|----|-------|--------|--------:|--------:|--------------:|-----------:|
| E03| 成功  | 失敗   | 0       | 10      | 90            | 0          |

「Parent 失敗」の再現手段: **提出前 expiry**。共有 Clock を Root Lease 期限・
Root deadline より後へ進め、独立 tick プロセスで失効させる（実時間 sleep は
使わない。時刻更新 commit の完了後に tick を起動する）。不正な一方的返金では
代替しない。

実プロセス構成: Requester は CLI 子プロセス、A / B は別々の MCP stdio
サーバープロセス、期限処理・決済は独立 tick プロセス。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "E03"

CHILD_BUDGET_UNITS = 10_000_000
REFUND_UNITS = 90_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_e03_child_ok_parent_fail(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_e03(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_e03(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    agent_a_wallet_baseline = after_fund.wallet_units[AGENT_A_ID]
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]
    requester_wallet_baseline = after_fund.wallet_units[REQUESTER_ID]

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

            # Child を E01 と同じ経路で成功させる（B が submit → A が承認）
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
            after_child_approve = world.observe("after child approve by A")
            assert after_child_approve.job_states[child_id] == "DONE"
            assert after_child_approve.locked_for("child_payout") == CHILD_BUDGET_UNITS
            assert after_child_approve.paid_to(AGENT_B_ID) == 0

    # B への 10 を確定させる（独立 tick）
    world.settle()
    after_child_payment = world.observe("after child payment")
    assert after_child_payment.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert after_child_payment.locked_for("child_payout") == 0
    assert after_child_payment.available_units == REFUND_UNITS
    assert (
        after_child_payment.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )

    # ------------------------------------------------------------------
    # Parent（Root）を提出前に失敗させる: 共有 Clock を進めて expiry
    # ------------------------------------------------------------------
    world.expire_job(root_id, label="tick --watch (root expiry)")
    after_parent_fail = world.observe("after root expiry")
    assert after_parent_fail.job_states[root_id] == "EXPIRED"
    # 第9節「Parent failure（Child 拘束 10）: available -90 / refund +90」。
    # 同じ tick round 内で返金送金まで進むため refund bucket の残高は 0 に戻る。
    # 内部移動が実際に起きたことは Journal（正本）で照合する。
    assert after_parent_fail.journal_total("refund", reason="reserve") == REFUND_UNITS
    assert (
        after_parent_fail.journal_total("available", reason="reserve") == -REFUND_UNITS
    )
    assert after_parent_fail.available_units == 0
    # 確定済み paid(B)=10 と Child の child_payout は返金予約へ移らない
    assert after_parent_fail.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert after_parent_fail.locked_for("child_payout") == 0
    assert after_parent_fail.journal_total("child_payout", reason="reserve") == (
        CHILD_BUDGET_UNITS
    )
    # refund への reserve は Root available からの 90 だけ（child 由来は無い）
    assert [
        int(e["delta_units"])
        for e in after_parent_fail.journal_moves("refund")
        if int(e["delta_units"]) > 0
    ] == [REFUND_UNITS]
    refund_ops = after_parent_fail.operations_for(root_id, "refund")
    assert len(refund_ops) == 1
    assert refund_ops[0]["payee_id"] == REQUESTER_ID
    assert refund_ops[0]["amount"] == "90.000000"

    # 確定済み返金予約を独立 tick で決済する（新しい予約は作らない）
    world.settle()
    final = world.observe("after refund settlement")

    # -- 4結果の判定（決済完了後） --------------------------------------
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.refunded_to(REQUESTER_ID) == REFUND_UNITS
    assert final.refunded_units == REFUND_UNITS
    assert final.escrow_units == 0
    assert final.available_units == 0
    assert final.locked_units == 0
    assert final.job_states[root_id] == "EXPIRED"
    assert final.job_states[child_id] == "DONE"

    # 受取権者との一致（保存則とは独立に導出する）
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert final.submission_worker(root_id) is None  # A は提出していない
    assert final.paid_by_payee_units == {
        final.submission_worker(child_id): CHILD_BUDGET_UNITS
    }
    assert final.refunded_by_payee_units == {
        final.job_requesters[root_id]: REFUND_UNITS
    }
    assert final.job_requesters[root_id] == REQUESTER_ID
    assert harness.check_payee_entitlement(final) == []

    # A の Wallet は全過程で不変（自己資金引落しも受取も無い）
    assert final.wallet_units[AGENT_A_ID] == agent_a_wallet_baseline
    assert (
        final.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    assert (
        final.wallet_units[REQUESTER_ID]
        == requester_wallet_baseline + REFUND_UNITS
    )

    world.report.note(
        "E03: Child 成功・Parent 失敗（Root Lease/deadline 越えの expiry）。"
        "A=0 / B=10 / 返金=90 / Escrow=0"
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
