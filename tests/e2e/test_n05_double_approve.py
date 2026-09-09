"""N05: 同じ Child を 2 回承認する（計画書 第18節 異常系 5 / N05）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 5 / N05 | 同じChildを2回承認（同ID・別ID・同時） | Acceptance/PaymentOperation/Receiptはそれぞれ1件。Bへ10だけ |

3 通りの二重承認をすべて同じ Child に対して実行する:

1. **同一 operation_id**: 冪等な再送（`replayed=true` で同じ結果）
2. **別 operation_id**: business_key=payout:{child_id} の既存
   PaymentOperation を返す（新しい予約を作らない）
3. **同時実行**: barrier で同時開始した 2 つの独立 CLI プロセス。直列化は
   DB の BEGIN IMMEDIATE と business_key の UNIQUE が担う（共有メモリ Lock
   に頼らない）

決済後に Acceptance・PaymentOperation・Receipt がそれぞれ 1 件で、B の
Wallet が +10 だけ増えることを確認する。二重承認は 3 通りとも決済前に
実行し、送金が 1 回しか起きないことを Receipt で照合する。

実プロセス構成: Requester / A / B の承認呼び出しは CLI 子プロセス、
Claim・Child 作成・提出は MCP stdio サーバープロセス、決済は独立 tick。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import harness, parallel_cli, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "N05"

CHILD_BUDGET_UNITS = 10_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n05_double_approve_pays_child_worker_once(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_n05(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_n05(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]

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
            submission_id = str(submit_child.data["submission_id"])
            world.observe("after child submit by B")

    approve_args = ["job", "approve", child_id, "--submission", submission_id]

    # ------------------------------------------------------------------
    # (a) 同時承認: barrier で同時開始した 2 つの独立 CLI プロセス
    #     （どちらも A = Child Requester。異なる operation_id）
    # ------------------------------------------------------------------
    concurrent = parallel_cli.run_simultaneously(
        world,
        [
            parallel_cli.Invocation(
                args=approve_args,
                actor=AGENT_A_ID,
                operation_id=world.next_operation_id("approve", f"concurrent-{n}"),
                action=f"job approve (concurrent {n})",
            )
            for n in (1, 2)
        ],
        barrier_name="n05-approve",
    )
    for result in concurrent:
        assert result.returncode == 0, (
            f"concurrent approve failed: exit={result.returncode}"
            f" stdout={result.stdout} stderr={result.stderr}"
        )
        assert result.ok, result.payload
    after_concurrent = world.observe("after concurrent approve by A")
    assert after_concurrent.job_states[child_id] == "DONE"
    _assert_single_approval(after_concurrent, child_id)

    # ------------------------------------------------------------------
    # (b) 同一 operation_id の再送（冪等な replay）
    # ------------------------------------------------------------------
    replay_operation_id = world.next_operation_id("approve", "replay")
    first = world.run_cli(
        approve_args,
        actor=AGENT_A_ID,
        operation_id=replay_operation_id,
        action="job approve (same operation_id, 1st)",
    )
    second = world.run_cli(
        approve_args,
        actor=AGENT_A_ID,
        operation_id=replay_operation_id,
        action="job approve (same operation_id, 2nd)",
    )
    assert second.payload is not None and second.payload["replayed"] is True
    assert second.data == first.data
    after_same_id = world.observe("after approve replay (same operation_id)")
    _assert_single_approval(after_same_id, child_id)

    # ------------------------------------------------------------------
    # (c) 別 operation_id での再承認（business_key で既存結果を返す）
    # ------------------------------------------------------------------
    other_id = world.run_cli(
        approve_args,
        actor=AGENT_A_ID,
        operation_id=world.next_operation_id("approve", "other-id"),
        action="job approve (different operation_id)",
    )
    assert other_id.ok
    after_other_id = world.observe("after approve with a different operation_id")
    _assert_single_approval(after_other_id, child_id)
    # 予約はどの経路でも増えない
    assert after_other_id.locked_for("child_payout") == CHILD_BUDGET_UNITS
    assert after_other_id.paid_to(AGENT_B_ID) == 0

    # ------------------------------------------------------------------
    # 決済（独立 tick）。Receipt も 1 件だけ
    # ------------------------------------------------------------------
    world.settle()
    final = world.observe("after settlement")
    _assert_single_approval(final, child_id)

    payouts = final.operations_for(child_id, "payout")
    assert payouts[0]["status"] == "SUCCEEDED"
    assert payouts[0]["amount"] == "10.000000"
    assert payouts[0]["payee_id"] == AGENT_B_ID
    child_receipts = [r for r in final.receipts if r["job_id"] == child_id]
    assert len(child_receipts) == 1
    assert child_receipts[0]["payee_id"] == AGENT_B_ID
    assert int(child_receipts[0]["amount_units"]) == CHILD_BUDGET_UNITS

    # B へ 10 だけ
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.paid_units == CHILD_BUDGET_UNITS
    assert (
        final.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    assert final.locked_for("child_payout") == 0
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.refunded_to(REQUESTER_ID) == 0
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert harness.check_payee_entitlement(final) == []

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label

    world.report.note(
        "N05: 同じ Child への二重承認（同時・同一 operation_id・別 operation_id）"
        "でも Acceptance / PaymentOperation / Receipt はそれぞれ 1 件。B へ 10 だけ"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {root_id: "LEASED", child_id: "DONE"},
        "paid": {AGENT_B_ID: "10.000000"},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


def _assert_single_approval(snapshot: harness.Snapshot, child_id: str) -> None:
    """Acceptance と PaymentOperation（と存在すれば Receipt）が 1 件ずつ。"""
    acceptances = [a for a in snapshot.acceptances if a["job_id"] == child_id]
    assert len(acceptances) == 1, acceptances
    assert acceptances[0]["decision"] == "APPROVED"
    assert acceptances[0]["decided_by"] == AGENT_A_ID

    payouts = snapshot.operations_for(child_id, "payout")
    assert len(payouts) == 1, payouts
    assert payouts[0]["amount"] == "10.000000"
    assert payouts[0]["payee_id"] == AGENT_B_ID

    receipts = [r for r in snapshot.receipts if r["job_id"] == child_id]
    assert len(receipts) <= 1, receipts
