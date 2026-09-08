"""E04: Child 失敗・Parent 失敗（計画書 第18節「指定4結果」）。

| ID | Child | Parent | A支払い | B支払い | Requester返金 | 最終Escrow |
|----|-------|--------|--------:|--------:|--------------:|-----------:|
| E04| 失敗  | 失敗   | 0       | 0       | 100           | 0          |

再現手段（どちらも提出前。不正な一方的返金では代替しない）:
- **Child 失敗**: B による提出前 abandon（CLI `job abandon CHILD --lease B_LEASE`）
- **Parent 失敗**: 提出前 expiry（共有 Clock を Root Lease 期限・Root deadline
  より後へ進め、独立 tick プロセスで失効させる。実時間 sleep は使わない）

**Child を先に失敗させてから Parent を失敗させる**。Child の原資 10 が
available へ戻った後に Parent が失敗するため、返金予約は累計 100 の 1 件になる
（Parent 先行返金 90 ＋ 追加返金 10 の順序は N13 の担当で、本シナリオでは扱わない）。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "E04"

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_e04_child_fail_parent_fail(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_e04(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_e04(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    agent_a_wallet_baseline = after_fund.wallet_units[AGENT_A_ID]
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]
    requester_wallet_baseline = after_fund.wallet_units[REQUESTER_ID]

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

            after_child_claim = world.snapshot("child claimed")
            assert after_child_claim.locked_for("child_work") == CHILD_BUDGET_UNITS
            assert (
                after_child_claim.available_units
                == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
            )

            # ----------------------------------------------------------
            # 1. Child を先に失敗させる（B の提出前 abandon）
            # ----------------------------------------------------------
            world.run_cli(
                ["job", "abandon", child_id, "--lease", start.child_lease_id],
                actor=AGENT_B_ID,
                operation_id=world.next_operation_id("abandon", "b-child"),
                action="job abandon (child)",
            )

    after_child_fail = world.observe("after child abandon by B")
    # 第9節「Child failure（判定未確定の原資）: child_work -10 / available +10」。
    # 内部移動そのものは Journal（reason='return'）を正本として照合する。
    assert after_child_fail.journal_total("child_work", reason="return") == (
        -CHILD_BUDGET_UNITS
    )
    assert after_child_fail.journal_total("available", reason="return") == (
        CHILD_BUDGET_UNITS
    )
    assert after_child_fail.job_states[child_id] == "FAILED"
    assert after_child_fail.locked_for("child_work") == 0
    assert after_child_fail.locked_units == 0
    assert after_child_fail.available_units == ROOT_BUDGET_UNITS
    assert after_child_fail.paid_units == 0
    assert after_child_fail.refunded_units == 0
    # A の Wallet への返金ではない
    assert after_child_fail.wallet_units[AGENT_A_ID] == agent_a_wallet_baseline
    assert after_child_fail.wallet_units[AGENT_B_ID] == agent_b_wallet_baseline

    # ------------------------------------------------------------------
    # 2. その後に Parent（Root）を失敗させる（提出前 expiry）
    # ------------------------------------------------------------------
    world.expire_job(root_id, label="tick --watch (root expiry)")
    after_parent_fail = world.observe("after root expiry")
    assert after_parent_fail.job_states[root_id] == "EXPIRED"
    # 第9節「Parent failure: available -100 / refund +100」。同じ tick round 内で
    # 返金送金まで進むため refund bucket の残高は 0 に戻る。移動は Journal で照合。
    assert after_parent_fail.journal_total("refund", reason="reserve") == (
        ROOT_BUDGET_UNITS
    )
    assert after_parent_fail.journal_total("available", reason="reserve") == (
        -ROOT_BUDGET_UNITS
    )
    assert after_parent_fail.available_units == 0
    assert after_parent_fail.paid_units == 0
    refund_ops = after_parent_fail.operations_for(root_id, "refund")
    assert len(refund_ops) == 1, "Child 先行失敗により返金予約は累計 100 の 1 件"
    assert refund_ops[0]["payee_id"] == REQUESTER_ID
    assert refund_ops[0]["amount"] == "100.000000"

    # 確定済み返金予約を独立 tick で決済する
    world.settle()
    final = world.observe("after refund settlement")

    # -- 4結果の判定（決済完了後） --------------------------------------
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.paid_to(AGENT_B_ID) == 0
    assert final.paid_units == 0
    assert final.refunded_to(REQUESTER_ID) == ROOT_BUDGET_UNITS
    assert final.refunded_units == ROOT_BUDGET_UNITS
    assert final.escrow_units == 0
    assert final.available_units == 0
    assert final.locked_units == 0
    assert final.job_states[root_id] == "EXPIRED"
    assert final.job_states[child_id] == "FAILED"

    # 受取権者との一致（保存則とは独立に導出する）
    assert final.submission_worker(root_id) is None
    assert final.submission_worker(child_id) is None
    assert final.paid_by_payee_units == {}
    assert final.refunded_by_payee_units == {
        final.job_requesters[root_id]: ROOT_BUDGET_UNITS
    }
    assert final.job_requesters[root_id] == REQUESTER_ID
    assert harness.check_payee_entitlement(final) == []

    # A・B の Wallet は全過程で不変（A の自己資金引落しも受取も無い）
    assert final.wallet_units[AGENT_A_ID] == agent_a_wallet_baseline
    assert final.wallet_units[AGENT_B_ID] == agent_b_wallet_baseline
    assert (
        final.wallet_units[REQUESTER_ID]
        == requester_wallet_baseline + ROOT_BUDGET_UNITS
    )

    world.report.note(
        "E04: Child 失敗（B の提出前 abandon）→ Parent 失敗（Root expiry）。"
        "A=0 / B=0 / 返金=100（1 件の予約）/ Escrow=0"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {root_id: "EXPIRED", child_id: "FAILED"},
        "paid": {},
        "refunded": {REQUESTER_ID: "100.000000"},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }
