"""E02: Child 失敗・Parent 成功（計画書 第18節「指定4結果」）。

| ID | Child | Parent | A支払い | B支払い | Requester返金 | 最終Escrow |
|----|-------|--------|--------:|--------:|--------------:|-----------:|
| E02| 失敗  | 成功   | 100     | 0       | 0             | 0          |

「Child 失敗」の再現手段: **B による提出前 abandon**（CLI
`job abandon CHILD --lease B_LEASE`）。第18節「Child 失敗は提出前 abandon
または expiry」のうち abandon を選び、不正な一方的返金では代替しない。

実プロセス構成: Requester は CLI 子プロセス、A / B は別々の MCP stdio
サーバープロセス、決済は独立 tick プロセス。4結果の判定は決済完了後に行う。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "E02"

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_e02_child_fail_parent_ok(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_e02(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_e02(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    assert after_fund.available_units == ROOT_BUDGET_UNITS
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
            # Child を提出前に失敗させる: B が CLI で abandon
            # ----------------------------------------------------------
            world.run_cli(
                ["job", "abandon", child_id, "--lease", start.child_lease_id],
                actor=AGENT_B_ID,
                operation_id=world.next_operation_id("abandon", "b-child"),
                action="job abandon (child)",
            )
            after_child_fail = world.observe("after child abandon by B")
            # 第9節「Child failure（判定未確定の原資）: child_work -10 / available +10」。
            # 内部移動は Journal（reason='return'）を正本として照合する。
            assert after_child_fail.journal_total("child_work", reason="return") == (
                -CHILD_BUDGET_UNITS
            )
            assert after_child_fail.journal_total("available", reason="return") == (
                CHILD_BUDGET_UNITS
            )
            assert after_child_fail.job_states[child_id] == "FAILED"
            assert after_child_fail.locked_for("child_work") == 0
            assert after_child_fail.locked_for("child_payout") == 0
            assert after_child_fail.locked_units == 0
            assert after_child_fail.available_units == ROOT_BUDGET_UNITS
            # A の Wallet への返金ではない（Escrow 内部で available へ戻る）
            assert after_child_fail.wallet_units[AGENT_A_ID] == agent_a_wallet_baseline
            assert after_child_fail.wallet_units[AGENT_B_ID] == agent_b_wallet_baseline
            assert (
                after_child_fail.wallet_units[REQUESTER_ID]
                == requester_wallet_baseline
            )
            assert after_child_fail.paid_units == 0
            assert after_child_fail.refunded_units == 0

            # ----------------------------------------------------------
            # 全 Child が判定終端になった後に A が Parent へ submit
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

    # Requester が CLI で Root を承認
    world.run_cli(
        ["job", "approve", root_id, "--submission", root_submission_id],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("approve", "r-root"),
        action="job approve (root)",
    )
    after_root_approve = world.observe("after root approve by Requester")
    assert after_root_approve.job_states[root_id] == "DONE"
    # Child が失敗して原資が戻ったので Parent の予約は 100
    assert after_root_approve.locked_for("parent_payout") == ROOT_BUDGET_UNITS
    assert after_root_approve.available_units == 0
    assert after_root_approve.paid_units == 0

    # 独立 tick プロセスで決済を完了させる
    world.settle()
    final = world.observe("after settlement")

    # -- 4結果の判定（決済完了後） --------------------------------------
    assert final.paid_to(AGENT_A_ID) == ROOT_BUDGET_UNITS
    assert final.paid_to(AGENT_B_ID) == 0
    assert final.refunded_to(REQUESTER_ID) == 0
    assert final.refunded_units == 0
    assert final.escrow_units == 0
    assert final.available_units == 0
    assert final.locked_units == 0
    assert final.job_states[root_id] == "DONE"
    assert final.job_states[child_id] == "FAILED"

    # 受取権者との一致（保存則とは独立に Lease の Worker から導出する）
    assert final.submission_worker(root_id) == AGENT_A_ID
    assert final.submission_worker(child_id) is None  # B は提出していない
    assert final.paid_by_payee_units == {
        final.submission_worker(root_id): ROOT_BUDGET_UNITS
    }
    assert final.refunded_by_payee_units == {}
    assert harness.check_payee_entitlement(final) == []

    # A の自己資金引落しが無い（+100 のみ）。B・Requester の Wallet は不変
    assert (
        final.wallet_units[AGENT_A_ID]
        == agent_a_wallet_baseline + ROOT_BUDGET_UNITS
    )
    assert final.wallet_units[AGENT_B_ID] == agent_b_wallet_baseline
    assert final.wallet_units[REQUESTER_ID] == requester_wallet_baseline

    world.report.note(
        "E02: Child 失敗（B の提出前 abandon）・Parent 成功。A=100 / B=0 / 返金=0 / Escrow=0"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {root_id: "DONE", child_id: "FAILED"},
        "paid": {AGENT_A_ID: "100.000000"},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }
