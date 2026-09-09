"""X12: 先行返金と生存 Child の child_work 保護（計画書 第18節 追加検証 X12）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X12 | max_amount=30、max_ratio_bps=3000、max_children=3、max_depth=1でpart-1/2/3を各10で作成。part-1成功・Bへ10支払済み、part-2失敗返却、part-3はBが未提出LEASEDのままParent失敗 | 先行返金80、part-3のchild_work=10を保護。別fixtureでpart-3成功ならB累計20・返金80、失敗ならB累計10・返金90。A=0、最終Escrow=0 |

policy は第11節の既定カード（max_amount=30 / max_ratio_bps=3000 /
max_children=3 / max_depth=1）のまま。共通の進行:

| 段階 | U | available | 説明 |
|---|---:|---:|---|
| 入金後 | 0 | 100 | |
| part-1 作成 | 10 | 90 | child_work 10 |
| part-1 承認 | 10 | 90 | child_payout 10（child_payout も U） |
| part-1 送金 | 10 | 90 | 支払い済み 10 も U に残る |
| part-2 作成 | 20 | 80 | 10（支払い済み）＋10（child_work） |
| part-2 失敗返却 | 10 | 90 | Parent 生存中なので available へ戻る |
| part-3 作成 | 20 | 80 | part-3 は未提出 LEASED |
| Parent 失敗 | 20 | 0 | 未拘束 80 が**先行返金**、part-3 の 10 は locked のまま |

`_job_terminal_fund_effects` は locked（child_work）に触らないため、
part-3 の `child_work=10` は Parent 終端後も保護される（第8節の表
「Parent 終端を Child へ伝播しない」）。その後の part-3 の判定で 2 通りに分かれる:

- **X12A（part-3 成功）**: A が承認して B へ 10。B 累計 20・返金 80
- **X12B（part-3 失敗）**: 提出前 abandon で child_work → available → refund の
  追加返金 10。B 累計 10・返金 90

どちらも A=0・最終 Escrow=0。
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET = "10.000000"
CHILD_BUDGET_UNITS = 10_000_000
PRIOR_REFUND_UNITS = 80_000_000
CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"


@dataclass
class Stage:
    """共通進行の結果（part-3 の判定前の状態）。"""

    root_id: str
    part1_id: str
    part2_id: str
    part3_id: str
    part3_version_id: str
    part3_lease_id: str


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x12_part3_succeeds_after_prior_refund(tmp_path: Path) -> None:
    """part-3 成功 → B 累計 20・返金 80・A=0・Escrow=0。"""
    await _run(harness.create_world(tmp_path, "X12A"), _run_x12a)


@pytest.mark.anyio
async def test_x12_part3_fails_after_prior_refund(tmp_path: Path) -> None:
    """part-3 失敗 → 追加返金 10 で B 累計 10・返金 90・A=0・Escrow=0。"""
    await _run(harness.create_world(tmp_path, "X12B"), _run_x12b)


async def _run(
    world: harness.E2EWorld,
    scenario: Callable[[harness.E2EWorld], Awaitable[dict]],
) -> None:
    failed = False
    try:
        expected = await scenario(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_x12a(world: harness.E2EWorld) -> dict:
    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            stage = await _run_common(world, agent_a, agent_b)

            # part-3 成功: B が提出し、Parent 終端後でも A（Child Requester）が承認する
            submit = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": stage.part3_id,
                    "lease_id": stage.part3_lease_id,
                    "version_id": stage.part3_version_id,
                    "artifact_json": json.dumps({"sum": 5}),
                    "operation_id": world.next_operation_id("submit", "b-part-3"),
                },
            )
            after_submit = world.observe("after part-3 submit (parent FAILED)")
            assert after_submit.job_states[stage.part3_id] == "SUBMITTED"
            assert after_submit.locked_for("child_work") == CHILD_BUDGET_UNITS
            _assert_usage(world, stage.root_id, in_use_units=20_000_000, available_units=0)

            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": stage.part3_id,
                    "submission_id": str(submit.data["submission_id"]),
                    "operation_id": world.next_operation_id("approve", "a-part-3"),
                },
            )
            after_approve = world.observe("after part-3 approve (parent FAILED)")
            assert after_approve.job_states[stage.part3_id] == "DONE"
            assert after_approve.locked_for("child_work") == 0
            assert after_approve.locked_for("child_payout") == CHILD_BUDGET_UNITS
            _assert_usage(world, stage.root_id, in_use_units=20_000_000, available_units=0)

    world.settle()
    final = world.observe("after final settlement")
    assert final.job_states[stage.part3_id] == "DONE"
    assert final.paid_to(AGENT_B_ID) == 2 * CHILD_BUDGET_UNITS
    assert final.refunded_to(REQUESTER_ID) == PRIOR_REFUND_UNITS
    assert len(final.operations_for(stage.root_id, "refund")) == 1
    assert final.operations_for(stage.part3_id, "refund") == []
    _assert_common_final(world, final, stage)
    _assert_usage(world, stage.root_id, in_use_units=20_000_000, available_units=0)

    world.report.note(
        "X12A: 先行返金 80 の後も part-3 の child_work=10 は保護され、"
        "Parent FAILED のまま A が承認して B へ 10。B 累計 20・返金 80・"
        "A=0・Escrow=0"
    )
    return {
        "scenario_id": "X12A",
        "terminal_job_states": {
            stage.root_id: "FAILED",
            stage.part1_id: "DONE",
            stage.part2_id: "FAILED",
            stage.part3_id: "DONE",
        },
        "paid": {AGENT_B_ID: "20.000000"},
        "refunded": {REQUESTER_ID: "80.000000"},
        "locked_breakdown": _EMPTY_LOCKED,
    }


async def _run_x12b(world: harness.E2EWorld) -> dict:
    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            stage = await _run_common(world, agent_a, agent_b)

    # part-3 失敗: 提出前 abandon（Parent 終端後なので追加返金になる）
    world.run_cli(
        ["job", "abandon", stage.part3_id, "--lease", stage.part3_lease_id],
        actor=AGENT_B_ID,
        operation_id=world.next_operation_id("abandon", "b-part-3"),
        action="job abandon (part-3)",
    )
    after_fail = world.observe("after part-3 abandon (parent FAILED)")
    assert after_fail.job_states[stage.part3_id] == "FAILED"
    combined = "child-return-after-parent-terminal"
    assert after_fail.journal_total("child_work", reason=combined) == (
        -CHILD_BUDGET_UNITS
    )
    assert after_fail.journal_total("available", reason=combined) == 0
    assert after_fail.journal_total("refund", reason=combined) == CHILD_BUDGET_UNITS
    assert after_fail.available_units == 0
    child_return = [
        op
        for op in after_fail.operations_for(stage.part3_id, "refund")
        if op["business_key"]
        == f"refund:{stage.root_id}:child-return:{stage.part3_id}"
    ]
    assert len(child_return) == 1
    assert child_return[0]["amount"] == "10.000000"
    _assert_usage(world, stage.root_id, in_use_units=10_000_000, available_units=0)

    world.settle()
    final = world.observe("after final settlement")
    assert final.job_states[stage.part3_id] == "FAILED"
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.refunded_to(REQUESTER_ID) == 90_000_000
    assert sorted(
        op["amount"] for op in final.operations if op["kind"] == "refund"
    ) == ["10.000000", "80.000000"]
    _assert_common_final(world, final, stage)
    _assert_usage(world, stage.root_id, in_use_units=10_000_000, available_units=0)

    world.report.note(
        "X12B: 先行返金 80 の後に part-3 が失敗すると追加返金 10 だけが作られ、"
        "返金累計 90・B 累計 10・A=0・Escrow=0"
    )
    return {
        "scenario_id": "X12B",
        "terminal_job_states": {
            stage.root_id: "FAILED",
            stage.part1_id: "DONE",
            stage.part2_id: "FAILED",
            stage.part3_id: "FAILED",
        },
        "paid": {AGENT_B_ID: "10.000000"},
        "refunded": {REQUESTER_ID: "90.000000"},
        "locked_breakdown": _EMPTY_LOCKED,
    }


# ---------------------------------------------------------------------------
# 共通進行（part-1 成功・支払済 / part-2 失敗返却 / part-3 未提出 LEASED / Parent 失敗）
# ---------------------------------------------------------------------------


_EMPTY_LOCKED = {
    "child_payout": "0.000000",
    "child_work": "0.000000",
    "parent_payout": "0.000000",
    "refund": "0.000000",
}


async def _run_common(
    world: harness.E2EWorld,
    agent_a: harness.AgentSession,
    agent_b: harness.AgentSession,
) -> Stage:
    root_id, root_version_id = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    assert after_fund.available_units == ROOT_BUDGET_UNITS
    _assert_usage(world, root_id, in_use_units=0, available_units=ROOT_BUDGET_UNITS)

    claim_root = await agent_a.call(
        "ojp_claim_job",
        {
            "job_id": root_id,
            "expected_version_id": root_version_id,
            "operation_id": world.next_operation_id("claim", "a-root"),
        },
    )
    root_lease_id = str(claim_root.data["lease_id"])
    world.observe("after root claim by A")

    # -- part-1: 成功して B へ 10 支払い済みにする -------------------------
    part1_id, part1_version_id = await _create_child(
        world, agent_a, root_id, root_lease_id, "part-1"
    )
    after_part1 = world.observe("after part-1 create")
    assert after_part1.locked_for("child_work") == CHILD_BUDGET_UNITS
    _assert_usage(
        world, root_id, in_use_units=10_000_000, available_units=90_000_000
    )

    part1_lease_id = await _claim_child(world, agent_b, part1_id, part1_version_id)
    submit1 = await agent_b.call(
        "ojp_submit",
        {
            "job_id": part1_id,
            "lease_id": part1_lease_id,
            "version_id": part1_version_id,
            "artifact_json": json.dumps({"sum": 6}),
            "operation_id": world.next_operation_id("submit", "b-part-1"),
        },
    )
    await agent_a.call(
        "ojp_approve",
        {
            "job_id": part1_id,
            "submission_id": str(submit1.data["submission_id"]),
            "operation_id": world.next_operation_id("approve", "a-part-1"),
        },
    )
    after_approve1 = world.observe("after part-1 approve")
    assert after_approve1.locked_for("child_payout") == CHILD_BUDGET_UNITS
    assert after_approve1.locked_for("child_work") == 0
    _assert_usage(
        world, root_id, in_use_units=10_000_000, available_units=90_000_000
    )

    world.settle()
    after_pay1 = world.observe("after part-1 payment")
    assert after_pay1.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert after_pay1.locked_units == 0
    # 支払い済みの 10 も U に残る（新しい Child へ再利用できない）
    _assert_usage(
        world, root_id, in_use_units=10_000_000, available_units=90_000_000
    )

    # -- part-2: 提出前 abandon で失敗返却 --------------------------------
    part2_id, part2_version_id = await _create_child(
        world, agent_a, root_id, root_lease_id, "part-2"
    )
    after_part2 = world.observe("after part-2 create")
    assert after_part2.locked_for("child_work") == CHILD_BUDGET_UNITS
    _assert_usage(
        world, root_id, in_use_units=20_000_000, available_units=80_000_000
    )

    part2_lease_id = await _claim_child(world, agent_b, part2_id, part2_version_id)
    world.run_cli(
        ["job", "abandon", part2_id, "--lease", part2_lease_id],
        actor=AGENT_B_ID,
        operation_id=world.next_operation_id("abandon", "b-part-2"),
        action="job abandon (part-2)",
    )
    after_part2_fail = world.observe("after part-2 abandon (parent alive)")
    assert after_part2_fail.job_states[part2_id] == "FAILED"
    assert after_part2_fail.locked_units == 0
    assert after_part2_fail.available_units == 90_000_000
    _assert_usage(
        world, root_id, in_use_units=10_000_000, available_units=90_000_000
    )
    assert after_part2_fail.refunded_units == 0

    # -- part-3: B が Claim して未提出 LEASED のまま -----------------------
    part3_id, part3_version_id = await _create_child(
        world, agent_a, root_id, root_lease_id, "part-3"
    )
    part3_lease_id = await _claim_child(world, agent_b, part3_id, part3_version_id)
    after_part3 = world.observe("after part-3 claim by B")
    assert after_part3.job_states[part3_id] == "LEASED"
    assert after_part3.locked_for("child_work") == CHILD_BUDGET_UNITS
    _assert_usage(
        world, root_id, in_use_units=20_000_000, available_units=80_000_000
    )

    # -- Parent 失敗: 未拘束 80 の先行返金、part-3 の 10 は保護される -------
    world.run_cli(
        ["job", "abandon", root_id, "--lease", root_lease_id],
        actor=AGENT_A_ID,
        operation_id=world.next_operation_id("abandon", "a-root"),
        action="job abandon (root)",
    )
    after_parent_fail = world.observe("after root abandon by A")
    assert after_parent_fail.job_states[root_id] == "FAILED"
    assert after_parent_fail.locked_for("refund") == PRIOR_REFUND_UNITS
    # part-3 の child_work は Parent 終端後も保護される
    assert after_parent_fail.locked_for("child_work") == CHILD_BUDGET_UNITS
    assert after_parent_fail.job_states[part3_id] == "LEASED"
    assert after_parent_fail.available_units == 0
    prior = after_parent_fail.operations_for(root_id, "refund")
    assert len(prior) == 1
    assert prior[0]["business_key"] == f"refund:{root_id}:terminal"
    assert prior[0]["amount"] == "80.000000"
    _assert_usage(world, root_id, in_use_units=20_000_000, available_units=0)

    world.settle()
    after_prior_refund = world.observe("after prior refund settlement")
    assert after_prior_refund.refunded_to(REQUESTER_ID) == PRIOR_REFUND_UNITS
    assert after_prior_refund.locked_for("child_work") == CHILD_BUDGET_UNITS
    assert after_prior_refund.escrow_units == CHILD_BUDGET_UNITS
    assert after_prior_refund.job_states[part3_id] == "LEASED"
    _assert_usage(world, root_id, in_use_units=20_000_000, available_units=0)

    return Stage(
        root_id=root_id,
        part1_id=part1_id,
        part2_id=part2_id,
        part3_id=part3_id,
        part3_version_id=part3_version_id,
        part3_lease_id=part3_lease_id,
    )


async def _create_child(
    world: harness.E2EWorld,
    agent_a: harness.AgentSession,
    root_id: str,
    root_lease_id: str,
    task_key: str,
) -> tuple[str, str]:
    created = await agent_a.call(
        "ojp_create_child_job",
        {
            "parent_job_id": root_id,
            "lease_id": root_lease_id,
            "task_key": task_key,
            "budget": CHILD_BUDGET,
            "deadline": CHILD_DEADLINE,
            "operation_id": world.next_operation_id("create", f"a-{task_key}"),
        },
    )
    child_id = str(created.data["child_id"])
    world.child_ids.append(child_id)
    return child_id, str(created.data["version_id"])


async def _claim_child(
    world: harness.E2EWorld,
    agent_b: harness.AgentSession,
    child_id: str,
    version_id: str,
) -> str:
    claimed = await agent_b.call(
        "ojp_claim_job",
        {
            "job_id": child_id,
            "expected_version_id": version_id,
            "operation_id": world.next_operation_id("claim", f"b-{child_id[-6:]}"),
        },
    )
    return str(claimed.data["lease_id"])


def _assert_usage(
    world: harness.E2EWorld,
    root_id: str,
    *,
    in_use_units: int,
    available_units: int,
) -> None:
    """U（child 支払い済み + Σ(child_work + child_payout)）と available を照合する。"""
    usage = harness.subcontract_usage(world, root_id, label="x12 usage")
    assert harness.parse_amount_string(usage["in_use"]) == in_use_units, usage
    assert harness.parse_amount_string(usage["available"]) == available_units, usage


def _assert_common_final(
    world: harness.E2EWorld, final: harness.Snapshot, stage: Stage
) -> None:
    assert final.job_states[stage.root_id] == "FAILED"
    assert final.job_states[stage.part1_id] == "DONE"
    assert final.job_states[stage.part2_id] == "FAILED"
    # A は 1 単位も受け取らない
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.escrow_units == 0
    assert final.available_units == 0
    assert final.locked_units == 0
    assert final.paid_units + final.refunded_units == ROOT_BUDGET_UNITS
    assert harness.check_payee_entitlement(final) == []
    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label
