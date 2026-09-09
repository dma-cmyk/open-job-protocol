"""N13: Parent 先行返金 90 の後の Child 判定（計画書 第18節 異常系 13 / N13）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 13 / N13 | Parent先行返金90、その後Child失敗 | 追加返金10のみで累計100。Child成功なら累計90のまま |

2 つの独立 fixture。どちらも Parent（Root）を提出前 abandon で失敗させ、
未拘束 available の 90 を先行返金として**決済まで確定**させてから Child を
判定する。

1. **Child 失敗**: B が提出前 abandon する。Parent は既に終端なので
   `child_work -10 / available +10 / refund +10` を 1 transaction で確定し
   （第9節）、`refund:{root}:child-return:{child}` の**追加返金 1 件だけ**が
   作られる。返金累計は 90 + 10 = 100、B=0、A=0、Escrow=0
2. **Child 成功**: B が提出し A が承認する。child_payout の 10 は B へ支払われ
   るので追加返金は発生せず、**返金累計は 90 のまま**。B=10、A=0、Escrow=0

どちらも返金の PaymentOperation の件数・business_key・金額まで照合し、
先行返金 90 が二重に作られていないことを確認する。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000
FIRST_REFUND_UNITS = 90_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n13_child_failure_adds_only_the_remaining_ten(tmp_path: Path) -> None:
    """先行返金 90 の後に Child 失敗 → 追加返金 10 のみ・累計 100。"""
    world = harness.create_world(tmp_path, "N13A")
    await _run(world, child_succeeds=False)


@pytest.mark.anyio
async def test_n13_child_success_keeps_refund_at_ninety(tmp_path: Path) -> None:
    """先行返金 90 の後に Child 成功 → 追加返金なし・累計 90 のまま・B へ 10。"""
    world = harness.create_world(tmp_path, "N13B")
    await _run(world, child_succeeds=True)


async def _run(world: harness.E2EWorld, *, child_succeeds: bool) -> None:
    failed = False
    try:
        expected = await _run_n13(world, child_succeeds=child_succeeds)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_n13(world: harness.E2EWorld, *, child_succeeds: bool) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    agent_a_wallet_baseline = after_fund.wallet_units[AGENT_A_ID]
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]
    requester_wallet_baseline = after_fund.wallet_units[REQUESTER_ID]

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

            if child_succeeds:
                # Child は Parent 失敗より前に提出だけ済ませ、判定は後で行う
                submit = await agent_b.call(
                    "ojp_submit",
                    {
                        "job_id": child_id,
                        "lease_id": start.child_lease_id,
                        "version_id": start.child_version_id,
                        "artifact_json": json.dumps({"sum": 6}),
                        "operation_id": world.next_operation_id("submit", "b-child"),
                    },
                )
                submission_id = str(submit.data["submission_id"])
                world.observe("after child submit by B")

            # ----------------------------------------------------------
            # Parent（Root）を提出前 abandon で失敗させ、先行返金 90 を確定
            # ----------------------------------------------------------
            world.run_cli(
                ["job", "abandon", root_id, "--lease", start.root_lease_id],
                actor=AGENT_A_ID,
                operation_id=world.next_operation_id("abandon", "a-root"),
                action="job abandon (root)",
            )
            after_parent_fail = world.observe("after root abandon by A")
            assert after_parent_fail.job_states[root_id] == "FAILED"
            assert after_parent_fail.locked_for("child_work") == CHILD_BUDGET_UNITS

            world.settle()
            after_first_refund = world.observe("after first refund settlement")
            # 先行返金は 90 だけ。Child の 10 は拘束されたまま残る
            assert after_first_refund.refunded_to(REQUESTER_ID) == FIRST_REFUND_UNITS
            assert after_first_refund.refunded_units == FIRST_REFUND_UNITS
            assert after_first_refund.locked_for("child_work") == CHILD_BUDGET_UNITS
            assert after_first_refund.available_units == 0
            first_refunds = after_first_refund.operations_for(root_id, "refund")
            assert len(first_refunds) == 1
            assert first_refunds[0]["business_key"] == f"refund:{root_id}:terminal"
            assert first_refunds[0]["amount"] == "90.000000"
            assert first_refunds[0]["status"] == "SUCCEEDED"
            assert (
                after_first_refund.wallet_units[REQUESTER_ID]
                == requester_wallet_baseline + FIRST_REFUND_UNITS
            )

            # ----------------------------------------------------------
            # Child を判定する
            # ----------------------------------------------------------
            if child_succeeds:
                await agent_a.call(
                    "ojp_approve",
                    {
                        "job_id": child_id,
                        "submission_id": submission_id,
                        "operation_id": world.next_operation_id("approve", "a-child"),
                    },
                )
                after_child = world.observe("after child approve (parent FAILED)")
                assert after_child.job_states[child_id] == "DONE"
                assert after_child.locked_for("child_payout") == CHILD_BUDGET_UNITS
            else:
                world.run_cli(
                    ["job", "abandon", child_id, "--lease", start.child_lease_id],
                    actor=AGENT_B_ID,
                    operation_id=world.next_operation_id("abandon", "b-child"),
                    action="job abandon (child)",
                )
                after_child = world.observe("after child abandon (parent FAILED)")
                assert after_child.job_states[child_id] == "FAILED"
                # 第9節: Parent 終端後の Child 失敗は child_work -10 /
                # available +10 / available -10 / refund +10 を 1 transaction
                # （reason='child-return-after-parent-terminal'）で確定する
                combined = "child-return-after-parent-terminal"
                assert after_child.journal_total("child_work", reason=combined) == (
                    -CHILD_BUDGET_UNITS
                )
                assert after_child.journal_total("available", reason=combined) == 0
                assert after_child.journal_total("refund", reason=combined) == (
                    CHILD_BUDGET_UNITS
                )
                # 途中の available を外から再利用させない（同一 transaction）
                assert after_child.available_units == 0
                child_return_refunds = [
                    op
                    for op in after_child.operations_for(child_id, "refund")
                    if op["business_key"]
                    == f"refund:{root_id}:child-return:{child_id}"
                ]
                assert len(child_return_refunds) == 1
                assert child_return_refunds[0]["amount"] == "10.000000"

    world.settle()
    final = world.observe("after final settlement")

    if child_succeeds:
        # 追加返金なし: 返金累計は 90 のまま、B は 10 を受領
        assert final.job_states[child_id] == "DONE"
        assert final.refunded_to(REQUESTER_ID) == FIRST_REFUND_UNITS
        assert final.refunded_units == FIRST_REFUND_UNITS
        assert len(final.operations_for(root_id, "refund")) == 1
        assert final.operations_for(child_id, "refund") == []
        assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
        assert final.submission_worker(child_id) == AGENT_B_ID
        assert (
            final.wallet_units[AGENT_B_ID]
            == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
        )
        assert (
            final.wallet_units[REQUESTER_ID]
            == requester_wallet_baseline + FIRST_REFUND_UNITS
        )
        expected_paid = {AGENT_B_ID: "10.000000"}
        expected_refunded = {REQUESTER_ID: "90.000000"}
        child_state = "DONE"
        note = (
            "N13（Child 成功）: 先行返金 90 の後に Child が成功しても追加返金は"
            "発生せず、返金累計は 90 のまま。B へ 10 / A=0 / Escrow=0"
        )
    else:
        # 追加返金 10 のみで累計 100
        assert final.job_states[child_id] == "FAILED"
        assert final.refunded_to(REQUESTER_ID) == ROOT_BUDGET_UNITS
        assert final.refunded_units == ROOT_BUDGET_UNITS
        refund_amounts = sorted(
            op["amount"]
            for op in final.operations
            if op["kind"] == "refund"
        )
        assert refund_amounts == ["10.000000", "90.000000"]
        assert final.paid_to(AGENT_B_ID) == 0
        assert final.paid_units == 0
        assert final.wallet_units[AGENT_B_ID] == agent_b_wallet_baseline
        assert (
            final.wallet_units[REQUESTER_ID]
            == requester_wallet_baseline + ROOT_BUDGET_UNITS
        )
        expected_paid = {}
        expected_refunded = {REQUESTER_ID: "100.000000"}
        child_state = "FAILED"
        note = (
            "N13（Child 失敗）: 先行返金 90 の後の Child 失敗は追加返金 10 のみを"
            "作り、返金累計は 100。A=0 / B=0 / Escrow=0"
        )

    # 共通: A は一切受け取らず、Escrow は空になる
    assert final.job_states[root_id] == "FAILED"
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.wallet_units[AGENT_A_ID] == agent_a_wallet_baseline
    assert final.escrow_units == 0
    assert final.available_units == 0
    assert final.locked_units == 0
    assert harness.check_payee_entitlement(final) == []
    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label

    world.report.note(note)
    return {
        "scenario_id": world.scenario_id,
        "terminal_job_states": {root_id: "FAILED", child_id: child_state},
        "paid": expected_paid,
        "refunded": expected_refunded,
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }
