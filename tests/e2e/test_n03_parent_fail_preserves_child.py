"""N03: Child が LEASED / SUBMITTED / DISPUTED のまま Parent 失敗
（計画書 第18節 異常系 3 / N03）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 3 / N03 | ChildをLEASED／SUBMITTED／DISPUTEDそれぞれにしてParent失敗 | Child状態と10の拘束を保持。最初の返金は90だけ。各Childは後続処理可能 |

3 つの独立 fixture（それぞれ新しい test DB）で次を確認する:

1. Parent 失敗（A の提出前 abandon）で Child の状態が変わらない
   （LEASED / SUBMITTED / DISPUTED のまま。Parent 終端を Child へ伝播しない）
2. child_work の 10 が拘束されたまま残る（返金原資に入らない）
3. **最初の返金は 90 だけ**（refund の PaymentOperation は 1 件・90）
4. その後も Child は後続処理できる:
   - LEASED → B が提出 → A が承認 → B へ 10
   - SUBMITTED → A が承認 → B へ 10
   - DISPUTED → 独立 tick の裁定（保存 PASS の再現）→ B へ 10
   いずれも最終的に paid(B)=10 / refunded=90 / Escrow=0 に収束する。

実プロセス構成: Requester は CLI 子プロセス、A / B は別々の MCP stdio
サーバープロセス、裁定・決済は独立 tick プロセス。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000
REFUND_UNITS = 90_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n03_parent_fail_with_leased_child(tmp_path: Path) -> None:
    """Child が LEASED のまま Parent 失敗 → Child 存続 → B 提出 → A 承認 → 10。"""
    await _run_scenario(tmp_path, "N03A", "LEASED")


@pytest.mark.anyio
async def test_n03_parent_fail_with_submitted_child(tmp_path: Path) -> None:
    """Child が SUBMITTED のまま Parent 失敗 → Child 存続 → A 承認 → 10。"""
    await _run_scenario(tmp_path, "N03B", "SUBMITTED")


@pytest.mark.anyio
async def test_n03_parent_fail_with_disputed_child(tmp_path: Path) -> None:
    """Child が DISPUTED のまま Parent 失敗 → Child 存続 → 裁定 → 10。"""
    await _run_scenario(tmp_path, "N03C", "DISPUTED")


async def _run_scenario(tmp_path: Path, scenario_id: str, child_state: str) -> None:
    world = harness.create_world(tmp_path, scenario_id)
    failed = False
    try:
        expected = await _run_n03(world, child_state)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_n03(world: harness.E2EWorld, child_state: str) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    agent_a_wallet_baseline = after_fund.wallet_units[AGENT_A_ID]
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]
    requester_wallet_baseline = after_fund.wallet_units[REQUESTER_ID]

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id
            submission_id: str | None = None

            if child_state in {"SUBMITTED", "DISPUTED"}:
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

            if child_state == "DISPUTED":
                # 異議は Child Requester（A）が出す（第5節）。裁定は tick が行う
                await agent_a.call(
                    "ojp_dispute",
                    {
                        "job_id": child_id,
                        "submission_id": submission_id,
                        "condition_id": "sum",
                        "reason_code": "CONDITION_MISMATCH",
                        "operation_id": world.next_operation_id("dispute", "a-child"),
                    },
                )
                world.observe("after child dispute by A")

            before_parent_fail = world.observe(f"child is {child_state}")
            assert before_parent_fail.job_states[child_id] == child_state
            assert before_parent_fail.locked_for("child_work") == CHILD_BUDGET_UNITS
            assert before_parent_fail.available_units == REFUND_UNITS

            # ----------------------------------------------------------
            # Parent（Root）を提出前 abandon で失敗させる
            # ----------------------------------------------------------
            world.run_cli(
                ["job", "abandon", root_id, "--lease", start.root_lease_id],
                actor=AGENT_A_ID,
                operation_id=world.next_operation_id("abandon", "a-root"),
                action="job abandon (root)",
            )
            after_parent_fail = world.observe("after root abandon by A")

            # 1. Child の状態は変わらない（Parent 終端を Child へ伝播しない）
            assert after_parent_fail.job_states[root_id] == "FAILED"
            assert after_parent_fail.job_states[child_id] == child_state
            # 2. 10 の拘束は保持される
            assert after_parent_fail.locked_for("child_work") == CHILD_BUDGET_UNITS
            # 返金 90 は予約されたまま（決済は独立 tick が行う）
            assert after_parent_fail.locked_for("refund") == REFUND_UNITS
            assert (
                after_parent_fail.locked_units == CHILD_BUDGET_UNITS + REFUND_UNITS
            )
            assert after_parent_fail.available_units == 0
            # 3. 最初の返金は 90 だけ
            refund_ops = after_parent_fail.operations_for(root_id, "refund")
            assert len(refund_ops) == 1
            assert refund_ops[0]["amount"] == "90.000000"
            assert refund_ops[0]["payee_id"] == REQUESTER_ID
            assert (
                after_parent_fail.journal_total("refund", reason="reserve")
                == REFUND_UNITS
            )
            assert after_parent_fail.journal_total("child_work") == CHILD_BUDGET_UNITS
            assert after_parent_fail.paid_units == 0

            # ----------------------------------------------------------
            # 4. 各 Child は後続処理できる
            # ----------------------------------------------------------
            if child_state == "LEASED":
                submit_child = await agent_b.call(
                    "ojp_submit",
                    {
                        "job_id": child_id,
                        "lease_id": start.child_lease_id,
                        "version_id": start.child_version_id,
                        "artifact_json": json.dumps({"sum": 6}),
                        "operation_id": world.next_operation_id(
                            "submit", "b-child-after-parent-fail"
                        ),
                    },
                )
                submission_id = str(submit_child.data["submission_id"])
                after = world.observe("after child submit (parent already FAILED)")
                assert after.job_states[child_id] == "SUBMITTED"

            if child_state in {"LEASED", "SUBMITTED"}:
                await agent_a.call(
                    "ojp_approve",
                    {
                        "job_id": child_id,
                        "submission_id": submission_id,
                        "operation_id": world.next_operation_id("approve", "a-child"),
                    },
                )
                after_approve = world.observe(
                    "after child approve (parent already FAILED)"
                )
                assert after_approve.job_states[child_id] == "DONE"
                assert after_approve.locked_for("child_payout") == CHILD_BUDGET_UNITS
                assert after_approve.locked_for("child_work") == 0

    if child_state == "DISPUTED":
        # 裁定は独立 tick プロセスが行う（保存 PASS を再現して承認側へ収束）
        world.run_tick_until(
            lambda conn: conn.execute(
                "SELECT state FROM jobs WHERE id = ?", (child_id,)
            ).fetchone()["state"]
            == "DONE",
            label="tick --watch (child dispute arbitration)",
        )
        after_resolve = world.observe("after child dispute resolution")
        assert after_resolve.job_states[child_id] == "DONE"
        assert [
            a["decision"] for a in after_resolve.acceptances if a["job_id"] == child_id
        ] == ["APPROVED"]

    # 返金 90 と Child の 10 を独立 tick で決済する
    world.settle()
    final = world.observe("after settlement")

    # -- 最終判定 -------------------------------------------------------
    assert final.job_states[root_id] == "FAILED"
    assert final.job_states[child_id] == "DONE"
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.refunded_to(REQUESTER_ID) == REFUND_UNITS
    # 返金は累計 90 のまま（Child は成功したので追加返金は発生しない）
    assert final.refunded_units == REFUND_UNITS
    assert len(final.operations_for(root_id, "refund")) == 1
    assert final.escrow_units == 0
    assert final.locked_units == 0
    assert final.available_units == 0

    assert final.submission_worker(child_id) == AGENT_B_ID
    assert final.submission_worker(root_id) is None
    assert final.paid_by_payee_units == {AGENT_B_ID: CHILD_BUDGET_UNITS}
    assert final.refunded_by_payee_units == {REQUESTER_ID: REFUND_UNITS}
    assert harness.check_payee_entitlement(final) == []

    assert final.wallet_units[AGENT_A_ID] == agent_a_wallet_baseline
    assert (
        final.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    assert (
        final.wallet_units[REQUESTER_ID] == requester_wallet_baseline + REFUND_UNITS
    )

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label

    world.report.note(
        f"N03（Child={child_state}）: Parent 失敗でも Child 状態と 10 の拘束を保持。"
        "最初の返金は 90 だけで、Child はその後 B へ 10 まで進む"
    )
    return {
        "scenario_id": world.scenario_id,
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
