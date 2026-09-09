"""N04: 有効提出後の拒否操作（計画書 第18節 異常系 4 / N04・「N04の具体的呼び出し」）。

R は Root Requester、A は Parent Worker かつ Child Requester、B は Child Worker。

fixture 1（Parent は有効 LEASED、Child は part-1・予算 10・B の有効提出による
SUBMITTED、返金予約なし、検収期限前の固定時刻）:

| 呼び出し | 期待コード |
|---|---|
| `ojp --actor A job refund CHILD` | FORBIDDEN |
| `ojp --actor A job refund ROOT`  | FORBIDDEN |
| `ojp --actor A job abandon CHILD --lease B_LEASE` | FORBIDDEN |
| `ojp --actor B job abandon CHILD --lease B_LEASE` | INVALID_STATE |
| `ojp --actor R job refund CHILD` | INVALID_TARGET |
| `ojp --actor R job refund ROOT`  | INVALID_STATE |

fixture 2（Root 自身の有効提出後・全 Child 判定終端・Root SUBMITTED・返金予約
なし）: R の `job refund ROOT` と A の `job abandon ROOT --lease A_LEASE` は
どちらも INVALID_STATE。

どちらの fixture でも:

- 拒否確認中は tick を進めない。各呼び出しに新しい operation_id を使う
- 全拒否操作の CLI 終了コードは 2
- Job 状態・有効 Submission・Acceptance・全口座残高・Journal・
  PaymentOperation・Receipt を前後比較して変化を認めない
- MCP の `tools/list` は第13節の 8 件と完全一致し、返金・任意 fail/cancel
  tool が無い

テストのために操作面（tool / command）を追加しない。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

CHILD_BUDGET_UNITS = 10_000_000
REFUND_UNITS = 90_000_000

# 計画書 第13節の 8 tools。返金・任意 fail/cancel tool は含まれない
EXPECTED_TOOL_NAMES = [
    "ojp_approve",
    "ojp_claim_job",
    "ojp_create_child_job",
    "ojp_dispute",
    "ojp_get_job",
    "ojp_heartbeat",
    "ojp_list_jobs",
    "ojp_submit",
]


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n04_rejections_after_child_submission(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, "N04A")
    failed = False
    try:
        expected = await _run_n04_child_submitted(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


@pytest.mark.anyio
async def test_n04_rejections_after_root_submission(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, "N04B")
    failed = False
    try:
        expected = await _run_n04_root_submitted(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_n04_child_submitted(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    world.observe("after root fund")

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            # MCP は第13節の 8 件だけ（返金・任意 fail/cancel tool は無い）
            assert await agent_a.list_tool_names() == EXPECTED_TOOL_NAMES
            assert await agent_b.list_tool_names() == EXPECTED_TOOL_NAMES

            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

            await agent_b.call(
                "ojp_submit",
                {
                    "job_id": child_id,
                    "lease_id": start.child_lease_id,
                    "version_id": start.child_version_id,
                    "artifact_json": json.dumps({"sum": 6}),
                    "operation_id": world.next_operation_id("submit", "b-child"),
                },
            )

    before = world.observe("before N04 rejections (child SUBMITTED)")
    # fixture の前提条件（Parent 有効 LEASED / Child SUBMITTED / 返金予約なし /
    # 検収期限前の固定時刻）
    assert before.job_states[root_id] == "LEASED"
    assert before.job_states[child_id] == "SUBMITTED"
    assert before.active_lease_worker(root_id) == AGENT_A_ID
    assert before.submission_worker(child_id) == AGENT_B_ID
    assert before.operations_for(root_id, "refund") == []
    assert before.operations_for(child_id, "refund") == []
    child_submission = _only_submission(before, child_id)
    assert world.current_clock_us() < int(child_submission["review_due_at_us"])
    assert before.locked_for("child_work") == CHILD_BUDGET_UNITS
    assert before.available_units == REFUND_UNITS

    rejections = [
        (AGENT_A_ID, ["job", "refund", child_id], "FORBIDDEN", "a-refund-child"),
        (AGENT_A_ID, ["job", "refund", root_id], "FORBIDDEN", "a-refund-root"),
        (
            AGENT_A_ID,
            ["job", "abandon", child_id, "--lease", start.child_lease_id],
            "FORBIDDEN",
            "a-abandon-child",
        ),
        (
            AGENT_B_ID,
            ["job", "abandon", child_id, "--lease", start.child_lease_id],
            "INVALID_STATE",
            "b-abandon-child",
        ),
        (REQUESTER_ID, ["job", "refund", child_id], "INVALID_TARGET", "r-refund-child"),
        (REQUESTER_ID, ["job", "refund", root_id], "INVALID_STATE", "r-refund-root"),
    ]
    _run_rejections(world, rejections)

    after = world.observe("after N04 rejections (child SUBMITTED)")
    _assert_state_unchanged(before, after)
    # 検収期限前の固定時刻のまま（tick を進めていない）
    assert world.current_clock_us() < int(child_submission["review_due_at_us"])

    world.report.note(
        "N04（Child SUBMITTED fixture）: 6 件の拒否呼び出しがすべて exit=2 と"
        "所定のエラーコードになり、Job 状態・Submission・Acceptance・残高・"
        "Journal・PaymentOperation・Receipt はいずれも不変"
    )
    return {
        "scenario_id": "N04A",
        "terminal_job_states": {root_id: "LEASED", child_id: "SUBMITTED"},
        "paid": {},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "10.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


async def _run_n04_root_submitted(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    world.observe("after root fund")

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            assert await agent_a.list_tool_names() == EXPECTED_TOOL_NAMES

            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

            # 全 Child を判定終端にする（B の提出前 abandon）
            world.run_cli(
                ["job", "abandon", child_id, "--lease", start.child_lease_id],
                actor=AGENT_B_ID,
                operation_id=world.next_operation_id("abandon", "b-child"),
                action="job abandon (child)",
            )
            world.observe("after child abandon by B")

            # Root 自身の有効提出
            await agent_a.call(
                "ojp_submit",
                {
                    "job_id": root_id,
                    "lease_id": start.root_lease_id,
                    "version_id": start.root_version_id,
                    "artifact_json": json.dumps({"sum": 15}),
                    "operation_id": world.next_operation_id("submit", "a-root"),
                },
            )

    before = world.observe("before N04 rejections (root SUBMITTED)")
    assert before.job_states[root_id] == "SUBMITTED"
    assert before.job_states[child_id] == "FAILED"
    assert before.submission_worker(root_id) == AGENT_A_ID
    assert before.operations == []  # 返金予約なし（PaymentOperation が 1 件も無い）
    assert before.acceptances == []
    root_submission = _only_submission(before, root_id)
    assert world.current_clock_us() < int(root_submission["review_due_at_us"])

    rejections = [
        (REQUESTER_ID, ["job", "refund", root_id], "INVALID_STATE", "r-refund-root"),
        (
            AGENT_A_ID,
            ["job", "abandon", root_id, "--lease", start.root_lease_id],
            "INVALID_STATE",
            "a-abandon-root",
        ),
    ]
    _run_rejections(world, rejections)

    after = world.observe("after N04 rejections (root SUBMITTED)")
    _assert_state_unchanged(before, after)

    world.report.note(
        "N04（Root SUBMITTED fixture）: R の job refund ROOT と A の"
        " job abandon ROOT --lease A_LEASE はどちらも exit=2 / INVALID_STATE で、"
        "Job 状態・Submission・Acceptance・残高・Journal・PaymentOperation・"
        "Receipt はいずれも不変"
    )
    return {
        "scenario_id": "N04B",
        "terminal_job_states": {root_id: "SUBMITTED", child_id: "FAILED"},
        "paid": {},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


def _run_rejections(
    world: harness.E2EWorld, rejections: list[tuple[str, list[str], str, str]]
) -> None:
    """各呼び出しを新しい operation_id で実行し、exit=2 と期待コードを確認する。

    tick は一切進めない（拒否確認中に Lifecycle を動かさない）。
    """
    for actor, args, expected_code, label in rejections:
        result = world.run_cli(
            args,
            actor=actor,
            operation_id=world.next_operation_id(args[1], label),
            expect_ok=False,
            action=f"{' '.join(args[:2])} ({label})",
        )
        described = f"{actor}: ojp {' '.join(args)}"
        assert result.returncode == 2, f"{described} -> exit={result.returncode}"
        assert result.error_code == expected_code, (
            f"{described} -> {result.error_code} (expected {expected_code});"
            f" stdout={result.stdout}"
        )


def _only_submission(snapshot: harness.Snapshot, job_id: str) -> dict[str, Any]:
    submissions = [s for s in snapshot.submissions if s["job_id"] == job_id]
    assert len(submissions) == 1, f"expected exactly one submission for {job_id}"
    return submissions[0]


def _assert_state_unchanged(
    before: harness.Snapshot, after: harness.Snapshot
) -> None:
    """Job 状態・有効 Submission・Acceptance・全口座残高・Journal・
    PaymentOperation・Receipt を前後比較する（拒否の Operation 結果と監査記録
    の追加だけは認めるため、operations テーブル・events は比較対象にしない）。"""
    assert after.job_states == before.job_states
    assert after.submissions == before.submissions
    assert after.acceptances == before.acceptances
    assert after.leases == before.leases
    assert after.accounts == before.accounts
    assert after.journal == before.journal
    assert after.operations == before.operations
    assert after.receipts == before.receipts
    assert after.wallet_units == before.wallet_units
    assert after.deposit_units == before.deposit_units
    assert after.escrow_units == before.escrow_units
    assert after.available_units == before.available_units
    assert after.locked_units == before.locked_units
    assert after.locked_breakdown_units == before.locked_breakdown_units
    assert after.paid_units == before.paid_units
    assert after.refunded_units == before.refunded_units
