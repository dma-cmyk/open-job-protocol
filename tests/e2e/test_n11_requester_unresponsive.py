"""N11: 有効提出後に Requester が無応答（計画書 第18節 異常系 11 / N11）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 11 / N11 | 有効提出後Requesterが無応答 | 異議なしなら期限後承認。異議ありなら固定裁定。裁定無応答も既定PASSで解消 |

3 つの独立 fixture:

1. **異議なし**: B が Child へ有効提出したあと A（Child Requester）は何もし
   ない。共有 Clock を `review_due_at` へ進めて独立 tick を回すと期限後承認
   （`decided_by=system`）で Child が DONE、B へ 10。
2. **異議あり**: A が検収期限内に異議を出したまま無応答になる。独立 tick の
   固定裁定が保存 PASS を再現し、承認側へ収束して B へ 10
   （`disputes.status='RESOLVED'`・`resolution.outcome='PASS'`）。
3. **裁定応答なし**: 異議を出したあと固定判定器が応答しない。
   `disputes.due_at_us` **前**の tick は何も書かず異議を OPEN のまま残し、
   期限到来後の tick が保存済み PASS の fallback
   （`UNRESPONSIVE_ARBITER_STORED_PASS`）で解消して B へ 10。

いずれも Acceptance と支払い予約は 1 組だけで、資金が永久凍結されない。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import failpoints, harness, report
from .harness import AGENT_A_ID, AGENT_B_ID

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n11_no_dispute_auto_approves_after_review_due(tmp_path: Path) -> None:
    """異議なし: 検収期限後に自動承認され B へ 10。"""
    world = harness.create_world(tmp_path, "N11A")
    await _run(world, _scenario_no_dispute)


@pytest.mark.anyio
async def test_n11_dispute_resolved_by_fixed_arbitration(tmp_path: Path) -> None:
    """異議あり: 固定裁定が保存 PASS を再現して承認側へ収束し B へ 10。"""
    world = harness.create_world(tmp_path, "N11B")
    await _run(world, _scenario_dispute)


@pytest.mark.anyio
async def test_n11_unresponsive_arbiter_falls_back_to_stored_pass(
    tmp_path: Path,
) -> None:
    """裁定応答なし: 期限前は何も書かず、期限後に保存 PASS の fallback で解消。"""
    world = harness.create_world(tmp_path, "N11C")
    await _run(world, _scenario_unresponsive_arbiter)


async def _run(world: harness.E2EWorld, scenario) -> None:
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


async def _submitted_child(world: harness.E2EWorld):
    """Root 入金 100 → A が Root Claim・part-1 Child 10 作成 → B が Claim・有効提出。

    戻り値は (root_id, ScenarioStart, submission_id)。
    """
    root_id, _ = harness.create_and_fund_root(world)
    world.observe("after root fund")

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            submit = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": start.child_id,
                    "lease_id": start.child_lease_id,
                    "version_id": start.child_version_id,
                    "artifact_json": json.dumps({"sum": 6}),
                    "operation_id": world.next_operation_id("submit", "b-child"),
                },
            )
            submission_id = str(submit.data["submission_id"])
    after_submit = world.observe("after child submit by B")
    assert after_submit.job_states[start.child_id] == "SUBMITTED"
    return root_id, start, submission_id


async def _dispute_child(
    world: harness.E2EWorld, child_id: str, submission_id: str
) -> int:
    """A（Child Requester）が検収期限内に異議を出し、その due_at_us を返す。"""
    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
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
    after_dispute = world.observe("after child dispute by A")
    assert after_dispute.job_states[child_id] == "DISPUTED"
    disputes = [d for d in after_dispute.disputes if d["job_id"] == child_id]
    assert len(disputes) == 1
    assert disputes[0]["status"] == "OPEN"
    return int(disputes[0]["due_at_us"])


# ---------------------------------------------------------------------------
# 1. 異議なし → 期限後承認
# ---------------------------------------------------------------------------


async def _scenario_no_dispute(world: harness.E2EWorld) -> dict:
    root_id, start, submission_id = await _submitted_child(world)
    child_id = start.child_id

    before = world.snapshot("before review_due_at")
    review_due_at_us = int(
        _only(before.submissions, child_id)["review_due_at_us"]
    )
    assert world.current_clock_us() < review_due_at_us

    # 検収期限前の tick は何もしない（Requester 無応答でも先走らない）
    world.tick_once(label="tick --once (before review_due_at)")
    early = world.observe("after tick before review_due_at")
    assert early.job_states[child_id] == "SUBMITTED"
    assert early.acceptances == []
    assert early.operations == []

    world.advance_clock_to(review_due_at_us)
    world.run_tick_until(
        lambda conn: conn.execute(
            "SELECT state FROM jobs WHERE id = ?", (child_id,)
        ).fetchone()["state"]
        == "DONE",
        label="tick --watch (auto approval at review_due_at)",
    )
    approved = world.observe("after auto approval")
    _assert_single_approval(approved, child_id, decided_by=harness.SYSTEM_ID)

    final = _settle_and_assert_payment(world, root_id, child_id)
    world.report.note(
        "N11（異議なし）: Requester 無応答でも review_due_at 到来で自動承認"
        "（decided_by=system）され、B へ 10。Acceptance と支払い予約は 1 組"
    )
    return _expected_report(world, root_id, child_id, final)


# ---------------------------------------------------------------------------
# 2. 異議あり → 固定裁定
# ---------------------------------------------------------------------------


async def _scenario_dispute(world: harness.E2EWorld) -> dict:
    root_id, start, submission_id = await _submitted_child(world)
    child_id = start.child_id
    await _dispute_child(world, child_id, submission_id)

    # 固定裁定は保存済み成果物を再検証する。改竄が無いので PASS を再現し、
    # 承認側へ収束する（第12節「既存条件への FAIL が再現された場合だけ FAILED」）
    world.run_tick_until(
        lambda conn: conn.execute(
            "SELECT state FROM jobs WHERE id = ?", (child_id,)
        ).fetchone()["state"]
        == "DONE",
        label="tick --watch (fixed arbitration)",
    )
    resolved = world.observe("after fixed arbitration")
    _assert_single_approval(resolved, child_id, decided_by=harness.SYSTEM_ID)
    dispute = _only(resolved.disputes, child_id)
    assert dispute["status"] == "RESOLVED"
    assert json.loads(dispute["resolution"])["outcome"] == "PASS"

    final = _settle_and_assert_payment(world, root_id, child_id)
    world.report.note(
        "N11（異議あり）: A の異議は固定裁定が保存 PASS を再現して RESOLVED/PASS"
        "となり、承認側へ収束して B へ 10"
    )
    return _expected_report(world, root_id, child_id, final)


# ---------------------------------------------------------------------------
# 3. 裁定応答なし → 保存 PASS の fallback
# ---------------------------------------------------------------------------


async def _scenario_unresponsive_arbiter(world: harness.E2EWorld) -> dict:
    root_id, start, submission_id = await _submitted_child(world)
    child_id = start.child_id
    dispute_due_at_us = await _dispute_child(world, child_id, submission_id)

    before_fallback = world.snapshot("before dispute due_at")
    assert world.current_clock_us() < dispute_due_at_us

    # 期限前: 判定器が応答しなくても何も書かない（異議 OPEN・資金不変）
    failpoints.tick_with_unresponsive_arbiter(
        world, label="tick --once (before dispute due_at)"
    )
    early = world.observe("after unresponsive tick before dispute due_at")
    assert early.job_states[child_id] == "DISPUTED"
    assert _only(early.disputes, child_id)["status"] == "OPEN"
    assert early.acceptances == []
    assert early.operations == []
    assert early.accounts == before_fallback.accounts
    assert early.journal == before_fallback.journal

    # 期限到来後: 保存済み PASS の fallback で必ず解消する
    world.advance_clock_to(dispute_due_at_us)
    failpoints.tick_with_unresponsive_arbiter(
        world, label="tick --once (at dispute due_at)"
    )
    resolved = world.observe("after unresponsive arbiter fallback")
    assert resolved.job_states[child_id] == "DONE"
    _assert_single_approval(resolved, child_id, decided_by=harness.SYSTEM_ID)
    dispute = _only(resolved.disputes, child_id)
    assert dispute["status"] == "RESOLVED"
    assert (
        json.loads(dispute["resolution"])["outcome"]
        == "UNRESPONSIVE_ARBITER_STORED_PASS"
    )

    final = _settle_and_assert_payment(world, root_id, child_id)
    world.report.note(
        "N11（裁定応答なし）: 期限前の tick は異議を OPEN のまま残して何も書かず、"
        "期限到来後は保存 PASS の fallback（UNRESPONSIVE_ARBITER_STORED_PASS）で"
        "解消して B へ 10。資金は永久凍結されない"
    )
    return _expected_report(world, root_id, child_id, final)


# ---------------------------------------------------------------------------
# 共通の判定
# ---------------------------------------------------------------------------


def _assert_single_approval(
    snapshot: harness.Snapshot, child_id: str, *, decided_by: str
) -> None:
    acceptances = [a for a in snapshot.acceptances if a["job_id"] == child_id]
    assert len(acceptances) == 1, acceptances
    assert acceptances[0]["decision"] == "APPROVED"
    assert acceptances[0]["decided_by"] == decided_by
    payouts = snapshot.operations_for(child_id, "payout")
    assert len(payouts) == 1, payouts
    assert payouts[0]["payee_id"] == AGENT_B_ID
    assert payouts[0]["amount"] == "10.000000"


def _settle_and_assert_payment(
    world: harness.E2EWorld, root_id: str, child_id: str
) -> harness.Snapshot:
    world.settle()
    final = world.observe("after settlement")
    assert final.job_states[child_id] == "DONE"
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.paid_units == CHILD_BUDGET_UNITS
    assert final.refunded_units == 0
    assert final.locked_units == 0
    assert final.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    assert len([r for r in final.receipts if r["job_id"] == child_id]) == 1
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert harness.check_payee_entitlement(final) == []
    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
    return final


def _expected_report(
    world: harness.E2EWorld,
    root_id: str,
    child_id: str,
    final: harness.Snapshot,
) -> dict:
    return {
        "scenario_id": world.scenario_id,
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


def _only(rows: list[dict], job_id: str) -> dict:
    matching = [row for row in rows if row["job_id"] == job_id]
    assert len(matching) == 1, matching
    return matching[0]
