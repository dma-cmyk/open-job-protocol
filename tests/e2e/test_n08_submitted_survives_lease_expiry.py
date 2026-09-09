"""N08: 有効提出後に旧 Lease 期限だけを越えても SUBMITTED を維持する
（計画書 第18節 異常系 8 / N08・「N08の専用時刻fixture」）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 8 / N08 | 専用timing policyで有効提出後、review_due_at前かつ旧Lease expires_at後へ進めてtick | SUBMITTEDを維持しOPEN/EXPIREDへ戻らない。Acceptance・支払い予約なし、Leaseは提出完了で閉じたまま |

専用時刻 fixture（第18節「N08の専用時刻fixture」そのまま）:

- Root JobVersion の Lease を 300 秒、Root deadline を `t0+300秒` より後
- Child JobVersion の Lease を 60 秒、review_window を 120 秒、Child deadline
  を `t0+180秒` より後（Child の timing はカタログ entry の事前許可として
  Root 公開前にカードへ書く。A が引数で timing を指定することはできない）
- 共有 Clock を `t0` に固定したまま A が Root を Claim して Child を作成し、
  B も Child を Claim する（A: `expires_at=t0+300秒`、B: `expires_at=t0+60秒`）
- B は共有 Clock の `t0+1秒` で有効提出する（`review_due_at=t0+121秒`）
- harness は共有 Clock を `t0+61秒` へ進め、
  `child_expires_at < now < child_review_due_at < root_lease_expires_at`
  を確認してから独立 tick を 1 回実行する

tick 後の期待結果:

- Child は SUBMITTED（OPEN / EXPIRED へ戻らない）
- Acceptance と PaymentOperation は 0 件
- 全口座残高は提出直後から不変
- Lease の `expires_at` は監査値として `t0+60秒` のまま、`closed_reason` は
  提出完了（'submitted'）のまま
- expiry 処理による Event も新 Lease も作らない

その後、同じ fixture を `review_due_at` まで進めて通常の自動承認経路が
機能することを N11 の一部として確認する。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "N08"

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000

ROOT_LEASE_SECONDS = 300
CHILD_LEASE_SECONDS = 60
CHILD_REVIEW_WINDOW_SECONDS = 120

ROOT_TIMING_POLICY = {
    "lease_seconds": ROOT_LEASE_SECONDS,
    "heartbeat_seconds": 20,
    "review_window_seconds": 30,
    "dispute_window_seconds": 30,
}
CHILD_TIMING_POLICY = {
    "lease_seconds": CHILD_LEASE_SECONDS,
    "heartbeat_seconds": 20,
    "review_window_seconds": CHILD_REVIEW_WINDOW_SECONDS,
    "dispute_window_seconds": 30,
}
# Root deadline は t0+300 秒より後、Child deadline は t0+180 秒より後
# （カードの既定 deadline 2027-01-16T00:00:00Z は t0 の 16 時間後で両方を満たす）
FAR_DEADLINE = "2027-01-16T00:00:00.000000Z"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n08_submitted_survives_old_lease_expiry(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_n08(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_n08(world: harness.E2EWorld) -> dict:
    card_path = harness.write_root_card(
        world,
        "n08",
        timing_policy=ROOT_TIMING_POLICY,
        catalog_timing_policy=CHILD_TIMING_POLICY,
        deadline=FAR_DEADLINE,
    )
    root_id, _ = harness.create_and_fund_root(world, card_path=card_path)
    world.observe("after root fund")

    t0_us = world.current_clock_us()

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            # 共有 Clock を t0 に固定したまま Claim / Child 作成 / Child Claim
            start = await harness.start_root_child_part1(
                world, agent_a, agent_b, child_deadline=FAR_DEADLINE
            )
            child_id = start.child_id
            assert world.current_clock_us() == t0_us

            root_lease_expires_us = world.lease_expires_at_us(start.root_lease_id)
            child_lease_expires_us = world.lease_expires_at_us(start.child_lease_id)
            assert root_lease_expires_us == t0_us + ROOT_LEASE_SECONDS * 1_000_000
            assert child_lease_expires_us == t0_us + CHILD_LEASE_SECONDS * 1_000_000

            # B は共有 Clock の t0+1 秒で有効提出する
            world.advance_clock_to(t0_us + 1_000_000)
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

    after_submit = world.observe("after child submit by B (t0+1s)")
    assert after_submit.job_states[child_id] == "SUBMITTED"
    child_submission = _only(after_submit.submissions, child_id)
    review_due_at_us = int(child_submission["review_due_at_us"])
    assert review_due_at_us == t0_us + (1 + CHILD_REVIEW_WINDOW_SECONDS) * 1_000_000
    # 提出完了で閉じた Lease（expires_at は監査値として据え置き）
    child_lease = _only_lease(after_submit, start.child_lease_id)
    assert child_lease["closed_reason"] == "submitted"
    assert int(child_lease["expires_at_us"]) == child_lease_expires_us

    # ------------------------------------------------------------------
    # 共有 Clock を t0+61 秒へ進め、順序条件を確認してから独立 tick を 1 回
    # ------------------------------------------------------------------
    world.advance_clock_to(t0_us + 61_000_000)
    now_us = world.current_clock_us()
    assert child_lease_expires_us < now_us < review_due_at_us < root_lease_expires_us

    world.tick_once(label="tick --once (after old child lease expiry)")
    after_tick = world.observe("after tick (t0+61s)")

    # SUBMITTED を維持し OPEN / EXPIRED へ戻らない
    assert after_tick.job_states[child_id] == "SUBMITTED"
    assert after_tick.job_states[root_id] == "LEASED"
    # Acceptance と PaymentOperation は 0 件
    assert after_tick.acceptances == []
    assert after_tick.operations == []
    assert after_tick.receipts == []
    # 全口座残高は提出直後から不変
    assert after_tick.accounts == after_submit.accounts
    assert after_tick.locked_breakdown_units == after_submit.locked_breakdown_units
    assert after_tick.available_units == after_submit.available_units
    assert after_tick.wallet_units == after_submit.wallet_units
    assert after_tick.journal == after_submit.journal
    assert after_tick.paid_units == 0
    assert after_tick.refunded_units == 0
    # Lease は提出完了で閉じたまま、expires_at は監査値として t0+60 秒のまま
    assert after_tick.leases == after_submit.leases
    tick_child_lease = _only_lease(after_tick, start.child_lease_id)
    assert tick_child_lease["closed_reason"] == "submitted"
    assert int(tick_child_lease["expires_at_us"]) == child_lease_expires_us
    # expiry 処理による Event も新 Lease も作らない
    assert after_tick.events == after_submit.events
    assert after_tick.events_for(child_id, "expire") == []
    assert len([l for l in after_tick.leases if l["job_id"] == child_id]) == 1
    # Submission も増えない（review_due_at も据え置き）
    assert after_tick.submissions == after_submit.submissions

    # ------------------------------------------------------------------
    # review_due_at まで進めると通常の自動承認経路が機能する（N11 の一部）
    # ------------------------------------------------------------------
    world.advance_clock_to(review_due_at_us)
    assert world.current_clock_us() == review_due_at_us
    world.run_tick_until(
        lambda conn: conn.execute(
            "SELECT state FROM jobs WHERE id = ?", (child_id,)
        ).fetchone()["state"]
        == "DONE",
        label="tick --watch (child auto approval at review_due_at)",
    )
    after_auto_approve = world.observe("after auto approval at review_due_at")
    assert after_auto_approve.job_states[child_id] == "DONE"
    child_acceptances = [
        a for a in after_auto_approve.acceptances if a["job_id"] == child_id
    ]
    assert len(child_acceptances) == 1
    assert child_acceptances[0]["decision"] == "APPROVED"
    assert child_acceptances[0]["decided_by"] == harness.SYSTEM_ID

    world.settle()
    final = world.observe("after settlement")
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.locked_for("child_payout") == 0
    assert final.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert harness.check_payee_entitlement(final) == []

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label

    world.report.note(
        "N08: child_expires_at < now < child_review_due_at < root_lease_expires_at"
        " での tick は Child を SUBMITTED のまま保ち、Acceptance・支払い予約・"
        "残高変化・expiry Event・新 Lease をいずれも作らない。"
        "review_due_at まで進めると通常の自動承認経路で B へ 10"
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


def _only(rows: list[dict], job_id: str) -> dict:
    matching = [row for row in rows if row["job_id"] == job_id]
    assert len(matching) == 1, matching
    return matching[0]


def _only_lease(snapshot: harness.Snapshot, lease_id: str) -> dict:
    matching = [lease for lease in snapshot.leases if lease["id"] == lease_id]
    assert len(matching) == 1, matching
    return matching[0]
