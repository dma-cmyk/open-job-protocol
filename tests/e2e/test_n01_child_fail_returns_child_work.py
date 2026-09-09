"""N01: Child 未提出の失敗（計画書 第18節「添付要件の異常系14項目」1 / N01）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 1 / N01 | Child未提出の失敗 | child_work 10 が Parent available へ戻る。A Wallet への返金ではない |

「Child 未提出の失敗」は第18節どおり**提出前 abandon**と**提出前 expiry**の
2 通りで再現する（不正な一方的返金では代替しない）。どちらでも:

- child_work -10 / available +10（Journal reason='return' が正本）
- Root Escrow の内部移動であり、A の Wallet へは 1 単位も動かない
- paid / refunded は 0 のまま（Requester へも返金されない）
- Parent（Root）は LEASED のまま生存し、Child だけが終端になる

expiry 版は共有 Clock を Child Lease 期限の直後まで進めて独立 tick で失効
させる。Root Lease は A の heartbeat で延長し、Parent 終端が混ざらないよう
にする（Parent 失敗との合成は N02・N03 が扱う）。

実プロセス構成: Requester は CLI 子プロセス、A / B は別々の MCP stdio
サーバープロセス、期限処理は独立 tick プロセス。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n01_child_abandon_returns_child_work(tmp_path: Path) -> None:
    """B が提出前に abandon した Child の 10 が Parent の available へ戻る。"""
    world = harness.create_world(tmp_path, "N01A")
    failed = False
    try:
        expected = await _run_n01_abandon(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


@pytest.mark.anyio
async def test_n01_child_expiry_returns_child_work(tmp_path: Path) -> None:
    """提出前に Lease 期限を越えた Child の 10 が Parent の available へ戻る。"""
    world = harness.create_world(tmp_path, "N01B")
    failed = False
    try:
        expected = await _run_n01_expiry(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_n01_abandon(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    baseline = world.observe("after root fund")

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)

    child_id = start.child_id
    before = world.observe("before child abandon")
    assert before.job_states[child_id] == "LEASED"
    assert before.locked_for("child_work") == CHILD_BUDGET_UNITS
    assert before.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS

    world.run_cli(
        ["job", "abandon", child_id, "--lease", start.child_lease_id],
        actor=AGENT_B_ID,
        operation_id=world.next_operation_id("abandon", "b-child"),
        action="job abandon (child)",
    )
    after = world.observe("after child abandon by B")

    _assert_child_work_returned_to_parent(after, root_id, child_id, baseline)
    assert after.job_states[child_id] == "FAILED"

    world.report.note(
        "N01（abandon）: B の提出前 abandon で child_work 10 が Parent の"
        " available へ戻る。A Wallet への返金ではない"
    )
    return _expected_report("N01A", root_id, child_id, "FAILED")


async def _run_n01_expiry(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    baseline = world.observe("after root fund")

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

            child_lease_due_us = world.lease_expires_at_us(start.child_lease_id)
            root_lease_due_us = world.lease_expires_at_us(start.root_lease_id)

            # Root Lease と Child Lease は同じ長さなので、そのまま Child 期限を
            # 越えると Parent も失効する。A が Root を heartbeat して延長し、
            # 「Parent 生存中の Child 失効」だけを観測できるようにする。
            world.advance_clock_to(child_lease_due_us - 1_000_000)
            await agent_a.call(
                "ojp_heartbeat",
                {
                    "job_id": root_id,
                    "lease_id": start.root_lease_id,
                    "generation": _lease_generation(world, start.root_lease_id),
                    "operation_id": world.next_operation_id("heartbeat", "a-root"),
                },
            )
            extended_root_lease_due_us = world.lease_expires_at_us(start.root_lease_id)
            assert extended_root_lease_due_us > root_lease_due_us

    before = world.observe("before child lease expiry")
    assert before.job_states[child_id] == "LEASED"
    assert before.locked_for("child_work") == CHILD_BUDGET_UNITS

    # Child Lease 期限の直後・Root Lease 期限より前へ共有 Clock を進める
    world.advance_clock_to(child_lease_due_us + 1_000_000)
    now_us = world.current_clock_us()
    assert child_lease_due_us <= now_us < extended_root_lease_due_us

    world.run_tick_until(
        lambda conn: conn.execute(
            "SELECT state FROM jobs WHERE id = ?", (child_id,)
        ).fetchone()["state"]
        in harness.TERMINAL_JOB_STATES,
        label="tick --watch (child lease expiry)",
    )
    after = world.observe("after child lease expiry")

    _assert_child_work_returned_to_parent(after, root_id, child_id, before)
    assert after.job_states[child_id] == "EXPIRED"
    child_leases = [lease for lease in after.leases if lease["job_id"] == child_id]
    assert [lease["closed_reason"] for lease in child_leases] == ["expired"]

    world.report.note(
        "N01（expiry）: 提出前の Child Lease 失効で child_work 10 が Parent の"
        " available へ戻る。A Wallet への返金ではない"
    )
    return _expected_report("N01B", root_id, child_id, "EXPIRED")


def _lease_generation(world: harness.E2EWorld, lease_id: str) -> int:
    with world.read_only_connection() as conn:
        row = conn.execute(
            "SELECT generation FROM leases WHERE id = ?", (lease_id,)
        ).fetchone()
    assert row is not None, f"lease not found: {lease_id}"
    return int(row["generation"])


def _assert_child_work_returned_to_parent(
    after: harness.Snapshot,
    root_id: str,
    child_id: str,
    baseline: harness.Snapshot,
) -> None:
    """N01 の期待結果: 10 は Parent の available へ戻り、A Wallet へは行かない。"""
    # 第9節「Child failure（判定未確定の原資）: child_work -10 / available +10」。
    # 内部移動は Journal（reason='return'）が正本。
    assert after.journal_total("child_work", reason="return") == -CHILD_BUDGET_UNITS
    assert after.journal_total("available", reason="return") == CHILD_BUDGET_UNITS
    assert after.locked_for("child_work") == 0
    assert after.locked_units == 0
    assert after.available_units == ROOT_BUDGET_UNITS
    assert after.escrow_units == ROOT_BUDGET_UNITS

    # A の Wallet への返金ではない（Root Escrow 内部の移動だけ）。
    # Requester・B の Wallet も動かない
    assert after.wallet_units == baseline.wallet_units
    assert after.paid_units == 0
    assert after.refunded_units == 0
    assert after.receipts == []
    assert after.operations == []
    assert after.refunded_to(REQUESTER_ID) == 0
    assert after.paid_to(AGENT_A_ID) == 0
    assert after.paid_to(AGENT_B_ID) == 0

    # Parent は生存したまま（Parent 終端を Child 失敗から誘発しない）
    assert after.job_states[root_id] == "LEASED"
    assert after.active_lease_worker(root_id) == AGENT_A_ID
    assert after.active_lease_worker(child_id) is None
    assert after.submission_worker(child_id) is None


def _expected_report(
    scenario_id: str, root_id: str, child_id: str, child_state: str
) -> dict:
    return {
        "scenario_id": scenario_id,
        "terminal_job_states": {root_id: "LEASED", child_id: child_state},
        "paid": {},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }
