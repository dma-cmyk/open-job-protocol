"""X05: 累計 Child 件数上限と孫発注の拒否（計画書 第18節 追加検証 X05）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X05 | max_children=3でpart-1を10で作成→失敗を3回繰り返し、4件目のpart-1を試行。別Rootではpart-1 Childの有効Worker Bが、そのChildを親としてpart-2の孫発注を試行 | 2・3件目の作成は成功し各失敗後U=0・available=100。4件目はPOLICY_LIMIT/MAX_CHILDREN。孫要求はtask_key検査より先にPOLICY_LIMIT/MAX_DEPTH、資金移動なし |

独立した 2 つの fixture:

- **X05A（件数枠）**: 同じ `part-1` を 10 で作成 → B の提出前 abandon で失敗、を
  3 回繰り返す。失敗・失効した task_key の再発注は可能だが**累計
  max_children を消費する**（第8節・第10節）。各失敗後は child_work の 10 が
  available へ戻るので U=0・available=100 に戻り、**予算枠には余裕がある**
  にもかかわらず 4 件目は `POLICY_LIMIT`/`MAX_CHILDREN` で拒否される
  （TASK_CONFLICT や MAX_AMOUNT へ化けない）
- **X05B（深さ）**: part-1 Child の有効 Worker B が、その Child を親として
  孫（part-2）を発注しようとすると `POLICY_LIMIT`/`MAX_DEPTH`。Child の
  公開 Version のカタログは空なので、**カタログ参照が先に走れば
  TASK_NOT_ALLOWED になる**。カタログ外の `part-unknown` を指定しても
  MAX_DEPTH のままであることで「task_key 検査より先」を固定する（第11節）。
  どちらの拒否でも資金は 1 単位も動かない
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET = "10.000000"
CHILD_BUDGET_UNITS = 10_000_000
CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"
MAX_CHILDREN = 3

# max_children=3 が唯一の拘束になるよう、金額・比率枠には余裕を持たせる
SUBCONTRACT_POLICY = {
    "enabled": True,
    "max_amount": "30.000000",
    "max_ratio_bps": 10000,
    "max_children": MAX_CHILDREN,
    "max_depth": 1,
}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x05_fourth_child_hits_max_children(tmp_path: Path) -> None:
    """件数枠: 失敗した 3 件分の枠は戻らず、4 件目は MAX_CHILDREN。"""
    world = harness.create_world(tmp_path, "X05A")
    await _run(world, _run_x05a)


@pytest.mark.anyio
async def test_x05_grandchild_request_is_rejected_by_max_depth(
    tmp_path: Path,
) -> None:
    """深さ: Child を親とする孫発注は task_key 検査より先に MAX_DEPTH。"""
    world = harness.create_world(tmp_path, "X05B")
    await _run(world, _run_x05b)


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


async def _run_x05a(world: harness.E2EWorld) -> dict:
    card_path = harness.write_root_card(
        world, "x05a", subcontract_policy=SUBCONTRACT_POLICY
    )
    root_id, root_version_id = harness.create_and_fund_root(
        world, card_path=card_path
    )
    baseline = world.observe("after root fund")
    assert baseline.available_units == ROOT_BUDGET_UNITS
    assert harness.subcontract_usage(world, root_id, label="initial")[
        "child_count"
    ] == 0

    child_ids: list[str] = []
    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
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

            # ----------------------------------------------------------
            # 同じ part-1 を「10 で作成 → 提出前 abandon で失敗」を 3 回
            # ----------------------------------------------------------
            for attempt in range(1, MAX_CHILDREN + 1):
                created = await agent_a.call(
                    "ojp_create_child_job",
                    {
                        "parent_job_id": root_id,
                        "lease_id": root_lease_id,
                        "task_key": "part-1",
                        "budget": CHILD_BUDGET,
                        "deadline": CHILD_DEADLINE,
                        "operation_id": world.next_operation_id(
                            "create", f"a-part-1-{attempt}"
                        ),
                    },
                )
                child_id = str(created.data["child_id"])
                child_version_id = str(created.data["version_id"])
                child_ids.append(child_id)
                world.child_ids.append(child_id)

                after_create = world.observe(f"after part-1 create #{attempt}")
                assert after_create.job_states[child_id] == "OPEN"
                assert after_create.locked_for("child_work") == CHILD_BUDGET_UNITS
                assert after_create.available_units == (
                    ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
                )
                _assert_usage(
                    world,
                    root_id,
                    in_use_units=CHILD_BUDGET_UNITS,
                    available_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
                    child_count=attempt,
                )

                claim_child = await agent_b.call(
                    "ojp_claim_job",
                    {
                        "job_id": child_id,
                        "expected_version_id": child_version_id,
                        "operation_id": world.next_operation_id(
                            "claim", f"b-part-1-{attempt}"
                        ),
                    },
                )
                # 提出前 abandon で失敗させる（不正な一方的返金で代替しない）
                world.run_cli(
                    [
                        "job",
                        "abandon",
                        child_id,
                        "--lease",
                        str(claim_child.data["lease_id"]),
                    ],
                    actor=AGENT_B_ID,
                    operation_id=world.next_operation_id(
                        "abandon", f"b-part-1-{attempt}"
                    ),
                    action=f"job abandon (part-1 #{attempt})",
                )
                after_fail = world.observe(f"after part-1 failure #{attempt}")
                assert after_fail.job_states[child_id] == "FAILED"
                # 各失敗後は U=0・available=100（child_work の 10 が戻る）
                assert after_fail.locked_units == 0
                assert after_fail.available_units == ROOT_BUDGET_UNITS
                _assert_usage(
                    world,
                    root_id,
                    in_use_units=0,
                    available_units=ROOT_BUDGET_UNITS,
                    child_count=attempt,
                )
                assert after_fail.paid_units == 0
                assert after_fail.refunded_units == 0
                assert after_fail.receipts == []

            before_fourth = world.observe("before 4th part-1 create")

            # ----------------------------------------------------------
            # 4 件目は POLICY_LIMIT/MAX_CHILDREN（枠は戻らない）
            # ----------------------------------------------------------
            rejected = await agent_a.call(
                "ojp_create_child_job",
                {
                    "parent_job_id": root_id,
                    "lease_id": root_lease_id,
                    "task_key": "part-1",
                    "budget": CHILD_BUDGET,
                    "deadline": CHILD_DEADLINE,
                    "operation_id": world.next_operation_id(
                        "create", "a-part-1-fourth"
                    ),
                },
                expect_ok=False,
            )
            assert rejected.error_code == "POLICY_LIMIT", rejected.payload
            details = rejected.payload["error"]["details"]
            assert details is not None and details["reason"] == "MAX_CHILDREN", details

    final = world.observe("after rejected 4th part-1 create")
    _assert_no_fund_movement(before_fourth, final)
    # 4 件目の Job 行は作られない（3 件のまま・すべて FAILED）
    assert sorted(job_id for job_id in final.job_states if job_id != root_id) == (
        sorted(child_ids)
    )
    assert [final.job_states[child_id] for child_id in child_ids] == (
        ["FAILED"] * MAX_CHILDREN
    )
    assert _child_rows_for_task(world, root_id, "part-1") == MAX_CHILDREN
    _assert_usage(
        world,
        root_id,
        in_use_units=0,
        available_units=ROOT_BUDGET_UNITS,
        child_count=MAX_CHILDREN,
    )
    assert final.available_units == ROOT_BUDGET_UNITS
    assert final.locked_units == 0
    assert final.paid_units == 0
    assert final.refunded_units == 0

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label

    world.report.note(
        "X05A: 同じ part-1 の作成→失敗を 3 回繰り返すと各失敗後に"
        " U=0・available=100 へ戻るが累計件数は 3 を消費し、4 件目は"
        " POLICY_LIMIT/MAX_CHILDREN。Job 行も資金移動も増えない"
    )
    return {
        "scenario_id": "X05A",
        "terminal_job_states": {
            root_id: "LEASED",
            **{child_id: "FAILED" for child_id in child_ids},
        },
        "paid": {},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


async def _run_x05b(world: harness.E2EWorld) -> dict:
    card_path = harness.write_root_card(
        world, "x05b", subcontract_policy=SUBCONTRACT_POLICY
    )
    root_id, _ = harness.create_and_fund_root(world, card_path=card_path)
    world.observe("after root fund")

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

            before = world.observe("before grandchild attempts")
            assert before.job_states[child_id] == "LEASED"
            assert before.active_lease_worker(child_id) == AGENT_B_ID
            assert before.locked_for("child_work") == CHILD_BUDGET_UNITS
            assert before.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
            # Child の公開 Version はカタログを持たない（空）ので、カタログ参照が
            # 先に走れば TASK_NOT_ALLOWED になる位置関係になっている
            assert _child_catalog(world, child_id) == []

            # ----------------------------------------------------------
            # 孫発注: カタログ内の part-2 でも、カタログ外の part-unknown でも
            # POLICY_LIMIT/MAX_DEPTH（task_key 検査より先）
            # ----------------------------------------------------------
            for task_key, label in (
                ("part-2", "grandchild-part-2"),
                ("part-unknown", "grandchild-unknown"),
            ):
                rejected = await agent_b.call(
                    "ojp_create_child_job",
                    {
                        "parent_job_id": child_id,
                        "lease_id": start.child_lease_id,
                        "task_key": task_key,
                        "budget": CHILD_BUDGET,
                        "deadline": CHILD_DEADLINE,
                        "operation_id": world.next_operation_id("create", label),
                    },
                    expect_ok=False,
                )
                assert rejected.error_code == "POLICY_LIMIT", rejected.payload
                assert rejected.error_code != "TASK_NOT_ALLOWED"
                details = rejected.payload["error"]["details"]
                assert details is not None and details["reason"] == "MAX_DEPTH", details

    final = world.observe("after rejected grandchild attempts")
    _assert_no_fund_movement(before, final)
    # 孫 Job は作られない（Root の子は part-1 だけ）
    assert set(final.job_states) == {root_id, child_id}
    with world.read_only_connection() as conn:
        grandchildren = conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE parent_id = ?", (child_id,)
        ).fetchone()["n"]
    assert int(grandchildren) == 0
    _assert_usage(
        world,
        root_id,
        in_use_units=CHILD_BUDGET_UNITS,
        available_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        child_count=1,
    )

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label

    world.report.note(
        "X05B: part-1 Child の有効 Worker B による孫発注は、カタログ内の"
        " part-2 でもカタログ外の part-unknown でも POLICY_LIMIT/MAX_DEPTH。"
        "TASK_NOT_ALLOWED へ化けず、資金も Job 行も動かない"
    )
    return {
        "scenario_id": "X05B",
        "terminal_job_states": {root_id: "LEASED", child_id: "LEASED"},
        "paid": {},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "10.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


def _assert_usage(
    world: harness.E2EWorld,
    root_id: str,
    *,
    in_use_units: int,
    available_units: int,
    child_count: int,
) -> None:
    usage = harness.subcontract_usage(world, root_id, label="policy usage")
    assert harness.parse_amount_string(usage["in_use"]) == in_use_units, usage
    assert harness.parse_amount_string(usage["available"]) == available_units, usage
    assert usage["child_count"] == child_count, usage


def _assert_no_fund_movement(
    before: harness.Snapshot, after: harness.Snapshot
) -> None:
    """拒否では資金が動かない（口座・Journal・Wallet・予約・Receipt が不変）。"""
    assert after.accounts == before.accounts
    assert after.journal == before.journal
    assert after.wallet_units == before.wallet_units
    assert after.operations == before.operations
    assert after.receipts == before.receipts
    assert after.deposit_units == before.deposit_units
    assert after.available_units == before.available_units
    assert after.locked_breakdown_units == before.locked_breakdown_units


def _child_rows_for_task(
    world: harness.E2EWorld, root_id: str, task_key: str
) -> int:
    with world.read_only_connection() as conn:
        return int(
            conn.execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE parent_id = ? AND task_key = ?",
                (root_id, task_key),
            ).fetchone()["n"]
        )


def _child_catalog(world: harness.E2EWorld, child_id: str) -> list:
    """Child の公開 Version の task_catalog（空であることの確認に使う）。"""
    with world.read_only_connection() as conn:
        row = conn.execute(
            "SELECT v.task_catalog FROM jobs j"
            " JOIN job_versions v ON v.id = j.version_id"
            " WHERE j.id = ?",
            (child_id,),
        ).fetchone()
    return json.loads(row["task_catalog"])
