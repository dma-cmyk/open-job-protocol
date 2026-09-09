"""X13: task_key 重複とカタログ外タスクの拒否（計画書 第18節 追加検証 X13）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X13 | part-1が生存中／成功済みの別fixtureでpart-1を再作成。別ケースで未知part-unknownを指定してカタログ外タスク作成を試行 | 重複はTASK_CONFLICT、未知はTASK_NOT_ALLOWED。予算・件数枠に余裕を持たせ、POLICY_LIMITに隠さない。入力等の書換え拒否はX06でも検証。都度Requester確認なし |

第8節「同じ task_key には同時に生存中または成功済み Child を複数作れない。
失敗・失効したタスクの再発注は新 Child として可能」と第11節「未知の task_key や
カタログ条件のすり替えは TASK_NOT_ALLOWED、生存中または成功済み task_key の
再作成は TASK_CONFLICT」を実プロセスで観測する。

3 つの独立 fixture:

- **X13A（生存中）**: part-1 を OPEN / LEASED / SUBMITTED の各状態にして
  それぞれ再作成を試み、いずれも `TASK_CONFLICT`
- **X13B（成功済み）**: part-1 を DONE（A が承認）にしてから再作成を試み
  `TASK_CONFLICT`。送金確定後にも同じ結論
- **X13C（未知）**: `part-unknown` などカタログ外の task_key は
  `TASK_NOT_ALLOWED`

どの拒否も **POLICY_LIMIT に隠さない**ことを、拒否の時点で予算枠
（`U + budget <= max_amount_units` かつ `<= floor(D * max_ratio_bps / 10000)`
かつ `budget <= available`）と件数枠（`child_count + 1 <= max_children`）に
余裕があったことを `ledger show` の `subcontract_usage` と公開 Version の
policy から導出して示す。

**都度 Requester 確認なし**: Child の作成・承認は A だけで完結し、Root
Requester は Child に対する操作を 1 件も行わない（レポートの operation 一覧と
Acceptance の decided_by で照合する）。
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET = "10.000000"
CHILD_BUDGET_UNITS = 10_000_000
CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"

UNKNOWN_TASK_KEYS = ("part-unknown", "part-4", "sum-v1")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x13_live_task_key_conflicts(tmp_path: Path) -> None:
    """生存中（OPEN / LEASED / SUBMITTED）の part-1 の再作成は TASK_CONFLICT。"""
    await _run(harness.create_world(tmp_path, "X13A"), _run_x13a)


@pytest.mark.anyio
async def test_x13_succeeded_task_key_conflicts(tmp_path: Path) -> None:
    """成功済み（DONE）の part-1 の再作成は TASK_CONFLICT。"""
    await _run(harness.create_world(tmp_path, "X13B"), _run_x13b)


@pytest.mark.anyio
async def test_x13_unknown_task_key_is_not_allowed(tmp_path: Path) -> None:
    """カタログ外の task_key は TASK_NOT_ALLOWED。"""
    await _run(harness.create_world(tmp_path, "X13C"), _run_x13c)


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


# ---------------------------------------------------------------------------
# X13A: 生存中の task_key
# ---------------------------------------------------------------------------


async def _run_x13a(world: harness.E2EWorld) -> dict:
    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            root_id, root_lease_id = await _start(world, agent_a)

            child_id, child_version_id = await _create_child(
                world, agent_a, root_id, root_lease_id, "part-1"
            )
            created = world.observe("after part-1 create (OPEN)")
            assert created.job_states[child_id] == "OPEN"

            # OPEN のまま再作成 → TASK_CONFLICT
            await _assert_conflict(
                world, agent_a, root_id, root_lease_id, "part-1", label="open"
            )

            # LEASED にして再作成 → TASK_CONFLICT
            child_lease_id = str(
                (
                    await agent_b.call(
                        "ojp_claim_job",
                        {
                            "job_id": child_id,
                            "expected_version_id": child_version_id,
                            "operation_id": world.next_operation_id(
                                "claim", "b-part-1"
                            ),
                        },
                    )
                ).data["lease_id"]
            )
            leased = world.observe("after part-1 claim (LEASED)")
            assert leased.job_states[child_id] == "LEASED"
            await _assert_conflict(
                world, agent_a, root_id, root_lease_id, "part-1", label="leased"
            )

            # SUBMITTED にして再作成 → TASK_CONFLICT
            await agent_b.call(
                "ojp_submit",
                {
                    "job_id": child_id,
                    "lease_id": child_lease_id,
                    "version_id": child_version_id,
                    "artifact_json": json.dumps({"sum": 6}),
                    "operation_id": world.next_operation_id("submit", "b-part-1"),
                },
            )
            submitted = world.observe("after part-1 submit (SUBMITTED)")
            assert submitted.job_states[child_id] == "SUBMITTED"
            await _assert_conflict(
                world, agent_a, root_id, root_lease_id, "part-1", label="submitted"
            )

    final = world.observe("after live task_key conflicts")
    # 拒否は Job も資金も増やさない（part-1 の 1 件だけ）
    assert set(final.job_states) == {root_id, child_id}
    assert _children_with_task_key(world, root_id, "part-1") == 1
    assert final.locked_for("child_work") == CHILD_BUDGET_UNITS
    assert final.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    assert final.paid_units == 0
    assert final.refunded_units == 0
    _assert_no_requester_child_interaction(world, child_id)
    _assert_invariants(world)

    world.report.note(
        "X13A: 生存中（OPEN / LEASED / SUBMITTED）の part-1 の再作成はいずれも"
        " TASK_CONFLICT。各拒否の時点で予算枠・件数枠に余裕があり"
        " POLICY_LIMIT に隠れていない"
    )
    return {
        "scenario_id": "X13A",
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


# ---------------------------------------------------------------------------
# X13B: 成功済みの task_key
# ---------------------------------------------------------------------------


async def _run_x13b(world: harness.E2EWorld) -> dict:
    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            root_id, root_lease_id = await _start(world, agent_a)
            child_id, child_version_id = await _create_child(
                world, agent_a, root_id, root_lease_id, "part-1"
            )
            child_lease_id = str(
                (
                    await agent_b.call(
                        "ojp_claim_job",
                        {
                            "job_id": child_id,
                            "expected_version_id": child_version_id,
                            "operation_id": world.next_operation_id(
                                "claim", "b-part-1"
                            ),
                        },
                    )
                ).data["lease_id"]
            )
            submit = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": child_id,
                    "lease_id": child_lease_id,
                    "version_id": child_version_id,
                    "artifact_json": json.dumps({"sum": 6}),
                    "operation_id": world.next_operation_id("submit", "b-part-1"),
                },
            )
            # 承認は A（作成時 Requester）だけで完結する
            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": child_id,
                    "submission_id": str(submit.data["submission_id"]),
                    "operation_id": world.next_operation_id("approve", "a-part-1"),
                },
            )
            done = world.observe("after part-1 approve (DONE)")
            assert done.job_states[child_id] == "DONE"

            # 成功済み（送金前）の再作成 → TASK_CONFLICT
            await _assert_conflict(
                world, agent_a, root_id, root_lease_id, "part-1", label="done"
            )

            world.settle()
            paid = world.observe("after part-1 payment")
            assert paid.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS

            # 送金確定後の再作成も TASK_CONFLICT（枠には余裕がある）
            await _assert_conflict(
                world, agent_a, root_id, root_lease_id, "part-1", label="paid"
            )

    final = world.observe("after succeeded task_key conflicts")
    assert set(final.job_states) == {root_id, child_id}
    assert _children_with_task_key(world, root_id, "part-1") == 1
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.locked_units == 0
    assert final.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    assert [
        a["decided_by"] for a in final.acceptances if a["job_id"] == child_id
    ] == [AGENT_A_ID]
    _assert_no_requester_child_interaction(world, child_id)
    assert harness.check_payee_entitlement(final) == []
    _assert_invariants(world)

    world.report.note(
        "X13B: 成功済み（DONE・送金確定後）の part-1 の再作成も TASK_CONFLICT。"
        "承認は A だけで完結し、Root Requester への都度確認は発生しない"
    )
    return {
        "scenario_id": "X13B",
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


# ---------------------------------------------------------------------------
# X13C: 未知の task_key
# ---------------------------------------------------------------------------


async def _run_x13c(world: harness.E2EWorld) -> dict:
    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        root_id, root_lease_id = await _start(world, agent_a)
        before = world.observe("before unknown task_key attempts")
        catalog_keys = _catalog_task_keys(world, root_id)
        assert catalog_keys == ["part-1", "part-2", "part-3"]

        for task_key in UNKNOWN_TASK_KEYS:
            assert task_key not in catalog_keys
            _assert_headroom(world, root_id, budget_units=CHILD_BUDGET_UNITS)
            rejected = await agent_a.call(
                "ojp_create_child_job",
                {
                    "parent_job_id": root_id,
                    "lease_id": root_lease_id,
                    "task_key": task_key,
                    "budget": CHILD_BUDGET,
                    "deadline": CHILD_DEADLINE,
                    "operation_id": world.next_operation_id(
                        "create", f"a-{task_key}"
                    ),
                },
                expect_ok=False,
            )
            assert rejected.error_code == "TASK_NOT_ALLOWED", rejected.payload
            assert rejected.error_code != "POLICY_LIMIT"
            assert rejected.error_code != "TASK_CONFLICT"

    final = world.observe("after unknown task_key attempts")
    # Job も資金も 1 単位も動かない
    assert set(final.job_states) == {root_id}
    assert final.accounts == before.accounts
    assert final.journal == before.journal
    assert final.wallet_units == before.wallet_units
    assert final.operations == []
    assert final.available_units == ROOT_BUDGET_UNITS
    assert final.locked_units == 0
    with world.read_only_connection() as conn:
        children = conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE parent_id = ?", (root_id,)
        ).fetchone()["n"]
    assert int(children) == 0
    _assert_invariants(world)

    world.report.note(
        "X13C: カタログ外の task_key（part-unknown / part-4 / Root の sum-v1）は"
        " TASK_NOT_ALLOWED。予算枠・件数枠は満杯ではないので POLICY_LIMIT には"
        "隠れず、Job も資金も動かない"
    )
    return {
        "scenario_id": "X13C",
        "terminal_job_states": {root_id: "LEASED"},
        "paid": {},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


# ---------------------------------------------------------------------------
# 共通手順
# ---------------------------------------------------------------------------


async def _start(
    world: harness.E2EWorld, agent_a: harness.AgentSession
) -> tuple[str, str]:
    """Root 入金 100 → A が Root を Claim（policy は第11節の既定カード）。"""
    root_id, root_version_id = harness.create_and_fund_root(world)
    world.observe("after root fund")
    claim = await agent_a.call(
        "ojp_claim_job",
        {
            "job_id": root_id,
            "expected_version_id": root_version_id,
            "operation_id": world.next_operation_id("claim", "a-root"),
        },
    )
    world.observe("after root claim by A")
    return root_id, str(claim.data["lease_id"])


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


async def _assert_conflict(
    world: harness.E2EWorld,
    agent_a: harness.AgentSession,
    root_id: str,
    root_lease_id: str,
    task_key: str,
    *,
    label: str,
) -> None:
    """再作成が TASK_CONFLICT になることと、枠に余裕があったことを確認する。"""
    before = world.observe(f"before task_key conflict ({label})")
    _assert_headroom(world, root_id, budget_units=CHILD_BUDGET_UNITS)
    rejected = await agent_a.call(
        "ojp_create_child_job",
        {
            "parent_job_id": root_id,
            "lease_id": root_lease_id,
            "task_key": task_key,
            "budget": CHILD_BUDGET,
            "deadline": CHILD_DEADLINE,
            "operation_id": world.next_operation_id("create", f"a-dup-{label}"),
        },
        expect_ok=False,
    )
    assert rejected.error_code == "TASK_CONFLICT", rejected.payload
    assert rejected.error_code != "POLICY_LIMIT"
    assert rejected.payload["error"]["details"] is None, rejected.payload
    after = world.observe(f"after task_key conflict ({label})")
    assert after.accounts == before.accounts
    assert after.journal == before.journal
    assert after.job_states == before.job_states
    assert after.wallet_units == before.wallet_units
    assert after.operations == before.operations


def _assert_headroom(
    world: harness.E2EWorld, root_id: str, *, budget_units: int
) -> None:
    """拒否の時点で予算枠・件数枠に余裕があったことを示す（POLICY_LIMIT に隠さない）。

    第10節の判定式そのものを `ledger show` の `subcontract_usage` と公開
    Version の policy から再現し、どの上限にも達していないことを確認する。
    """
    usage = harness.subcontract_usage(world, root_id, label="headroom")
    policy = _subcontract_policy(world, root_id)
    in_use = harness.parse_amount_string(usage["in_use"])
    deposit = harness.parse_amount_string(usage["deposit"])
    available = harness.parse_amount_string(usage["available"])
    ratio_cap = policy["max_ratio_bps"] * deposit // 10_000
    assert in_use + budget_units <= int(policy["max_amount_units"]), (usage, policy)
    assert in_use + budget_units <= ratio_cap, (usage, policy)
    assert budget_units <= available, usage
    assert usage["child_count"] + 1 <= int(policy["max_children"]), (usage, policy)


def _subcontract_policy(world: harness.E2EWorld, root_id: str) -> dict:
    result = world.run_cli(
        ["job", "get", root_id],
        actor=REQUESTER_ID,
        action="job get (policy)",
    )
    return result.data["version"]["subcontract_policy"]


def _catalog_task_keys(world: harness.E2EWorld, root_id: str) -> list[str]:
    result = world.run_cli(
        ["job", "get", root_id],
        actor=REQUESTER_ID,
        action="job get (catalog)",
    )
    return [entry["task_key"] for entry in result.data["version"]["task_catalog"]]


def _children_with_task_key(
    world: harness.E2EWorld, root_id: str, task_key: str
) -> int:
    with world.read_only_connection() as conn:
        return int(
            conn.execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE parent_id = ? AND task_key = ?",
                (root_id, task_key),
            ).fetchone()["n"]
        )


def _assert_no_requester_child_interaction(
    world: harness.E2EWorld, child_id: str
) -> None:
    """Root Requester が Child に対する操作を 1 件も行っていない（都度確認なし）。"""
    requester_writes = [
        op
        for op in world.report.operations
        if op.actor == REQUESTER_ID
        and op.channel in {"cli", "mcp"}
        and not op.action.startswith(("ledger show", "job get", "job list"))
    ]
    assert all(
        child_id not in op.action
        and not op.action.startswith(("child create", "job approve", "job dispute"))
        for op in requester_writes
    ), [op.action for op in requester_writes]
    with world.read_only_connection() as conn:
        rows = conn.execute(
            "SELECT actor_id, action FROM events WHERE job_id = ?", (child_id,)
        ).fetchall()
    assert REQUESTER_ID not in {row["actor_id"] for row in rows}, [
        dict(row) for row in rows
    ]


def _assert_invariants(world: harness.E2EWorld) -> None:
    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label
