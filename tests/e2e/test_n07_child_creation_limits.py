"""N07: Child 作成上限の同時競合（計画書 第18節 異常系 7 / N07・
「N07の独立した上限競合fixture」）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 7 / N07 | 新規Rootでmax_amount=30、max_ratio_bps=10000、max_children=3。異なるIDでpart-1/part-2を各20で同時作成 | 1件だけ成立、敗者はPOLICY_LIMIT/MAX_AMOUNT。U=20、available=80 |

3 ケースそれぞれが新しい Root・入金 100・有効な Parent Lease を持つ独立
fixture で、part-1 と part-2 を**異なる operation_id で同時作成**する
（barrier で同時開始した 2 つの独立 CLI プロセス）。カタログは第11節のまま
で、policy だけを Root 公開前に設定する。

| ケース | policy | 予算 | 敗者の理由 | 確定残高 |
|---|---|---|---|---|
| 金額 | max_amount=30 / max_ratio_bps=10000 / max_children=3 | 各 20 | POLICY_LIMIT/MAX_AMOUNT | U=20 / available=80 |
| 比率 | max_amount=100 / max_ratio_bps=3000 / max_children=3 | 各 20 | POLICY_LIMIT/MAX_RATIO | U=20 / available=80 |
| 件数 | max_amount=100 / max_ratio_bps=10000 / max_children=1 | 各 10 | POLICY_LIMIT/MAX_CHILDREN | 作成数 1 / U=10 / available=90 |

通信成功だけでなく、拒否理由（`details.reason`）と確定残高まで照合する。
**重複 task_key 拒否では代替しない**（敗者が TASK_CONFLICT を返していない
ことを明示的に assert する）。拒否された作成は Job も task_key も確保しない
ため、敗者側の task_key の Job 行が存在しないことも確認する。
"""

from __future__ import annotations

from pathlib import Path

from . import harness, parallel_cli, report
from .harness import AGENT_A_ID

ROOT_BUDGET_UNITS = 100_000_000
CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"


def test_n07_concurrent_create_child_max_amount(tmp_path: Path) -> None:
    """金額ケース: max_amount=30 で各 20 → 敗者 MAX_AMOUNT、U=20 / available=80。"""
    _run_case(
        tmp_path,
        scenario_id="N07A",
        policy={
            "enabled": True,
            "max_amount": "30.000000",
            "max_ratio_bps": 10000,
            "max_children": 3,
            "max_depth": 1,
        },
        budget="20.000000",
        expected_reason="MAX_AMOUNT",
        expected_in_use_units=20_000_000,
    )


def test_n07_concurrent_create_child_max_ratio(tmp_path: Path) -> None:
    """比率ケース: max_ratio_bps=3000 で各 20 → 敗者 MAX_RATIO、U=20 / available=80。"""
    _run_case(
        tmp_path,
        scenario_id="N07B",
        policy={
            "enabled": True,
            "max_amount": "100.000000",
            "max_ratio_bps": 3000,
            "max_children": 3,
            "max_depth": 1,
        },
        budget="20.000000",
        expected_reason="MAX_RATIO",
        expected_in_use_units=20_000_000,
    )


def test_n07_concurrent_create_child_max_children(tmp_path: Path) -> None:
    """件数ケース: max_children=1 で各 10 → 敗者 MAX_CHILDREN、U=10 / available=90。"""
    _run_case(
        tmp_path,
        scenario_id="N07C",
        policy={
            "enabled": True,
            "max_amount": "100.000000",
            "max_ratio_bps": 10000,
            "max_children": 1,
            "max_depth": 1,
        },
        budget="10.000000",
        expected_reason="MAX_CHILDREN",
        expected_in_use_units=10_000_000,
    )


def _run_case(
    tmp_path: Path,
    *,
    scenario_id: str,
    policy: dict,
    budget: str,
    expected_reason: str,
    expected_in_use_units: int,
) -> None:
    world = harness.create_world(tmp_path, scenario_id)
    failed = False
    try:
        expected = _run_n07(
            world,
            policy=policy,
            budget=budget,
            expected_reason=expected_reason,
            expected_in_use_units=expected_in_use_units,
        )
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


def _run_n07(
    world: harness.E2EWorld,
    *,
    policy: dict,
    budget: str,
    expected_reason: str,
    expected_in_use_units: int,
) -> dict:
    # policy だけを Root 公開前に設定する（カタログは第11節のまま）
    card_path = harness.write_root_card(
        world, world.scenario_id.lower(), subcontract_policy=policy
    )
    root_id, root_version_id = harness.create_and_fund_root(
        world, card_path=card_path
    )
    world.observe("after root fund")

    # 有効な Parent Lease（A が CLI で Root を Claim）
    claim = world.run_cli(
        ["job", "claim", root_id, "--version", root_version_id],
        actor=AGENT_A_ID,
        operation_id=world.next_operation_id("claim", "a-root"),
        action="job claim (root)",
    )
    parent_lease_id = str(claim.data["lease_id"])
    before = world.observe("after root claim by A")
    assert before.available_units == ROOT_BUDGET_UNITS
    assert before.locked_units == 0

    # ------------------------------------------------------------------
    # part-1 / part-2 を異なる operation_id で同時作成する
    # ------------------------------------------------------------------
    task_keys = ("part-1", "part-2")
    results = parallel_cli.run_simultaneously(
        world,
        [
            parallel_cli.Invocation(
                args=[
                    "child",
                    "create",
                    root_id,
                    "--lease",
                    parent_lease_id,
                    "--task",
                    task_key,
                    "--budget",
                    budget,
                    "--deadline",
                    CHILD_DEADLINE,
                ],
                actor=AGENT_A_ID,
                operation_id=world.next_operation_id("create", f"a-{task_key}"),
                action=f"child create ({task_key})",
            )
            for task_key in task_keys
        ],
        barrier_name=f"{world.scenario_id.lower()}-create-child",
    )
    outcomes = dict(zip(task_keys, results))

    winners = [key for key, result in outcomes.items() if result.ok]
    losers = [key for key, result in outcomes.items() if not result.ok]
    assert len(winners) == 1, {key: r.payload for key, r in outcomes.items()}
    winner_task, loser_task = winners[0], losers[0]

    # 敗者は POLICY_LIMIT/<reason>。重複 task_key 拒否では代替しない
    loser_result = outcomes[loser_task]
    assert loser_result.returncode == 2, loser_result.stdout
    assert loser_result.error_code == "POLICY_LIMIT", loser_result.payload
    assert loser_result.error_code != "TASK_CONFLICT"
    details = loser_result.payload["error"]["details"]
    assert details is not None and details["reason"] == expected_reason, details

    child_id = str(outcomes[winner_task].data["child_id"])
    world.child_ids.append(child_id)

    after = world.observe("after concurrent child create")

    # 成立したのは 1 件だけ。拒否された作成は Job も task_key も確保しない
    child_ids = [job_id for job_id in after.job_states if job_id != root_id]
    assert child_ids == [child_id]
    with world.read_only_connection() as conn:
        loser_jobs = conn.execute(
            "SELECT id FROM jobs WHERE parent_id = ? AND task_key = ?",
            (root_id, loser_task),
        ).fetchall()
    assert loser_jobs == []

    # 確定残高: U（child_work + child_payout + child paid）と available
    assert after.locked_for("child_work") == expected_in_use_units
    assert after.locked_for("child_payout") == 0
    assert after.locked_units == expected_in_use_units
    assert after.available_units == ROOT_BUDGET_UNITS - expected_in_use_units
    assert after.child_budget_units[child_id] == expected_in_use_units
    subcontract = _subcontract_usage(world, root_id)
    assert harness.parse_amount_string(subcontract["in_use"]) == expected_in_use_units
    assert harness.parse_amount_string(subcontract["available"]) == (
        ROOT_BUDGET_UNITS - expected_in_use_units
    )
    assert subcontract["child_count"] == 1

    # 資金は移動していない（paid / refunded / Receipt なし）
    assert after.paid_units == 0
    assert after.refunded_units == 0
    assert after.receipts == []
    assert after.operations == []
    assert harness.check_conservation(after) == []

    world.report.note(
        f"N07（{expected_reason}）: part-1 / part-2 の同時作成で 1 件だけ成立し、"
        f"敗者は POLICY_LIMIT/{expected_reason}。"
        f"U={harness.format_amount_units(expected_in_use_units)} /"
        f" available={harness.format_amount_units(ROOT_BUDGET_UNITS - expected_in_use_units)}"
    )
    return {
        "scenario_id": world.scenario_id,
        "terminal_job_states": {root_id: "LEASED", child_id: "OPEN"},
        "paid": {},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": harness.format_amount_units(expected_in_use_units),
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


def _subcontract_usage(world: harness.E2EWorld, root_id: str) -> dict:
    """`ojp ledger show ROOT --json` の subcontract_usage（U と available の正本）。"""
    result = world.run_cli(
        ["ledger", "show", root_id],
        actor=harness.REQUESTER_ID,
        action="ledger show (subcontract usage)",
    )
    return result.data["subcontract_usage"]
