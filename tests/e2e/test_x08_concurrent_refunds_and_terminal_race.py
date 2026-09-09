"""X08: 同時 Refund と Parent 終了・Child return の競合（計画書 第18節 追加検証 X08）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X08 | 同時Refund（初回90・追加10それぞれ）、Parent終了とChild returnの競合 | 返金累計100以下。どちらの順でも同じ最終結果 |

3 つの独立 fixture を同じ最終結果へ収束させる。どれも Root 入金 100・A の
part-1 への Child 発注 10・B の Claim から始め、失敗は提出前 abandon で作る:

- **X08A（Parent 終了 → Child return）**: Parent 失敗で `refund:{root}:terminal`
  の 90 が確定した直後に `job refund ROOT` を **2 プロセス同時**に実行し、
  さらに Child 失敗で追加された 10 についても同時実行する
  （「初回 90・追加 10 それぞれ」の同時 Refund）
- **X08B（Child return → Parent 終了）**: 先に Child を失敗させて 10 を
  available へ戻し、その後 Parent を失敗させる。予約が 1 件も無い時点の
  `job refund ROOT` は `INVALID_STATE`（第14節「新しい返金予約は作らない」）
- **X08C（競合）**: Parent 終了と Child return を barrier で**同時開始**する。
  どちらが先に commit しても、返金予約の合計は入金額 100 を超えない

各段階で「確定済み返金予約の合計 ≤ 入金 100」を検査し、最後に 3 fixture の
最終結果（Job 状態・受取人別 paid / refunded・Escrow・locked 内訳）が
完全一致することを照合する。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import harness, parallel_cli, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET = "10.000000"
CHILD_BUDGET_UNITS = 10_000_000
FIRST_REFUND_UNITS = 90_000_000
CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"

EXPECTED_REPORT_TAIL = {
    "paid": {},
    "refunded": {REQUESTER_ID: "100.000000"},
    "locked_breakdown": {
        "child_payout": "0.000000",
        "child_work": "0.000000",
        "parent_payout": "0.000000",
        "refund": "0.000000",
    },
}


def test_x08_concurrent_refunds_converge_in_both_orders(tmp_path: Path) -> None:
    """Parent 終了先行・Child return 先行・同時競合の 3 通りが同じ最終結果になる。"""
    outcomes: dict[str, dict[str, Any]] = {}
    for scenario_id, runner in (
        ("X08A", _run_parent_terminal_first),
        ("X08B", _run_child_return_first),
        ("X08C", _run_simultaneous_terminals),
    ):
        world = harness.create_world(tmp_path, scenario_id)
        failed = False
        try:
            outcomes[scenario_id] = runner(world)
        except BaseException:
            failed = True
            raise
        finally:
            report_path = world.report.write()
            assert report_path.exists()
        if not failed:
            report.assert_report_contents(
                report_path,
                scenario_id=scenario_id,
                terminal_job_states=outcomes[scenario_id].pop("job_states"),
                **EXPECTED_REPORT_TAIL,
            )

    # どちらの順でも（同時競合でも）同じ最終結果
    assert outcomes["X08A"] == outcomes["X08B"] == outcomes["X08C"], outcomes


# ---------------------------------------------------------------------------
# fixture ごとの進行
# ---------------------------------------------------------------------------


def _run_parent_terminal_first(world: harness.E2EWorld) -> dict[str, Any]:
    root_id, child_id, root_lease_id, child_lease_id = _start(world)

    # Parent 失敗 → refund:{root}:terminal の 90 が確定する
    _abandon(world, root_id, root_lease_id, actor=AGENT_A_ID, label="a-root")
    after_parent_fail = world.observe("after root abandon by A")
    assert after_parent_fail.job_states[root_id] == "FAILED"
    assert after_parent_fail.locked_for("refund") == FIRST_REFUND_UNITS
    assert after_parent_fail.locked_for("child_work") == CHILD_BUDGET_UNITS
    _assert_reserved_refund_within_deposit(world, after_parent_fail)

    # 初回 90 の同時 Refund
    _refund_simultaneously(world, root_id, label="first-90")
    after_first = world.observe("after concurrent refund (first 90)")
    assert after_first.refunded_to(REQUESTER_ID) == FIRST_REFUND_UNITS
    assert len(after_first.receipts) == 1
    assert after_first.locked_for("child_work") == CHILD_BUDGET_UNITS
    _assert_reserved_refund_within_deposit(world, after_first)

    # Child 失敗 → 追加返金 10
    _abandon(world, child_id, child_lease_id, actor=AGENT_B_ID, label="b-child")
    after_child_fail = world.observe("after child abandon by B")
    assert after_child_fail.job_states[child_id] == "FAILED"
    _assert_reserved_refund_within_deposit(world, after_child_fail)

    # 追加 10 の同時 Refund
    _refund_simultaneously(world, root_id, label="extra-10")
    world.report.note(
        "X08A: Parent 終了 → Child return の順で、初回 90 と追加 10 の"
        "それぞれについて 2 プロセス同時の job refund を実行しても返金は"
        "各 1 回だけ。返金累計は 100 で止まる"
    )
    # 順序が違えば返金予約の**構成**は変わる（ここは 90 + 10 の 2 件）が、
    # 最終結果（Job 状態・受取人別金額・Escrow・locked 内訳）は一致する
    return _finish(
        world,
        root_id,
        child_id,
        expected_refund_amounts=["10.000000", "90.000000"],
    )


def _run_child_return_first(world: harness.E2EWorld) -> dict[str, Any]:
    root_id, child_id, root_lease_id, child_lease_id = _start(world)

    # Child 失敗（Parent 生存中）→ child_work 10 は available へ戻る
    _abandon(world, child_id, child_lease_id, actor=AGENT_B_ID, label="b-child")
    after_child_fail = world.observe("after child abandon by B (parent alive)")
    assert after_child_fail.job_states[child_id] == "FAILED"
    assert after_child_fail.job_states[root_id] == "LEASED"
    assert after_child_fail.locked_units == 0
    assert after_child_fail.available_units == ROOT_BUDGET_UNITS
    assert after_child_fail.operations == []

    # 返金予約が 1 件も無い時点の refund は INVALID_STATE（予約を作らない）
    too_early = world.run_cli(
        ["job", "refund", root_id],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("refund", "r-root-early"),
        expect_ok=False,
        action="job refund (no reservation yet)",
    )
    assert too_early.returncode == 2, too_early.stdout
    assert too_early.error_code == "INVALID_STATE", too_early.payload
    after_early = world.observe("after premature refund attempt")
    assert after_early.operations == []
    assert after_early.receipts == []
    assert after_early.journal == after_child_fail.journal

    # Parent 失敗 → 未拘束 available の 100 がまとめて返金予約になる
    _abandon(world, root_id, root_lease_id, actor=AGENT_A_ID, label="a-root")
    after_parent_fail = world.observe("after root abandon by A")
    assert after_parent_fail.job_states[root_id] == "FAILED"
    assert after_parent_fail.locked_for("refund") == ROOT_BUDGET_UNITS
    _assert_reserved_refund_within_deposit(world, after_parent_fail)

    _refund_simultaneously(world, root_id, label="single-100")
    world.report.note(
        "X08B: Child return → Parent 終了の順では、予約が無い時点の"
        " job refund が INVALID_STATE となり、Parent 終了時に未拘束の 100 が"
        "まとめて返金予約になる。同時 Refund でも返金は 1 回だけ"
    )
    # Child return が先なら 10 は available へ戻っているため、返金予約は
    # 100 の 1 件になる（構成は違っても最終結果は X08A と一致する）
    return _finish(
        world, root_id, child_id, expected_refund_amounts=["100.000000"]
    )


def _run_simultaneous_terminals(world: harness.E2EWorld) -> dict[str, Any]:
    root_id, child_id, root_lease_id, child_lease_id = _start(world)

    # Parent 終了と Child return を barrier で同時開始する
    results = parallel_cli.run_simultaneously(
        world,
        [
            parallel_cli.Invocation(
                args=["job", "abandon", root_id, "--lease", root_lease_id],
                actor=AGENT_A_ID,
                operation_id=world.next_operation_id("abandon", "a-root"),
                action="job abandon (root, concurrent)",
            ),
            parallel_cli.Invocation(
                args=["job", "abandon", child_id, "--lease", child_lease_id],
                actor=AGENT_B_ID,
                operation_id=world.next_operation_id("abandon", "b-child"),
                action="job abandon (child, concurrent)",
            ),
        ],
        barrier_name="x08c-abandon",
    )
    for result in results:
        assert result.returncode == 0, (
            f"concurrent abandon failed: exit={result.returncode}"
            f" stdout={result.stdout} stderr={result.stderr}"
        )
        assert result.ok, result.payload

    after_race = world.observe("after concurrent parent/child abandon")
    assert after_race.job_states[root_id] == "FAILED"
    assert after_race.job_states[child_id] == "FAILED"
    # どちらが先に commit しても返金予約の合計は入金額を超えない
    _assert_reserved_refund_within_deposit(world, after_race)

    _refund_simultaneously(world, root_id, label="race")
    world.report.note(
        "X08C: Parent 終了と Child return を同時開始しても、どちらが先に"
        " commit しても返金予約の合計は入金 100 を超えず、同時 Refund の後の"
        "最終結果は他の順序と一致する"
    )
    # どちらが勝ったかは予約の構成から決まる。Root が先なら 90 + 10、
    # Child が先なら 100 の 1 件で、どちらでも最終結果は同じ
    return _finish(
        world,
        root_id,
        child_id,
        expected_refund_amounts=_race_refund_amounts(world, root_id, child_id),
    )


def _race_refund_amounts(
    world: harness.E2EWorld, root_id: str, child_id: str
) -> list[str]:
    """競合の勝者から、正当な返金予約の構成を一意に決める。

    `refund:{root}:terminal` が 90 なら Parent 終了が先に commit し、Child
    return の 10 が追加返金になっている。100 なら Child return が先に
    commit し、未拘束 available が 100 のまま 1 件で予約されている。
    """
    with world.read_only_connection() as conn:
        rows = conn.execute(
            "SELECT business_key, amount_units FROM payment_operations"
            " WHERE root_id = ? AND kind = 'refund'",
            (root_id,),
        ).fetchall()
    reserved = {str(row["business_key"]): int(row["amount_units"]) for row in rows}
    terminal_key = f"refund:{root_id}:terminal"
    child_return_key = f"refund:{root_id}:child-return:{child_id}"
    if reserved.get(terminal_key) == ROOT_BUDGET_UNITS:
        assert set(reserved) == {terminal_key}, reserved
        return ["100.000000"]
    assert reserved.get(terminal_key) == FIRST_REFUND_UNITS, reserved
    assert reserved.get(child_return_key) == CHILD_BUDGET_UNITS, reserved
    assert set(reserved) == {terminal_key, child_return_key}, reserved
    return ["10.000000", "90.000000"]


# ---------------------------------------------------------------------------
# 共通手順
# ---------------------------------------------------------------------------


def _start(world: harness.E2EWorld) -> tuple[str, str, str, str]:
    """Root 入金 100 → A が Root Claim・part-1 を 10 で発注 → B が Child Claim。"""
    root_id, root_version_id = harness.create_and_fund_root(world)
    world.observe("after root fund")

    claim_root = world.run_cli(
        ["job", "claim", root_id, "--version", root_version_id],
        actor=AGENT_A_ID,
        operation_id=world.next_operation_id("claim", "a-root"),
        action="job claim (root)",
    )
    root_lease_id = str(claim_root.data["lease_id"])

    created = world.run_cli(
        [
            "child",
            "create",
            root_id,
            "--lease",
            root_lease_id,
            "--task",
            "part-1",
            "--budget",
            CHILD_BUDGET,
            "--deadline",
            CHILD_DEADLINE,
        ],
        actor=AGENT_A_ID,
        operation_id=world.next_operation_id("create", "a-part-1"),
        action="child create (part-1)",
    )
    child_id = str(created.data["child_id"])
    child_version_id = str(created.data["version_id"])
    world.child_ids.append(child_id)

    claim_child = world.run_cli(
        ["job", "claim", child_id, "--version", child_version_id],
        actor=AGENT_B_ID,
        operation_id=world.next_operation_id("claim", "b-part-1"),
        action="job claim (part-1)",
    )
    child_lease_id = str(claim_child.data["lease_id"])

    start = world.observe("after start conditions")
    assert start.locked_for("child_work") == CHILD_BUDGET_UNITS
    assert start.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    return root_id, child_id, root_lease_id, child_lease_id


def _abandon(
    world: harness.E2EWorld,
    job_id: str,
    lease_id: str,
    *,
    actor: str,
    label: str,
) -> None:
    world.run_cli(
        ["job", "abandon", job_id, "--lease", lease_id],
        actor=actor,
        operation_id=world.next_operation_id("abandon", label),
        action=f"job abandon ({label})",
    )


def _refund_simultaneously(
    world: harness.E2EWorld, root_id: str, *, label: str
) -> None:
    """`job refund ROOT` を 2 プロセス同時に実行する（異なる operation_id）。"""
    results = parallel_cli.run_simultaneously(
        world,
        [
            parallel_cli.Invocation(
                args=["job", "refund", root_id],
                actor=REQUESTER_ID,
                operation_id=world.next_operation_id("refund", f"{label}-{n}"),
                action=f"job refund (concurrent {label} {n})",
            )
            for n in (1, 2)
        ],
        barrier_name=f"{world.scenario_id.lower()}-refund-{label}",
    )
    for result in results:
        assert result.returncode == 0, (
            f"concurrent refund failed: exit={result.returncode}"
            f" stdout={result.stdout} stderr={result.stderr}"
        )
        assert result.ok, result.payload
    world.observe(f"after concurrent refund ({label})")


def _finish(
    world: harness.E2EWorld,
    root_id: str,
    child_id: str,
    *,
    expected_refund_amounts: list[str],
) -> dict[str, Any]:
    """残った予約を独立 tick で確定させ、比較用に正規化した最終結果を返す。

    返金予約・Receipt の**構成**（90 + 10 か 100 の 1 件か）は Parent 終了と
    Child return の順序で変わるため、その順序依存の値はここで個別に照合し、
    fixture 間で比較する最終結果には含めない。
    """
    world.settle()
    final = world.observe("after final settlement")

    assert final.job_states[root_id] == "FAILED"
    assert final.job_states[child_id] == "FAILED"
    assert final.refunded_to(REQUESTER_ID) == ROOT_BUDGET_UNITS
    assert final.refunded_units == ROOT_BUDGET_UNITS
    assert final.paid_units == 0
    assert final.escrow_units == 0
    assert final.available_units == 0
    assert final.locked_units == 0
    _assert_reserved_refund_within_deposit(world, final)
    assert harness.check_payee_entitlement(final) == []

    # 順序依存の値（返金予約・Receipt の構成）は fixture ごとに個別照合する
    assert sorted(
        op["amount"] for op in final.operations if op["kind"] == "refund"
    ) == expected_refund_amounts
    assert sorted(
        harness.format_amount_units(int(receipt["amount_units"]))
        for receipt in final.receipts
        if receipt["kind"] == "refund"
    ) == expected_refund_amounts
    assert all(
        op["status"] == "SUCCEEDED"
        for op in final.operations
        if op["kind"] == "refund"
    )

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label

    return {
        "job_states": {root_id: "FAILED", child_id: "FAILED"},
        "root_state": final.job_states[root_id],
        "child_state": final.job_states[child_id],
        "paid_by_payee": {
            payee: harness.format_amount_units(units)
            for payee, units in sorted(final.paid_by_payee_units.items())
        },
        "refunded_by_payee": {
            payee: harness.format_amount_units(units)
            for payee, units in sorted(final.refunded_by_payee_units.items())
        },
        "totals": final.to_amount_strings(),
        "locked_breakdown": {
            bucket: harness.format_amount_units(units)
            for bucket, units in sorted(final.locked_breakdown_units.items())
        },
    }


def _assert_reserved_refund_within_deposit(
    world: harness.E2EWorld, snapshot: harness.Snapshot
) -> None:
    """確定済み返金予約の合計が入金額を超えない（返金累計 100 以下）。"""
    reserved = sum(
        harness.parse_amount_string(op["amount"])
        for op in snapshot.operations
        if op["kind"] == "refund"
    )
    assert reserved <= snapshot.deposit_units, (snapshot.label, reserved)
    assert snapshot.refunded_units <= snapshot.deposit_units, snapshot.label
    assert snapshot.refunded_units <= reserved, snapshot.label
