"""N14: 全 commit の会計不変条件（計画書 第18節 異常系 14 / N14）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 14 / N14 | E01〜E04、N01〜N13、追加シナリオの各commitを観測 | D=E+paid+refunded、口座非負、親子二重計上なし |

不変条件そのものは全シナリオの `world.observe()` が**各 commit 後に**検査して
いる（`harness.check_conservation` / `negative_accounts` /
`harness.check_payee_entitlement` / `harness.check_no_double_counting` /
`harness.check_commit_history`）。違反があればその場で `HarnessError` になる
ため、E01〜E04・N01〜N13 が pass している事実そのものが「各 commit で
成立している」ことの証拠になる。

観測点のスナップショットは外部操作や tick の**完了後**しか写せないが、
`scheduler.tick_once` は失効処理・自動承認・裁定・返金予約・送金 1 件ずつを
別々の DB commit で確定する。その中間 commit は
`harness.check_commit_history` が `journal_transactions` の commit 列を
再生して検査する。

このファイルはその機構が**形だけになっていない**ことを 4 点で確認する:

1. 代表的な進行（Root 入金 → Child 拘束 → Child 承認 → 送金 → Parent 承認
   → 送金）を実プロセスで通し、各 commit で 5 条件が成立する
2. チェッカーが**実際に破れを検出できる**（既知の破れを渡すと報告する）。
   とくに親子二重計上は、総額しか見ない保存則が成立したままでも検出される
   ことを示す
3. 中間 commit の破れが、完了後のスナップショットしか見ないチェッカーでは
   素通りする一方で `check_commit_history` では検出される
4. 各シナリオの終了時に通る `report.assert_report_contents` が 5 条件を
   **すべて要求している**（1 つでも落とすと失敗する）
"""

from __future__ import annotations

import dataclasses
import json
import tempfile
from pathlib import Path
from typing import Any

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "N14"

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000
PARENT_PAYOUT_UNITS = 90_000_000

# `report.assert_report_contents` へ渡す最小の健全レポートと、その期待値。
_SYNTHETIC_EXPECTED = {
    "scenario_id": "SYNTHETIC",
    "terminal_job_states": {"job:root": "DONE"},
    "paid": {AGENT_A_ID: "100.000000"},
    "refunded": {},
    "locked_breakdown": {
        "child_payout": "0.000000",
        "child_work": "0.000000",
        "parent_payout": "0.000000",
        "refund": "0.000000",
    },
}


def _synthetic_report() -> dict[str, Any]:
    """4 条件がすべて成立している最小のレポート（毎回新しい dict を返す）。"""
    return {
        "scenario_id": "SYNTHETIC",
        "operation_ids": ["create:synthetic-01"],
        "operations": [],
        "terminal_job_states": {"job:root": "DONE"},
        "amounts_by_payee": {
            "paid": {AGENT_A_ID: "100.000000"},
            "refunded": {},
        },
        "locked_breakdown": dict(_SYNTHETIC_EXPECTED["locked_breakdown"]),
        "final_totals": {},
        "conservation": [
            {
                "label": "final",
                "conservation_ok": True,
                "accounts_non_negative": True,
                "negative_accounts": [],
                "payee_entitlement_ok": True,
                "payee_entitlement_violations": [],
                "no_double_counting_ok": True,
                "double_counting_violations": [],
                "commit_count": 1,
                "commits_ok": True,
                "commit_violations": [],
            }
        ],
        "conservation_ok": True,
        "notes": [],
    }


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n14_invariants_hold_at_every_commit(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_n14(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


def test_n14_checkers_detect_violations() -> None:
    """チェッカーが素通りしないこと（壊した観測を違反として報告する）。

    `world.observe()` は成立時に何も言わないため、成立の記録だけでは
    「検査が実装されている」証拠にならない。ここでは既知の破れを作って
    各チェッカーが報告することを確認する。
    """
    base = _reference_snapshot()
    assert harness.check_conservation(base) == []
    assert harness.negative_accounts(base) == []
    assert harness.check_no_double_counting(base) == []

    # 1. D != E + paid + refunded（保存則）
    broken_conservation = dataclasses.replace(base, deposit_units=110_000_000)
    assert harness.check_conservation(broken_conservation)

    # 2. 口座が負
    negative_accounts_snapshot = dataclasses.replace(
        base,
        accounts=[
            {**account, "amount": "-1.000000"}
            if account["bucket"] == "child_work"
            else account
            for account in base.accounts
        ],
    )
    assert harness.negative_accounts(negative_accounts_snapshot)
    assert harness.check_conservation(negative_accounts_snapshot)

    # 3. 親子二重計上: 予算 10 の Child へ Root Escrow から 20 が拘束されて
    #    いる（allocate が二重に走った状態）。available を 80 に合わせてある
    #    ため、保存則も口座非負も成立したまま二重計上だけが起きる
    double_counted = _double_counted_snapshot(base)
    assert harness.check_conservation(double_counted) == [], (
        "この fixture は保存則が成立したまま二重計上だけを起こす"
    )
    violations = harness.check_no_double_counting(double_counted)
    assert violations, "親子二重計上が検出されていない"


def test_n14_commit_history_detects_violations_hidden_by_the_final_state() -> None:
    """中間 commit の破れは、完了後のスナップショットだけでは見えない。

    `observe()` は外部操作や tick の**完了後**しか観測できないが、
    `scheduler.tick_once` は失効処理・自動承認・裁定・返金予約・送金 1 件ずつを
    別々の commit で確定する。ここでは「途中の commit で保存則が破れ、最後の
    commit で辻褄が合う」Journal を作り、最終状態しか見ない既存チェッカーが
    素通りする一方で `check_commit_history` が中間 commit を報告することを
    確認する。
    """
    healthy = _commit_log_snapshot(
        [
            _commit("op:fund", "fund", [(ROOT, "available", ROOT_BUDGET_UNITS)]),
            _commit(
                "op:allocate",
                "allocate",
                [
                    (ROOT, "available", -CHILD_BUDGET_UNITS),
                    (CHILD, "child_work", CHILD_BUDGET_UNITS),
                ],
            ),
        ]
    )
    assert harness.check_commit_history(healthy) == []

    hidden = _commit_log_snapshot(
        [
            _commit("op:fund", "fund", [(ROOT, "available", ROOT_BUDGET_UNITS)]),
            # 拘束だけを先に commit し、available の引落しを同じ commit に
            # 含めない（この時点では E = 110 で D = 100 を超える）
            _commit(
                "op:allocate", "allocate", [(CHILD, "child_work", CHILD_BUDGET_UNITS)]
            ),
            # 最後の commit で辻褄が合うので、完了後の状態は健全に見える
            _commit(
                "op:late", "allocate", [(ROOT, "available", -CHILD_BUDGET_UNITS)]
            ),
        ]
    )
    assert harness.check_conservation(hidden) == [], (
        "この fixture は完了後の状態では保存則が成立している"
    )
    assert harness.negative_accounts(hidden) == []
    assert harness.check_no_double_counting(hidden) == []

    violations = harness.check_commit_history(hidden)
    assert violations, "中間 commit の保存則違反が検出されていない"
    assert any("commit #2" in violation for violation in violations), violations


def test_n14_every_scenario_report_enforces_the_invariants() -> None:
    """レポート検証の入口が 5 条件すべてを要求していることを確認する。

    各シナリオは終了時に `report.assert_report_contents` を通す。そこが
    `conservation_ok` / `accounts_non_negative` / `payee_entitlement_ok` /
    `no_double_counting_ok` / `commits_ok` のどれか 1 つでも落としていると、
    E01〜N13 の
    「各 commit を観測」は形だけになる。ここでは条件を 1 つずつ落とした
    レポートを渡し、入口が確実に失敗することを確かめる（ディスク上の
    既存レポートを読むと実行順に依存するため、合成レポートで検査する）。
    """
    flags = (
        "conservation_ok",
        "accounts_non_negative",
        "payee_entitlement_ok",
        "no_double_counting_ok",
        "commits_ok",
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir) / "healthy.json"
        base_path.write_text(
            json.dumps(_synthetic_report()), encoding="utf-8"
        )
        report.assert_report_contents(base_path, **_SYNTHETIC_EXPECTED)

        for flag in flags:
            data = _synthetic_report()
            data["conservation"][0][flag] = False
            broken_path = Path(tmpdir) / f"broken-{flag}.json"
            broken_path.write_text(json.dumps(data), encoding="utf-8")
            with pytest.raises(AssertionError):
                report.assert_report_contents(broken_path, **_SYNTHETIC_EXPECTED)


async def _run_n14(world: harness.E2EWorld) -> dict:
    """代表的な進行を実プロセスで通し、各 commit で 3 条件を確認する。"""
    root_id, _ = harness.create_and_fund_root(world)
    world.observe("after root fund")

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

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
            world.observe("after child submit by B")

            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": child_id,
                    "submission_id": str(submit_child.data["submission_id"]),
                    "operation_id": world.next_operation_id("approve", "a-child"),
                },
            )
            world.observe("after child approve by A")
            world.settle()

            submit_root = await agent_a.call(
                "ojp_submit",
                {
                    "job_id": root_id,
                    "lease_id": start.root_lease_id,
                    "version_id": start.root_version_id,
                    "artifact_json": json.dumps({"sum": 15}),
                    "operation_id": world.next_operation_id("submit", "a-root"),
                },
            )
            root_submission_id = str(submit_root.data["submission_id"])
            world.observe("after root submit by A")

    world.run_cli(
        ["job", "approve", root_id, "--submission", root_submission_id],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("approve", "r-root"),
        action="job approve (root)",
    )
    world.observe("after root approve by Requester")
    world.settle()
    final = world.observe("after settlement")

    # 全観測点で 3 条件が成立している（observe が破れで失敗するので、
    # ここまで到達している時点で成立しているが、明示的にも照合する）
    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.negative_accounts(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label
        assert harness.check_payee_entitlement(snap) == [], snap.label
        assert snap.conservation_ok_reported, snap.label

    assert final.paid_to(AGENT_A_ID) == PARENT_PAYOUT_UNITS
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.escrow_units == 0
    assert final.deposit_units == ROOT_BUDGET_UNITS

    world.report.note(
        "N14: 代表的な進行の全 commit で D=E+paid+refunded・口座非負・"
        "受取権者一致・親子二重計上なしが成立。チェッカー自体が破れを"
        "検出できることは test_n14_checkers_detect_violations が示す"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {root_id: "DONE", child_id: "DONE"},
        "paid": {AGENT_A_ID: "90.000000", AGENT_B_ID: "10.000000"},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


# ---------------------------------------------------------------------------
# チェッカー検証用の合成スナップショット
# ---------------------------------------------------------------------------


def _account(owner_job_id: str, bucket: str, amount: str) -> dict:
    return {
        "account_id": f"acct:{owner_job_id}:{bucket}",
        "owner_job_id": owner_job_id,
        "bucket": bucket,
        "purpose": None,
        "amount": amount,
        "beneficiary_id": None,
        "source_key": None,
    }


def _journal(owner_job_id: str, bucket: str, delta_units: int, reason: str) -> dict:
    return {
        "operation_id": f"op:{owner_job_id}:{bucket}:{reason}",
        "reason": reason,
        "created_at_us": 0,
        "entry_no": 0,
        "delta_units": delta_units,
        "bucket": bucket,
        "owner_job_id": owner_job_id,
        "beneficiary_id": None,
    }


def _commit(
    operation_id: str, reason: str, entries: list[tuple[str, str, int]]
) -> list[dict]:
    """1 つの DB commit（journal_transactions 1 行）にあたる Journal 明細。"""
    return [
        {
            "operation_id": operation_id,
            "reason": reason,
            "created_at_us": 0,
            "entry_no": entry_no,
            "delta_units": delta_units,
            "bucket": bucket,
            "owner_job_id": owner_job_id,
            "beneficiary_id": None,
        }
        for entry_no, (owner_job_id, bucket, delta_units) in enumerate(entries)
    ]


def _commit_log_snapshot(commits: list[list[dict]]) -> harness.Snapshot:
    """commit 列から観測スナップショットを導出する（完了後の状態だけを写す）。

    残高・D・locked 内訳は Journal を畳んで導出するので、`check_commit_history`
    の再生妥当性チェックが通る「実際にありうる観測」になる。
    """
    journal = [entry for commit in commits for entry in commit]
    balances: dict[tuple[str, str], int] = {}
    deposit_units = 0
    for entry in journal:
        key = (entry["owner_job_id"], entry["bucket"])
        balances[key] = balances.get(key, 0) + entry["delta_units"]
        if (
            entry["reason"] == "fund"
            and entry["bucket"] == "available"
            and entry["delta_units"] > 0
        ):
            deposit_units += entry["delta_units"]
    available_units = sum(
        units for (_owner, bucket), units in balances.items() if bucket == "available"
    )
    breakdown = {bucket: 0 for bucket in harness.LOCKED_BUCKETS}
    for (_owner, bucket), units in balances.items():
        if bucket in breakdown:
            breakdown[bucket] += units
    locked_units = sum(breakdown.values())
    return harness.Snapshot(
        label="commit log",
        root_id=ROOT,
        deposit_units=deposit_units,
        escrow_units=available_units + locked_units,
        available_units=available_units,
        locked_units=locked_units,
        locked_breakdown_units=breakdown,
        paid_units=0,
        refunded_units=0,
        accounts=[
            _account(owner, bucket, harness.format_amount_units(units))
            for (owner, bucket), units in sorted(balances.items())
        ],
        operations=[],
        conservation_ok_reported=True,
        job_states={ROOT: "LEASED", CHILD: "LEASED"},
        job_requesters={ROOT: REQUESTER_ID, CHILD: AGENT_A_ID},
        leases=[],
        submissions=[],
        acceptances=[],
        receipts=[],
        wallet_units={REQUESTER_ID: 0, AGENT_A_ID: 0, AGENT_B_ID: 0},
        journal=journal,
        events=[],
        submission_attempts=[],
        disputes=[],
        child_budget_units={ROOT: ROOT_BUDGET_UNITS, CHILD: CHILD_BUDGET_UNITS},
    )


ROOT = "job:root"
CHILD = "job:child"


def _reference_snapshot() -> harness.Snapshot:
    """Child 拘束 10・Root available 90 の健全な観測（送金はまだ無い）。"""
    return harness.Snapshot(
        label="reference",
        root_id=ROOT,
        deposit_units=ROOT_BUDGET_UNITS,
        escrow_units=ROOT_BUDGET_UNITS,
        available_units=PARENT_PAYOUT_UNITS,
        locked_units=CHILD_BUDGET_UNITS,
        locked_breakdown_units={
            "child_work": CHILD_BUDGET_UNITS,
            "child_payout": 0,
            "parent_payout": 0,
            "refund": 0,
        },
        paid_units=0,
        refunded_units=0,
        accounts=[
            _account(ROOT, "available", "90.000000"),
            _account(CHILD, "child_work", "10.000000"),
        ],
        operations=[],
        conservation_ok_reported=True,
        job_states={ROOT: "LEASED", CHILD: "LEASED"},
        job_requesters={ROOT: REQUESTER_ID, CHILD: AGENT_A_ID},
        leases=[],
        submissions=[],
        acceptances=[],
        receipts=[],
        wallet_units={REQUESTER_ID: 0, AGENT_A_ID: 0, AGENT_B_ID: 0},
        journal=[
            _journal(ROOT, "available", ROOT_BUDGET_UNITS, "fund"),
            _journal(ROOT, "available", -CHILD_BUDGET_UNITS, "allocate"),
            _journal(CHILD, "child_work", CHILD_BUDGET_UNITS, "allocate"),
        ],
        events=[],
        submission_attempts=[],
        disputes=[],
        child_budget_units={ROOT: ROOT_BUDGET_UNITS, CHILD: CHILD_BUDGET_UNITS},
    )


def _double_counted_snapshot(base: harness.Snapshot) -> harness.Snapshot:
    """同じ Child の予算 10 を Root Escrow から二重に拘束した観測を作る。

    `allocate` が同じ Child に対して 2 回走り、child_work へ 20 が流入した
    状態。available は 80 へ減り、locked は 20 になるため
    **D = E + paid + refunded も E = available + locked も口座非負も成立する**
    （総額しか見ない保存則では検出できない）。検出できるのは
    「Child ごとの原資の行き先」を公開 Version の予算と突き合わせる
    `check_no_double_counting` だけである。
    """
    return dataclasses.replace(
        base,
        label="double counted",
        available_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS * 2,
        locked_units=CHILD_BUDGET_UNITS * 2,
        locked_breakdown_units={
            "child_work": CHILD_BUDGET_UNITS * 2,
            "child_payout": 0,
            "parent_payout": 0,
            "refund": 0,
        },
        accounts=[
            _account(ROOT, "available", "80.000000"),
            _account(CHILD, "child_work", "20.000000"),
        ],
        journal=list(base.journal)
        + [
            # 同じ Child への 2 回目の拘束（予算 10 の Child に 20 が拘束される）
            _journal(ROOT, "available", -CHILD_BUDGET_UNITS, "allocate"),
            _journal(CHILD, "child_work", CHILD_BUDGET_UNITS, "allocate"),
        ],
    )
