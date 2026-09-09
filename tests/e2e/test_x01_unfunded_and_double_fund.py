"""X01: 未入金 Root の Claim・二重 fund・金額不足入金（計画書 第18節 追加検証 X01）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X01 | 未入金RootをClaim／二重fund／金額不足入金 | 未入金OPENなし。入金・引落しは1回だけ |

Root を作成しただけ（DRAFT・未入金）の状態から実プロセスで次を観測する:

1. **未入金 OPEN なし**: `job list --state OPEN` に 1 件も現れず、Job 状態は
   DRAFT のまま。A / B の Claim は `INVALID_STATE`（第7節「未入金 Root は
   OPEN にならないので DRAFT の Claim はここで拒否される」）
2. **金額不足入金**: 公開予算 100 に対する 50 の入金は `FORBIDDEN`
   （公開予算は `job_versions.budget_units` が正本）。DRAFT のまま資金は
   一切動かない
3. **正確な全額入金**: 100 の入金で DRAFT → OPEN。Requester Wallet は
   ちょうど 100 減り、`reason='fund'` の Journal は 1 件だけ
4. **二重 fund**: 同一 operation_id の再送は保存済み結果の replay、別
   operation_id での再入金（同額・別額とも）は `business_key=fund:{root_id}`
   で拒否される。どの経路でも入金・引落しは 1 回だけ
5. 入金後は Claim が成立する（拒否理由が未入金だったことの対照）

拒否の前後で Job 状態・Lease・口座残高・Journal・Wallet・PaymentOperation・
Receipt を比較し、変化を認めない（拒否の Operation 結果と監査記録だけは
追加を認める）。
"""

from __future__ import annotations

from pathlib import Path

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "X01"

ROOT_BUDGET_UNITS = 100_000_000
SHORT_AMOUNT = "50.000000"
FULL_AMOUNT = "100.000000"


def test_x01_unfunded_root_is_not_claimable_and_funds_exactly_once(
    tmp_path: Path,
) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = _run_x01(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


def _run_x01(world: harness.E2EWorld) -> dict:
    # ------------------------------------------------------------------
    # 1. Root を作成するだけ（未入金）
    # ------------------------------------------------------------------
    root_id, version_id = harness.create_root_job(world)
    drafted = world.observe("after root create (unfunded)")
    assert drafted.job_states[root_id] == "DRAFT"
    assert drafted.deposit_units == 0
    assert drafted.escrow_units == 0
    assert drafted.available_units == 0
    assert drafted.locked_units == 0
    assert drafted.journal == []
    requester_wallet_baseline = drafted.wallet_units[REQUESTER_ID]

    # 未入金 Root は OPEN として公開されない
    _assert_no_open_jobs(world, "unfunded")

    # ------------------------------------------------------------------
    # 2. 未入金 Root への Claim は A / B とも INVALID_STATE
    # ------------------------------------------------------------------
    for actor, label in ((AGENT_A_ID, "a-root"), (AGENT_B_ID, "b-root")):
        rejected = world.run_cli(
            ["job", "claim", root_id, "--version", version_id],
            actor=actor,
            operation_id=world.next_operation_id("claim", f"unfunded-{label}"),
            expect_ok=False,
            action=f"job claim (unfunded, {label})",
        )
        assert rejected.returncode == 2, rejected.stdout
        assert rejected.error_code == "INVALID_STATE", rejected.payload
    after_claim_attempts = world.observe("after claim attempts on unfunded root")
    _assert_unchanged(drafted, after_claim_attempts)
    assert after_claim_attempts.leases == []

    # ------------------------------------------------------------------
    # 3. 金額不足入金（公開予算 100 に対する 50）は FORBIDDEN
    # ------------------------------------------------------------------
    short = harness.fund_root(
        world,
        root_id,
        amount=SHORT_AMOUNT,
        label="root-short",
        expect_ok=False,
    )
    assert short.returncode == 2, short.stdout
    assert short.error_code == "FORBIDDEN", short.payload
    after_short = world.observe("after short funding attempt")
    _assert_unchanged(drafted, after_short)
    assert after_short.job_states[root_id] == "DRAFT"
    _assert_no_open_jobs(world, "after-short-funding")

    # ------------------------------------------------------------------
    # 4. 正確な全額入金で DRAFT → OPEN。引落しはちょうど 1 回
    # ------------------------------------------------------------------
    fund_operation_id = world.next_operation_id("fund", "root")
    first = world.run_cli(
        ["job", "fund", root_id, "--amount", FULL_AMOUNT],
        actor=REQUESTER_ID,
        operation_id=fund_operation_id,
        action="job fund (first)",
    )
    funded = world.observe("after root fund")
    assert funded.job_states[root_id] == "OPEN"
    assert funded.deposit_units == ROOT_BUDGET_UNITS
    assert funded.available_units == ROOT_BUDGET_UNITS
    assert funded.locked_units == 0
    assert funded.paid_units == 0
    assert funded.refunded_units == 0
    assert (
        funded.wallet_units[REQUESTER_ID]
        == requester_wallet_baseline - ROOT_BUDGET_UNITS
    )
    assert funded.journal_total("available", reason="fund") == ROOT_BUDGET_UNITS
    assert _fund_effect_counts(world, root_id) == (1, 1)

    # ------------------------------------------------------------------
    # 5. 二重 fund: 同一 operation_id の再送は replay で効果を増やさない
    # ------------------------------------------------------------------
    replay = world.run_cli(
        ["job", "fund", root_id, "--amount", FULL_AMOUNT],
        actor=REQUESTER_ID,
        operation_id=fund_operation_id,
        action="job fund (same operation_id replay)",
    )
    assert replay.payload is not None and replay.payload["replayed"] is True
    assert replay.data == first.data
    after_replay = world.observe("after fund replay (same operation_id)")
    _assert_unchanged(funded, after_replay)

    # 別 operation_id での再入金は同額・別額ともに拒否される
    for amount, label in ((FULL_AMOUNT, "root-again"), (SHORT_AMOUNT, "root-again-short")):
        duplicate = harness.fund_root(
            world,
            root_id,
            amount=amount,
            label=label,
            expect_ok=False,
        )
        assert duplicate.returncode == 2, duplicate.stdout
        assert duplicate.error_code == "INVALID_STATE", duplicate.payload
    after_duplicates = world.observe("after duplicate fund attempts")
    _assert_unchanged(funded, after_duplicates)

    # 入金・引落しは 1 回だけ（fund の業務効果と Journal transaction が各 1 件）
    assert _fund_effect_counts(world, root_id) == (1, 1)
    assert after_duplicates.deposit_units == ROOT_BUDGET_UNITS
    assert (
        after_duplicates.wallet_units[REQUESTER_ID]
        == requester_wallet_baseline - ROOT_BUDGET_UNITS
    )
    assert after_duplicates.journal_total("available", reason="fund") == (
        ROOT_BUDGET_UNITS
    )

    # ------------------------------------------------------------------
    # 6. 入金後は Claim が成立する（拒否理由が未入金だったことの対照）
    # ------------------------------------------------------------------
    claim = world.run_cli(
        ["job", "claim", root_id, "--version", version_id],
        actor=AGENT_A_ID,
        operation_id=world.next_operation_id("claim", "a-root"),
        action="job claim (funded root)",
    )
    final = world.observe("after claim on funded root")
    assert final.job_states[root_id] == "LEASED"
    assert final.active_lease_worker(root_id) == AGENT_A_ID
    assert str(claim.data["lease_id"]) in {lease["id"] for lease in final.leases}
    assert final.deposit_units == ROOT_BUDGET_UNITS
    assert final.available_units == ROOT_BUDGET_UNITS
    assert final.paid_units == 0
    assert final.refunded_units == 0
    assert final.receipts == []
    assert final.operations == []

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label

    world.report.note(
        "X01: 未入金 Root は OPEN 一覧に現れず Claim は INVALID_STATE。"
        "金額不足入金は FORBIDDEN。正確な全額入金だけが DRAFT→OPEN を起こし、"
        "同一 operation_id の再送は replay、別 operation_id の再入金は"
        " business_key fund:{root} で拒否されるため、入金・引落しは 1 回だけ"
    )
    return {
        "scenario_id": SCENARIO_ID,
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


def _assert_no_open_jobs(world: harness.E2EWorld, label: str) -> None:
    """`job list --state OPEN` に 1 件も現れない（未入金 OPEN なし）。"""
    listed = world.run_cli(
        ["job", "list", "--state", "OPEN"],
        actor=REQUESTER_ID,
        action=f"job list --state OPEN ({label})",
    )
    assert listed.data["jobs"] == [], listed.data


def _fund_effect_counts(world: harness.E2EWorld, root_id: str) -> tuple[int, int]:
    """(business_key=fund:{root} の Operation 件数, reason='fund' の Journal 件数)。"""
    with world.read_only_connection() as conn:
        operations = conn.execute(
            "SELECT COUNT(*) AS n FROM operations WHERE business_key = ?",
            (f"fund:{root_id}",),
        ).fetchone()["n"]
        transactions = conn.execute(
            "SELECT COUNT(*) AS n FROM journal_transactions WHERE reason = 'fund'"
        ).fetchone()["n"]
    return int(operations), int(transactions)


def _assert_unchanged(
    before: harness.Snapshot, after: harness.Snapshot
) -> None:
    """Job 状態・Lease・口座・Journal・Wallet・PaymentOperation・Receipt が不変。

    拒否の Operation 結果と監査記録（events）だけは追加を認めるため比較しない。
    """
    assert after.job_states == before.job_states
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
