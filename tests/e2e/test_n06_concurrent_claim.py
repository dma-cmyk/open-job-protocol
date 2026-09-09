"""N06: A と B が同じ Job へ同時 Claim（計画書 第18節 異常系 6 / N06）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 6 / N06 | AとBが同じJobへ同時Claim | 1人だけ成立。敗者のheartbeat/submitを拒否 |

barrier で同時開始した 2 つの独立 CLI プロセス（A と B）が同じ Root を
Claim する。直列化は DB の BEGIN IMMEDIATE と leases の UNIQUE index
（leases_one_active_per_job / UNIQUE(job_id, generation)）が担う
（共有メモリ Lock に頼らない。第16節）。

確認する内容:

- 成立するのは 1 人だけ。敗者は `CLAIM_CONFLICT`（exit=2）
- 有効 Lease は 1 件、Job は LEASED、Worker は勝者
- 敗者の heartbeat / submit を拒否する:
  - 未成立 Lease ID（自分の claim operation_id から導かれるはずだった ID）
    → INVALID_TARGET
  - 勝者の Lease ID → FORBIDDEN
- 拒否後も Job・Lease・Submission・残高は不変
- その後**勝者は同じ Lease から submit できる**

実プロセス構成: Claim・heartbeat・submit はすべて CLI 子プロセス
（同時開始のため）。時刻は固定 Clock のままで tick を進めない。
"""

from __future__ import annotations

from pathlib import Path

from . import harness, parallel_cli, report
from .harness import AGENT_A_ID, AGENT_B_ID

SCENARIO_ID = "N06"

ROOT_BUDGET_UNITS = 100_000_000


def test_n06_concurrent_claim_rejects_loser(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = _run_n06(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


def _run_n06(world: harness.E2EWorld) -> dict:
    root_id, root_version_id = harness.create_and_fund_root(world)
    before_claim = world.observe("after root fund")
    assert before_claim.job_states[root_id] == "OPEN"

    # ------------------------------------------------------------------
    # A と B が barrier で同時に同じ Root を Claim する
    # ------------------------------------------------------------------
    claim_operation_ids = {
        AGENT_A_ID: world.next_operation_id("claim", "a-root"),
        AGENT_B_ID: world.next_operation_id("claim", "b-root"),
    }
    claim_args = ["job", "claim", root_id, "--version", root_version_id]
    results = parallel_cli.run_simultaneously(
        world,
        [
            parallel_cli.Invocation(
                args=claim_args,
                actor=actor,
                operation_id=claim_operation_ids[actor],
                action=f"job claim (concurrent {actor})",
            )
            for actor in (AGENT_A_ID, AGENT_B_ID)
        ],
        barrier_name="n06-claim",
    )
    outcomes = dict(zip((AGENT_A_ID, AGENT_B_ID), results))

    winners = [actor for actor, result in outcomes.items() if result.ok]
    losers = [actor for actor, result in outcomes.items() if not result.ok]
    assert len(winners) == 1, {
        actor: result.payload for actor, result in outcomes.items()
    }
    winner, loser = winners[0], losers[0]

    loser_result = outcomes[loser]
    assert loser_result.returncode == 2, loser_result.stdout
    assert loser_result.error_code == "CLAIM_CONFLICT", loser_result.payload

    winner_lease_id = str(outcomes[winner].data["lease_id"])
    winner_generation = int(outcomes[winner].data["generation"])

    after_claim = world.observe("after concurrent claim")
    assert after_claim.job_states[root_id] == "LEASED"
    assert after_claim.active_lease_worker(root_id) == winner
    root_leases = [lease for lease in after_claim.leases if lease["job_id"] == root_id]
    assert len(root_leases) == 1
    assert root_leases[0]["id"] == winner_lease_id
    assert root_leases[0]["worker_id"] == winner
    assert after_claim.available_units == ROOT_BUDGET_UNITS

    # ------------------------------------------------------------------
    # 敗者の heartbeat / submit を拒否する
    # ------------------------------------------------------------------
    # 敗者の claim が成立していれば得られたはずの Lease ID（未成立なので存在しない）
    unrealized_lease_id = f"lease:{claim_operation_ids[loser]}"
    with world.read_only_connection() as conn:
        assert (
            conn.execute(
                "SELECT 1 FROM leases WHERE id = ?", (unrealized_lease_id,)
            ).fetchone()
            is None
        )

    artifact_path = harness.write_artifact(world, "n06-root", {"sum": 15})
    rejections = [
        (
            ["job", "heartbeat", root_id, "--lease", unrealized_lease_id],
            "INVALID_TARGET",
            "loser-heartbeat-unrealized",
        ),
        (
            [
                "job",
                "heartbeat",
                root_id,
                "--lease",
                winner_lease_id,
                "--generation",
                str(winner_generation),
            ],
            "FORBIDDEN",
            "loser-heartbeat-winner-lease",
        ),
        (
            [
                "job",
                "submit",
                root_id,
                "--lease",
                unrealized_lease_id,
                "--version",
                root_version_id,
                "--artifact",
                str(artifact_path),
            ],
            "INVALID_TARGET",
            "loser-submit-unrealized",
        ),
        (
            [
                "job",
                "submit",
                root_id,
                "--lease",
                winner_lease_id,
                "--version",
                root_version_id,
                "--artifact",
                str(artifact_path),
            ],
            "FORBIDDEN",
            "loser-submit-winner-lease",
        ),
    ]
    for args, expected_code, label in rejections:
        result = world.run_cli(
            args,
            actor=loser,
            operation_id=world.next_operation_id(args[1], label),
            expect_ok=False,
            action=f"{' '.join(args[:2])} ({label})",
        )
        described = f"{loser}: ojp {' '.join(args)}"
        assert result.returncode == 2, f"{described} -> exit={result.returncode}"
        assert result.error_code == expected_code, (
            f"{described} -> {result.error_code} (expected {expected_code});"
            f" stdout={result.stdout}"
        )

    after_rejections = world.observe("after loser heartbeat/submit rejections")
    assert after_rejections.job_states == after_claim.job_states
    assert after_rejections.leases == after_claim.leases
    assert after_rejections.submissions == []
    assert after_rejections.accounts == after_claim.accounts
    assert after_rejections.journal == after_claim.journal
    assert after_rejections.operations == []
    assert after_rejections.receipts == []
    assert after_rejections.wallet_units == after_claim.wallet_units

    # ------------------------------------------------------------------
    # 勝者は同じ Lease から submit できる
    # ------------------------------------------------------------------
    submit = world.run_cli(
        [
            "job",
            "submit",
            root_id,
            "--lease",
            winner_lease_id,
            "--version",
            root_version_id,
            "--artifact",
            str(artifact_path),
        ],
        actor=winner,
        operation_id=world.next_operation_id("submit", "winner-root"),
        action="job submit (winner)",
    )
    assert submit.data["state"] == "SUBMITTED"
    final = world.observe("after winner submit")
    assert final.job_states[root_id] == "SUBMITTED"
    assert final.submission_worker(root_id) == winner
    assert len(final.submissions) == 1
    assert final.paid_units == 0
    assert final.refunded_units == 0

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label

    world.report.note(
        f"N06: A と B の同時 Claim で {winner} だけが成立し、{loser} は"
        " CLAIM_CONFLICT。敗者の heartbeat / submit は未成立 Lease で"
        " INVALID_TARGET、勝者 Lease で FORBIDDEN。勝者はその後 submit できる"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {root_id: "SUBMITTED"},
        "paid": {},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }
