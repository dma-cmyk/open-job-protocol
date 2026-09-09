"""X15: 別Root・別Lease・非検収者からの操作（計画書 第18節 追加検証 X15）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X15 | 別Root・別Lease・非検収者からの操作 | 権限拒否しJob・資金・成果物公開範囲が不変 |

第5節「サービス経由の別 Actor・別 Job への不正操作は必ず拒否する」、第12節
「Requester が approve できるのは、自分が検収者である SUBMITTED の有効
Submission のみ」、第14節「refund は Root Requester または system」を実プロセスで
観測する。

同じ DB に 2 つの Root を作り、Lease と役割を交差させる:

- **Root1**: Requester R / Worker A。A が part-1 の Child1（予算 10）を発注し、
  B が Claim して有効提出する（Child1 の Requester は A、Worker は B）
- **Root2**: Requester R / Worker B。B が part-1 の Child2（予算 10）を発注し、
  A が Claim して有効提出する（Child2 の Requester は B、Worker は A）

この構成で、別 Root の Lease・別 Job の Lease・検収権のない Actor からの操作を
一通り試す。

| 操作 | 期待コード |
|---|---|
| A: `child create ROOT1 --lease ROOT2_LEASE` | INVALID_TARGET |
| B: `child create ROOT2 --lease ROOT1_LEASE` | INVALID_TARGET |
| A: `job heartbeat ROOT1 --lease ROOT2_LEASE` | INVALID_TARGET |
| B: `job abandon ROOT1 --lease ROOT2_LEASE` | INVALID_TARGET |
| A: `job submit CHILD2 --lease ROOT1_LEASE` | INVALID_TARGET |
| B: `job approve CHILD1` | FORBIDDEN（Worker は検収者でない） |
| R: `job approve CHILD1` | FORBIDDEN（Root Requester は Child の検収者でない） |
| R: `job approve CHILD2` | FORBIDDEN |
| A: `job approve CHILD2` | FORBIDDEN |
| B: `job dispute CHILD1` | FORBIDDEN（異議は Job Requester か Root Requester） |
| A: `ledger show ROOT1` | FORBIDDEN（会計ビューは Root Requester か system） |
| A: `job refund ROOT1` | FORBIDDEN |
| B: `job refund ROOT2` | FORBIDDEN |
| R: `job refund CHILD1` | INVALID_TARGET（返金対象は Root のみ） |

全拒否操作の CLI 終了コードは 2。拒否の前後で 2 つの Root の Job 状態・
Submission・Acceptance・Lease・口座残高・Journal・PaymentOperation・Receipt・
Wallet がいずれも不変で、**成果物の公開範囲**（`job get` が誰に本文・hash・
検証証跡を返すか）も変わらない。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "X15"

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET = "10.000000"
CHILD_BUDGET_UNITS = 10_000_000
CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"
OUTSIDER_ID = "pt-outsider"
ARTIFACT_SECRET_KEYS = ("artifact_json", "artifact_hash", "verification_evidence")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x15_cross_root_lease_and_role_operations_are_rejected(
    tmp_path: Path,
) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_x15(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_x15(world: harness.E2EWorld) -> dict:
    # Root2 を先に作り、最後に Root1 を作ることで world.root_id を Root1 にする
    root2_id, root2_version_id = harness.create_and_fund_root(world)
    root1_id, root1_version_id = harness.create_and_fund_root(world)
    assert world.root_id == root1_id

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            # -- Root1: Worker A、Child1 の Worker は B --------------------
            root1_lease_id = await _claim(
                world, agent_a, root1_id, root1_version_id, "a-root1"
            )
            child1_id, child1_version_id = await _create_child(
                world, agent_a, root1_id, root1_lease_id, "a-child1"
            )
            child1_lease_id = await _claim(
                world, agent_b, child1_id, child1_version_id, "b-child1"
            )
            submission1_id = await _submit(
                world, agent_b, child1_id, child1_lease_id, child1_version_id, 6
            )

            # -- Root2: Worker B、Child2 の Worker は A --------------------
            root2_lease_id = await _claim(
                world, agent_b, root2_id, root2_version_id, "b-root2"
            )
            child2_id, child2_version_id = await _create_child(
                world, agent_b, root2_id, root2_lease_id, "b-child2"
            )
            child2_lease_id = await _claim(
                world, agent_a, child2_id, child2_version_id, "a-child2"
            )
            submission2_id = await _submit(
                world, agent_a, child2_id, child2_lease_id, child2_version_id, 6
            )

    before_root1 = world.observe("before X15 rejections (root1)")
    with world.observing_root(root2_id):
        before_root2 = world.observe("before X15 rejections (root2)")

    # 前提条件（役割が交差していること）
    assert before_root1.job_states[child1_id] == "SUBMITTED"
    assert before_root1.job_requesters[child1_id] == AGENT_A_ID
    assert before_root1.submission_worker(child1_id) == AGENT_B_ID
    assert before_root2.job_states[child2_id] == "SUBMITTED"
    assert before_root2.job_requesters[child2_id] == AGENT_B_ID
    assert before_root2.submission_worker(child2_id) == AGENT_A_ID

    scope_before = _artifact_scope(world, [child1_id, child2_id])

    # ------------------------------------------------------------------
    # 別Root・別Lease・非検収者からの操作
    # ------------------------------------------------------------------
    # CLI の `job submit` は `--artifact` にファイルパスを要求する（第14節）。
    # 別 Lease による拒否を argparse エラーで代替しないため、実体を用意する。
    artifact_path = harness.write_artifact(world, "x15-child2", {"sum": 6})

    rejections: list[tuple[str, list[str], str, str]] = [
        # 別 Lease（他 Job の有効 Lease を使う）
        (
            AGENT_A_ID,
            [
                "child",
                "create",
                root1_id,
                "--lease",
                root2_lease_id,
                "--task",
                "part-2",
                "--budget",
                CHILD_BUDGET,
                "--deadline",
                CHILD_DEADLINE,
            ],
            "INVALID_TARGET",
            "a-create-root1-with-root2-lease",
        ),
        (
            AGENT_B_ID,
            [
                "child",
                "create",
                root2_id,
                "--lease",
                root1_lease_id,
                "--task",
                "part-2",
                "--budget",
                CHILD_BUDGET,
                "--deadline",
                CHILD_DEADLINE,
            ],
            "INVALID_TARGET",
            "b-create-root2-with-root1-lease",
        ),
        (
            AGENT_A_ID,
            ["job", "heartbeat", root1_id, "--lease", root2_lease_id],
            "INVALID_TARGET",
            "a-heartbeat-root1-with-root2-lease",
        ),
        (
            AGENT_B_ID,
            ["job", "abandon", root1_id, "--lease", root2_lease_id],
            "INVALID_TARGET",
            "b-abandon-root1-with-root2-lease",
        ),
        (
            AGENT_A_ID,
            [
                "job",
                "submit",
                child2_id,
                "--lease",
                root1_lease_id,
                "--version",
                child2_version_id,
                "--artifact",
                str(artifact_path),
            ],
            "INVALID_TARGET",
            "a-submit-child2-with-root1-lease",
        ),
        # 非検収者（Worker 本人・Root Requester・別 Root の Requester）
        (
            AGENT_B_ID,
            ["job", "approve", child1_id, "--submission", submission1_id],
            "FORBIDDEN",
            "b-approve-child1",
        ),
        (
            REQUESTER_ID,
            ["job", "approve", child1_id, "--submission", submission1_id],
            "FORBIDDEN",
            "r-approve-child1",
        ),
        (
            REQUESTER_ID,
            ["job", "approve", child2_id, "--submission", submission2_id],
            "FORBIDDEN",
            "r-approve-child2",
        ),
        (
            AGENT_A_ID,
            ["job", "approve", child2_id, "--submission", submission2_id],
            "FORBIDDEN",
            "a-approve-child2",
        ),
        (
            AGENT_B_ID,
            [
                "job",
                "dispute",
                child1_id,
                "--submission",
                submission1_id,
                "--condition",
                "sum",
                "--reason",
                "CONDITION_MISMATCH",
            ],
            "FORBIDDEN",
            "b-dispute-child1",
        ),
        # 会計ビューと返金（Root Requester または system のみ）
        (AGENT_A_ID, ["ledger", "show", root1_id], "FORBIDDEN", "a-ledger-root1"),
        (AGENT_A_ID, ["job", "refund", root1_id], "FORBIDDEN", "a-refund-root1"),
        (AGENT_B_ID, ["job", "refund", root2_id], "FORBIDDEN", "b-refund-root2"),
        # 別 Job（返金対象は Root のみ）
        (
            REQUESTER_ID,
            ["job", "refund", child1_id],
            "INVALID_TARGET",
            "r-refund-child1",
        ),
    ]
    for actor, args, expected_code, label in rejections:
        result = world.run_cli(
            args,
            actor=actor,
            operation_id=(
                None
                if args[0] == "ledger"
                else world.next_operation_id(_operation_kind(args), label)
            ),
            expect_ok=False,
            action=f"{' '.join(args[:2])} ({label})",
        )
        described = f"{actor}: ojp {' '.join(args)}"
        assert result.returncode == 2, f"{described} -> exit={result.returncode}"
        assert result.error_code == expected_code, (
            f"{described} -> {result.error_code} (expected {expected_code});"
            f" stdout={result.stdout}"
        )

    # 未登録の無関係 Actor には読取経路も非公開のまま
    for job_id, label in ((child1_id, "child1"), (child2_id, "child2")):
        rejected = world.run_cli(
            ["job", "get", job_id],
            actor=OUTSIDER_ID,
            expect_ok=False,
            action=f"job get {label} (outsider)",
        )
        assert rejected.returncode == 2, rejected.stdout
        assert rejected.error_code == "FORBIDDEN", rejected.payload

    # ------------------------------------------------------------------
    # Job・資金・成果物公開範囲が不変
    # ------------------------------------------------------------------
    after_root1 = world.observe("after X15 rejections (root1)")
    with world.observing_root(root2_id):
        after_root2 = world.observe("after X15 rejections (root2)")
    _assert_unchanged(before_root1, after_root1)
    _assert_unchanged(before_root2, after_root2)
    assert _artifact_scope(world, [child1_id, child2_id]) == scope_before

    # 正当な検収者（各 Child の Requester）は拒否の後でも通常どおり承認できる
    world.run_cli(
        ["job", "approve", child1_id, "--submission", submission1_id],
        actor=AGENT_A_ID,
        operation_id=world.next_operation_id("approve", "a-child1"),
        action="job approve (child1 by A)",
    )
    world.run_cli(
        ["job", "approve", child2_id, "--submission", submission2_id],
        actor=AGENT_B_ID,
        operation_id=world.next_operation_id("approve", "b-child2"),
        action="job approve (child2 by B)",
    )
    world.settle()
    final_root1 = world.observe("after legitimate approvals (root1)")
    with world.observing_root(root2_id):
        final_root2 = world.observe("after legitimate approvals (root2)")

    # Root1 の Child1 は B へ、Root2 の Child2 は A へ（受取権者どおり）
    assert final_root1.job_states[child1_id] == "DONE"
    assert final_root1.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final_root1.paid_to(AGENT_A_ID) == 0
    assert final_root2.job_states[child2_id] == "DONE"
    assert final_root2.paid_to(AGENT_A_ID) == CHILD_BUDGET_UNITS
    assert final_root2.paid_to(AGENT_B_ID) == 0
    assert harness.check_payee_entitlement(final_root1) == []
    assert harness.check_payee_entitlement(final_root2) == []

    # Root 間の資金移動は起きていない（各 Root の D は 100 のまま）
    assert final_root1.deposit_units == ROOT_BUDGET_UNITS
    assert final_root2.deposit_units == ROOT_BUDGET_UNITS

    # 最後にレポート対象の Root1 を観測して終える
    report_final = world.observe("final (root1)")
    assert report_final.job_states == final_root1.job_states

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label

    world.report.note(
        "X15: 別Root の Lease・別 Job の Lease・非検収者（Worker 本人・Root"
        " Requester・別 Root の Requester）からの操作はすべて exit=2 で"
        " INVALID_TARGET / FORBIDDEN となり、2 つの Root の Job 状態・資金・"
        "成果物公開範囲はいずれも不変。正当な検収者による承認だけが成立する"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {root1_id: "LEASED", child1_id: "DONE"},
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
# helper
# ---------------------------------------------------------------------------


def _operation_kind(args: list[str]) -> str:
    """CLI 引数から operation_id の接頭辞（service が要求する kind）を導く。"""
    if args[0] == "child":
        return "create"
    return args[1]


async def _claim(
    world: harness.E2EWorld,
    session: harness.AgentSession,
    job_id: str,
    version_id: str,
    label: str,
) -> str:
    result = await session.call(
        "ojp_claim_job",
        {
            "job_id": job_id,
            "expected_version_id": version_id,
            "operation_id": world.next_operation_id("claim", label),
        },
    )
    return str(result.data["lease_id"])


async def _create_child(
    world: harness.E2EWorld,
    session: harness.AgentSession,
    parent_id: str,
    lease_id: str,
    label: str,
) -> tuple[str, str]:
    result = await session.call(
        "ojp_create_child_job",
        {
            "parent_job_id": parent_id,
            "lease_id": lease_id,
            "task_key": "part-1",
            "budget": CHILD_BUDGET,
            "deadline": CHILD_DEADLINE,
            "operation_id": world.next_operation_id("create", label),
        },
    )
    child_id = str(result.data["child_id"])
    world.child_ids.append(child_id)
    return child_id, str(result.data["version_id"])


async def _submit(
    world: harness.E2EWorld,
    session: harness.AgentSession,
    job_id: str,
    lease_id: str,
    version_id: str,
    total: int,
) -> str:
    result = await session.call(
        "ojp_submit",
        {
            "job_id": job_id,
            "lease_id": lease_id,
            "version_id": version_id,
            "artifact_json": json.dumps({"sum": total}),
            "operation_id": world.next_operation_id("submit", f"{job_id[-6:]}"),
        },
    )
    return str(result.data["submission_id"])


def _artifact_scope(
    world: harness.E2EWorld, job_ids: list[str]
) -> dict[tuple[str, str], Any]:
    """各 Actor が各 Job の成果物をどこまで読めるかの一覧（公開範囲）。

    権限のある Actor には本文・hash・検証証跡を含む submission をそのまま、
    未登録の無関係 Actor には拒否コードを記録する。
    """
    scope: dict[tuple[str, str], Any] = {}
    for job_id in job_ids:
        for actor in (REQUESTER_ID, AGENT_A_ID, AGENT_B_ID):
            result = world.run_cli(
                ["job", "get", job_id],
                actor=actor,
                action=f"job get (scope {actor})",
            )
            submission = result.data["submission"]
            scope[(job_id, actor)] = {
                "artifact_readable": submission["artifact_readable"],
                **{
                    key: submission.get(key)
                    for key in ARTIFACT_SECRET_KEYS
                },
            }
        rejected = world.run_cli(
            ["job", "get", job_id],
            actor=OUTSIDER_ID,
            expect_ok=False,
            action="job get (scope outsider)",
        )
        scope[(job_id, OUTSIDER_ID)] = rejected.error_code
    return scope


def _assert_unchanged(
    before: harness.Snapshot, after: harness.Snapshot
) -> None:
    """Job 状態・Submission・Acceptance・Lease・口座・Journal・予約・Receipt・Wallet が不変。"""
    assert after.job_states == before.job_states
    assert after.submissions == before.submissions
    assert after.acceptances == before.acceptances
    assert after.leases == before.leases
    assert after.accounts == before.accounts
    assert after.journal == before.journal
    assert after.operations == before.operations
    assert after.receipts == before.receipts
    assert after.wallet_units == before.wallet_units
    assert after.disputes == before.disputes
    assert after.deposit_units == before.deposit_units
    assert after.available_units == before.available_units
    assert after.locked_breakdown_units == before.locked_breakdown_units
