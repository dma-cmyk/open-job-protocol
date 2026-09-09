"""X11: A 停止後の成果物取得と無関係 Actor への非公開（計画書 第18節 追加検証 X11）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X11 | Root RequesterがA停止後にBの成果物取得、無関係Actorが取得 | Requesterには保存内容/hash一致、無関係Actorには非公開 |

第5節「Requester が資金拠出した Child の有効成果物は、A 停止・Parent 終了後も
Root Requester が取得できる。利用権を JobVersion に事前明記し、読取権を
Parent の状態や A の接続に依存させない」「サービス経由の別 Actor・別 Job への
不正操作は必ず拒否する」を実プロセスで観測する。

2 つの独立 fixture:

- **X11A（A 停止・Parent 終了後の取得）**: B の有効提出後に A の MCP サーバー
  プロセスを終了し、共有 Clock を進めて system の期限処理で Child を自動承認・
  Root を失効させる。Parent が EXPIRED になり A が接続していない状態でも、
  Root Requester は Child の成果物本文・検証証跡・hash を取得でき、保存内容と
  一致する。未登録の無関係 Actor は `job get` / `job list` / `ledger show` の
  いずれも FORBIDDEN。Root Requester でない B は会計ビュー（`ledger show`）を
  取得できない
- **X11B（無関係 Actor には非公開）**: Root 自身も A の有効提出を持つ状態を作り、
  A を停止させる。Root Requester は Root と Child の両方の成果物を取得できるが、
  **Root の成果物に対して無関係な B**（Root の Worker でも Requester でもない）
  には `artifact_readable=false` で本文・hash・検証証跡が返らない。取得試行の
  前後で公開範囲・Job 状態・資金はいずれも変わらない

`artifact_hash` は canonical 成果物の SHA-256 で総当たり可能なため、本文と
同じ読取権を要求する（第5節）。未権限 Actor の応答に本文・hash・証跡の
キーが 1 つも含まれないことを assert する。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

CHILD_BUDGET_UNITS = 10_000_000
PARENT_PAYOUT_UNITS = 90_000_000
REFUND_UNITS = 90_000_000

# demo init が seed しない Participant（登録されていない無関係 Actor）
OUTSIDER_ID = "pt-outsider"

# 成果物本文と、そこから提出内容を復元できる派生情報のキー
ARTIFACT_SECRET_KEYS = ("artifact_json", "artifact_hash", "verification_evidence")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x11_requester_reads_artifact_after_agent_stops(
    tmp_path: Path,
) -> None:
    """A 停止・Parent 終了後も Root Requester は保存内容と一致する成果物を取得できる。"""
    await _run(harness.create_world(tmp_path, "X11A"), _run_x11a)


@pytest.mark.anyio
async def test_x11_unrelated_actor_cannot_read_artifact(tmp_path: Path) -> None:
    """無関係 Actor には成果物本文・hash・検証証跡を返さない。"""
    await _run(harness.create_world(tmp_path, "X11B"), _run_x11b)


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
# X11A: A 停止・Parent 終了後の取得
# ---------------------------------------------------------------------------


async def _run_x11a(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    world.observe("after root fund")

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id
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
    # A（と B）の MCP サーバープロセスは終了済み
    stopped = world.observe("after agent A stopped")
    assert stopped.job_states[child_id] == "SUBMITTED"
    assert stopped.acceptances == []

    # A 停止後は system の期限処理が Child の判定を代替し、Root は失効する
    world.advance_clock_to(
        world.lease_expires_at_us(start.root_lease_id) + 1_000_000
    )
    world.settle()
    terminal = world.observe("after system lifecycle settles everything")
    assert terminal.job_states[root_id] == "EXPIRED"
    assert terminal.job_states[child_id] == "DONE"
    assert terminal.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert terminal.refunded_to(REQUESTER_ID) == REFUND_UNITS
    assert terminal.escrow_units == 0

    # ------------------------------------------------------------------
    # Root Requester は Parent 終端・A 停止後も成果物を取得できる
    # ------------------------------------------------------------------
    stored = _stored_submission(world, child_id)
    requester_view = _job_get(world, child_id, actor=REQUESTER_ID, label="requester")
    submission = requester_view["submission"]
    assert submission is not None
    assert submission["artifact_readable"] is True
    assert submission["artifact_json"] == stored["artifact_json"]
    assert submission["artifact_hash"] == stored["artifact_hash"]
    assert submission["verification_evidence"] == stored["verification_evidence"]
    # hash は保存済み canonical 成果物の SHA-256 と一致する
    assert submission["artifact_hash"] == hashlib.sha256(
        str(stored["artifact_json"]).encode("utf-8")
    ).hexdigest()
    assert json.loads(submission["artifact_json"]) == {"sum": 6}
    # 読取権は JobVersion に事前明記された利用権から来る（Parent 状態に依存しない）
    assert requester_view["version"]["artifact_access_policy"] == {
        "requester_can_read": True
    }
    assert requester_view["job"]["state"] == "DONE"

    # ------------------------------------------------------------------
    # 無関係 Actor（未登録）にはどの取得経路も非公開
    # ------------------------------------------------------------------
    for args, label in (
        (["job", "get", child_id], "job get child"),
        (["job", "get", root_id], "job get root"),
        (["job", "list"], "job list"),
        (["ledger", "show", root_id], "ledger show"),
    ):
        rejected = world.run_cli(
            args,
            actor=OUTSIDER_ID,
            expect_ok=False,
            action=f"{label} (outsider)",
        )
        assert rejected.returncode == 2, rejected.stdout
        assert rejected.error_code == "FORBIDDEN", rejected.payload
        assert not _leaks_artifact(rejected.stdout, stored)

    # Root Requester でも system でもない B には会計ビューを返さない
    b_ledger = world.run_cli(
        ["ledger", "show", root_id],
        actor=AGENT_B_ID,
        expect_ok=False,
        action="ledger show (agent B)",
    )
    assert b_ledger.returncode == 2, b_ledger.stdout
    assert b_ledger.error_code == "FORBIDDEN", b_ledger.payload

    # 取得試行は Job・資金・公開範囲を変えない
    after_attempts = world.observe("after access attempts")
    _assert_unchanged(terminal, after_attempts)
    assert _job_get(world, child_id, actor=REQUESTER_ID, label="requester recheck") == (
        requester_view
    )

    _assert_invariants(world)
    world.report.note(
        "X11A: A の MCP プロセス停止後に system の期限処理で Child が自動承認され"
        " Root が EXPIRED になっても、Root Requester は Child の成果物本文・"
        "検証証跡・hash を保存内容どおり取得できる。未登録の無関係 Actor は"
        " job get / job list / ledger show のいずれも FORBIDDEN"
    )
    return {
        "scenario_id": "X11A",
        "terminal_job_states": {root_id: "EXPIRED", child_id: "DONE"},
        "paid": {AGENT_B_ID: "10.000000"},
        "refunded": {REQUESTER_ID: "90.000000"},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


# ---------------------------------------------------------------------------
# X11B: 無関係 Actor には非公開
# ---------------------------------------------------------------------------


async def _run_x11b(world: harness.E2EWorld) -> dict:
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
            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": child_id,
                    "submission_id": str(submit_child.data["submission_id"]),
                    "operation_id": world.next_operation_id("approve", "a-child"),
                },
            )
            # Root 自身も A の有効提出を持つ（全 Child 判定終端の後）
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
    # A の MCP サーバープロセスは終了済み
    before = world.observe("after agent A stopped")
    assert before.job_states[root_id] == "SUBMITTED"
    assert before.job_states[child_id] == "DONE"

    stored_child = _stored_submission(world, child_id)
    stored_root = _stored_submission(world, root_id)

    # ------------------------------------------------------------------
    # Root Requester は Root と Child の両方を取得できる
    # ------------------------------------------------------------------
    for job_id, stored in ((child_id, stored_child), (root_id, stored_root)):
        view = _job_get(world, job_id, actor=REQUESTER_ID, label="requester")
        submission = view["submission"]
        assert submission is not None
        assert submission["artifact_readable"] is True
        assert submission["artifact_json"] == stored["artifact_json"]
        assert submission["artifact_hash"] == stored["artifact_hash"]
        assert submission["verification_evidence"] == stored["verification_evidence"]

    # ------------------------------------------------------------------
    # Root の成果物に無関係な B には非公開（登録済みだが権限がない）
    # ------------------------------------------------------------------
    b_root_view = _job_get(world, root_id, actor=AGENT_B_ID, label="agent B")
    b_root_submission = b_root_view["submission"]
    assert b_root_submission is not None
    assert b_root_submission["artifact_readable"] is False
    for key in ARTIFACT_SECRET_KEYS:
        assert key not in b_root_submission, key
    # 公開事実（提出の存在・検証結果・期限）だけは返る
    assert b_root_submission["verification_result"] == "PASS"
    assert b_root_submission["submission_id"] == root_submission_id
    assert not _leaks_artifact(json.dumps(b_root_view), stored_root)

    # B 自身が Worker の Child は読める（受取権者本人）
    b_child_view = _job_get(world, child_id, actor=AGENT_B_ID, label="agent B child")
    assert b_child_view["submission"]["artifact_readable"] is True

    # 未登録の無関係 Actor はどの Job も取得できない
    for job_id, label in ((root_id, "root"), (child_id, "child")):
        rejected = world.run_cli(
            ["job", "get", job_id],
            actor=OUTSIDER_ID,
            expect_ok=False,
            action=f"job get {label} (outsider)",
        )
        assert rejected.returncode == 2, rejected.stdout
        assert rejected.error_code == "FORBIDDEN", rejected.payload
        assert not _leaks_artifact(rejected.stdout, stored_root)
        assert not _leaks_artifact(rejected.stdout, stored_child)

    after_attempts = world.observe("after access attempts")
    _assert_unchanged(before, after_attempts)

    # ------------------------------------------------------------------
    # Root 承認・決済まで進めても公開範囲は変わらない
    # ------------------------------------------------------------------
    world.run_cli(
        ["job", "approve", root_id, "--submission", root_submission_id],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("approve", "r-root"),
        action="job approve (root)",
    )
    world.settle()
    final = world.observe("after settlement")
    assert final.job_states[root_id] == "DONE"
    assert final.paid_to(AGENT_A_ID) == PARENT_PAYOUT_UNITS
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS

    after_root_done = _job_get(
        world, root_id, actor=AGENT_B_ID, label="agent B after done"
    )
    assert after_root_done["submission"]["artifact_readable"] is False
    for key in ARTIFACT_SECRET_KEYS:
        assert key not in after_root_done["submission"], key
    requester_after = _job_get(
        world, root_id, actor=REQUESTER_ID, label="requester after done"
    )
    assert requester_after["submission"]["artifact_json"] == (
        stored_root["artifact_json"]
    )
    assert harness.check_payee_entitlement(final) == []

    _assert_invariants(world)
    world.report.note(
        "X11B: Root Requester は Root と Child の成果物を保存内容どおり取得できる"
        "一方、Root の成果物に無関係な B には artifact_readable=false で本文・"
        "hash・検証証跡が返らない。未登録 Actor は FORBIDDEN。Root 承認・決済の"
        "前後で公開範囲は変わらない"
    )
    return {
        "scenario_id": "X11B",
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
# helper
# ---------------------------------------------------------------------------


def _job_get(
    world: harness.E2EWorld, job_id: str, *, actor: str, label: str
) -> dict[str, Any]:
    result = world.run_cli(
        ["job", "get", job_id],
        actor=actor,
        action=f"job get ({label})",
    )
    return result.data


def _stored_submission(
    world: harness.E2EWorld, job_id: str
) -> dict[str, Any]:
    """DB に保存された有効 Submission の本文・hash・検証証跡（比較の正本）。"""
    with world.read_only_connection() as conn:
        row = conn.execute(
            "SELECT artifact_json, artifact_hash, verification_evidence"
            " FROM submissions WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    assert row is not None, job_id
    return dict(row)


def _leaks_artifact(text: str, stored: dict[str, Any]) -> bool:
    """応答テキストが保存成果物・hash・検証証跡を含んでいないかを確認する。"""
    return any(
        str(stored[key]) in text
        for key in ("artifact_json", "artifact_hash", "verification_evidence")
        if stored.get(key)
    )


def _assert_unchanged(
    before: harness.Snapshot, after: harness.Snapshot
) -> None:
    assert after.job_states == before.job_states
    assert after.submissions == before.submissions
    assert after.acceptances == before.acceptances
    assert after.leases == before.leases
    assert after.accounts == before.accounts
    assert after.journal == before.journal
    assert after.operations == before.operations
    assert after.receipts == before.receipts
    assert after.wallet_units == before.wallet_units


def _assert_invariants(world: harness.E2EWorld) -> None:
    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label
