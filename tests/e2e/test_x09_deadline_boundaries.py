"""X09: 期限境界（直前・ちょうど・直後）の3点検証（計画書 第18節 追加検証 X09）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X09 | heartbeat/submitと失効、approve/disputeと期限ちょうど | 境界定義どおり。閉じたLeaseや終端状態が復活しない |

第7節「期限は書込ロックを取得した後のサーバー時刻で決め、**期限ちょうどは
失効側を優先する**（`now < expires_at` を有効、`now >= expires_at` を失効）」
と第12節「`now >= review_due_at` なら新規異議を拒否して自動承認処理へ進む」
「期限が来た操作は tick だけでなく approve/dispute コマンドでも同じ期限判定を
使う」を、**期限直前・期限ちょうど・期限直後の3点**で観測する（第17節）。

境界を跨ぐたびに Lease が延びてしまわないよう、1 つの Root に part-1 /
part-2 / part-3 の 3 Child を作り、同じ境界を別の Job で 3 点観測する。
Root の Lease と検収窓は Child より長く取り、Child の境界だけを跨ぐ。

- **X09A（Lease 失効境界）**: Child Lease 60 秒。
  直前（`expires_at - 1µs`）で part-1 の heartbeat と part-2 の submit が成立、
  ちょうど（`expires_at`）で part-3 の heartbeat / submit が `LEASE_EXPIRED` と
  なり独立 tick が part-3 を EXPIRED にする、直後（`expires_at + 1µs`）でも
  閉じた Lease は復活せず（`LEASE_EXPIRED`）、EXPIRED の Job は再 Claim できない
  （`INVALID_STATE`）。heartbeat で延長した part-1 は境界を越えても LEASED、
  提出済みの part-2 は SUBMITTED のまま。
- **X09B（検収期限境界・dispute）**: Child 検収窓 30 秒。
  直前で part-1 の dispute が成立、ちょうどで part-3 の dispute が
  `DISPUTE_WINDOW_CLOSED` となり同じ transaction で自動承認され、直後は
  終端（DONE）なので dispute / approve とも `INVALID_STATE`。
- **X09C（検収期限境界・approve）**: 直前・ちょうど・直後のいずれでも承認は
  成立する（第12節 手順 6「期限後は自動承認と同じ結論」）。直後は tick の
  自動承認経路が同じ結論（Acceptance 1 件・payout 1 件）を出し、その後の
  手動 approve も新しい Acceptance や送金を作らない。
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

CHILD_BUDGET = "10.000000"
CHILD_BUDGET_UNITS = 10_000_000
ROOT_BUDGET_UNITS = 100_000_000
CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"
TASK_KEYS = ("part-1", "part-2", "part-3")

# Root は境界を跨がない長さ、Child だけが境界を持つ（第18節の専用 fixture と
# 同じ考え方で、Root 公開前にカタログの timing を事前許可として設定する）
ROOT_TIMING = {
    "lease_seconds": 3600,
    "heartbeat_seconds": 20,
    "review_window_seconds": 3600,
    "dispute_window_seconds": 3600,
}
CHILD_LEASE_SECONDS = 60
CHILD_REVIEW_WINDOW_SECONDS = 30


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x09_lease_expiry_boundary(tmp_path: Path) -> None:
    """Lease 失効境界: 直前は有効、ちょうど・直後は失効側。"""
    await _run(harness.create_world(tmp_path, "X09A"), _run_x09a)


@pytest.mark.anyio
async def test_x09_review_deadline_boundary_for_dispute(tmp_path: Path) -> None:
    """検収期限境界（dispute）: 直前は受理、ちょうどは窓を閉じて自動承認。"""
    await _run(harness.create_world(tmp_path, "X09B"), _run_x09b)


@pytest.mark.anyio
async def test_x09_review_deadline_boundary_for_approve(tmp_path: Path) -> None:
    """検収期限境界（approve）: 3点すべてで承認は同じ結論に収束する。"""
    await _run(harness.create_world(tmp_path, "X09C"), _run_x09c)


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
# X09A: Lease 失効境界
# ---------------------------------------------------------------------------


async def _run_x09a(world: harness.E2EWorld) -> dict:
    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            root_id, children = await _start_three_children(
                world, agent_a, agent_b, name="x09a"
            )
            part1, part2, part3 = (children[key] for key in TASK_KEYS)

            # 3 つの Child Lease は同じ時刻に Claim したので同じ境界を持つ
            boundary_us = world.lease_expires_at_us(part3["lease_id"])
            for child in children.values():
                assert world.lease_expires_at_us(child["lease_id"]) == boundary_us
            assert boundary_us == world.current_clock_us() + (
                CHILD_LEASE_SECONDS * 1_000_000
            )

            # -- 直前（expires_at - 1µs）: heartbeat と submit が成立する ----
            world.advance_clock_to(boundary_us - 1)
            before = world.observe("at lease boundary - 1us")
            assert world.current_clock_us() < boundary_us

            heartbeat = await agent_b.call(
                "ojp_heartbeat",
                {
                    "job_id": part1["job_id"],
                    "lease_id": part1["lease_id"],
                    "generation": 1,
                    "operation_id": world.next_operation_id(
                        "heartbeat", "b-part-1-before"
                    ),
                },
            )
            extended_us = int(heartbeat.data["expires_at"])
            assert extended_us > boundary_us

            submitted = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": part2["job_id"],
                    "lease_id": part2["lease_id"],
                    "version_id": part2["version_id"],
                    "artifact_json": json.dumps({"sum": 4}),
                    "operation_id": world.next_operation_id(
                        "submit", "b-part-2-before"
                    ),
                },
            )
            part2_submission_id = str(submitted.data["submission_id"])
            after_before = world.observe("after operations just before the boundary")
            assert after_before.job_states[part2["job_id"]] == "SUBMITTED"
            assert after_before.job_states[part1["job_id"]] == "LEASED"
            assert after_before.job_states[part3["job_id"]] == "LEASED"

            # -- ちょうど（expires_at）: 失効側を優先する -------------------
            world.advance_clock_to(boundary_us)
            assert world.current_clock_us() == boundary_us

            for tool, arguments, label in (
                (
                    "ojp_heartbeat",
                    {
                        "job_id": part3["job_id"],
                        "lease_id": part3["lease_id"],
                        "generation": 1,
                    },
                    "b-part-3-at",
                ),
                (
                    "ojp_submit",
                    {
                        "job_id": part3["job_id"],
                        "lease_id": part3["lease_id"],
                        "version_id": part3["version_id"],
                        "artifact_json": json.dumps({"sum": 5}),
                    },
                    "b-part-3-at-submit",
                ),
            ):
                rejected = await agent_b.call(
                    tool,
                    {
                        **arguments,
                        "operation_id": world.next_operation_id(
                            "heartbeat" if tool == "ojp_heartbeat" else "submit",
                            label,
                        ),
                    },
                    expect_ok=False,
                )
                assert rejected.error_code == "LEASE_EXPIRED", rejected.payload

            at_boundary = world.observe("at the lease boundary")
            # 拒否だけでは Job も Lease も変わらない（失効の確定は tick）
            assert at_boundary.job_states[part3["job_id"]] == "LEASED"
            assert at_boundary.active_lease_worker(part3["job_id"]) == AGENT_B_ID

            # 独立 tick が期限ちょうどの Lease を失効させる
            world.tick_once(label="tick --once (at the lease boundary)")
            expired = world.observe("after tick at the lease boundary")
            assert expired.job_states[part3["job_id"]] == "EXPIRED"
            part3_lease = _lease(expired, part3["lease_id"])
            assert part3_lease["closed_reason"] == "expired"
            assert int(part3_lease["expires_at_us"]) == boundary_us
            # heartbeat で延長した part-1 と提出済みの part-2 は影響を受けない
            assert expired.job_states[part1["job_id"]] == "LEASED"
            assert expired.job_states[part2["job_id"]] == "SUBMITTED"
            assert _lease(expired, part2["lease_id"])["closed_reason"] == "submitted"
            # part-3 の child_work 10 は Parent 生存中なので available へ戻る
            assert expired.locked_for("child_work") == 2 * CHILD_BUDGET_UNITS
            assert expired.available_units == ROOT_BUDGET_UNITS - (
                2 * CHILD_BUDGET_UNITS
            )

            # -- 直後（expires_at + 1µs）: 閉じた Lease も終端状態も復活しない --
            world.advance_clock_to(boundary_us + 1)
            for tool, arguments, label in (
                (
                    "ojp_heartbeat",
                    {
                        "job_id": part3["job_id"],
                        "lease_id": part3["lease_id"],
                        "generation": 1,
                    },
                    "b-part-3-after",
                ),
                (
                    "ojp_submit",
                    {
                        "job_id": part3["job_id"],
                        "lease_id": part3["lease_id"],
                        "version_id": part3["version_id"],
                        "artifact_json": json.dumps({"sum": 5}),
                    },
                    "b-part-3-after-submit",
                ),
            ):
                rejected = await agent_b.call(
                    tool,
                    {
                        **arguments,
                        "operation_id": world.next_operation_id(
                            "heartbeat" if tool == "ojp_heartbeat" else "submit",
                            label,
                        ),
                    },
                    expect_ok=False,
                )
                assert rejected.error_code == "LEASE_EXPIRED", rejected.payload

            # EXPIRED の Job は再 Claim できない（終端状態は復活しない）
            reclaim = await agent_b.call(
                "ojp_claim_job",
                {
                    "job_id": part3["job_id"],
                    "expected_version_id": part3["version_id"],
                    "operation_id": world.next_operation_id(
                        "claim", "b-part-3-reclaim"
                    ),
                },
                expect_ok=False,
            )
            assert reclaim.error_code == "INVALID_STATE", reclaim.payload

            after_boundary = world.observe("after the lease boundary")
            assert after_boundary.job_states == expired.job_states
            assert after_boundary.leases == expired.leases
            assert after_boundary.accounts == expired.accounts
            assert after_boundary.journal == expired.journal

            # 追加 tick でも EXPIRED は動かない（新しい Lease も作らない）
            world.tick_once(label="tick --once (after the lease boundary)")
            settled = world.observe("after tick following the boundary")
            assert settled.job_states[part3["job_id"]] == "EXPIRED"
            assert settled.leases == expired.leases

            # part-2 の提出は生きている（A が承認して B へ 10 支払う）
            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": part2["job_id"],
                    "submission_id": part2_submission_id,
                    "operation_id": world.next_operation_id("approve", "a-part-2"),
                },
            )

    world.settle()
    final = world.observe("after settlement")
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.job_states[part2["job_id"]] == "DONE"
    assert final.job_states[part3["job_id"]] == "EXPIRED"
    assert final.job_states[part1["job_id"]] == "LEASED"
    assert final.locked_for("child_work") == CHILD_BUDGET_UNITS  # part-1 の 10
    assert harness.check_payee_entitlement(final) == []
    _assert_invariants(world)

    world.report.note(
        "X09A: Lease 期限の直前では heartbeat / submit が成立し、期限ちょうどと"
        "直後は LEASE_EXPIRED。独立 tick が期限ちょうどで part-3 を EXPIRED に"
        "し、閉じた Lease の再利用も EXPIRED の再 Claim も拒否される"
    )
    return {
        "scenario_id": "X09A",
        "terminal_job_states": {
            root_id: "LEASED",
            part1["job_id"]: "LEASED",
            part2["job_id"]: "DONE",
            part3["job_id"]: "EXPIRED",
        },
        "paid": {AGENT_B_ID: "10.000000"},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "10.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


# ---------------------------------------------------------------------------
# X09B: 検収期限境界（dispute）
# ---------------------------------------------------------------------------


async def _run_x09b(world: harness.E2EWorld) -> dict:
    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            root_id, children = await _start_three_children(
                world, agent_a, agent_b, name="x09b"
            )
            submissions = await _submit_all(world, agent_b, children)
            part1, part2, part3 = (children[key] for key in TASK_KEYS)

            boundary_us = _review_due_at(world, submissions[part3["job_id"]])
            for submission_id in submissions.values():
                assert _review_due_at(world, submission_id) == boundary_us

            # -- 直前: dispute が受理される ---------------------------------
            world.advance_clock_to(boundary_us - 1)
            world.run_cli(
                [
                    "job",
                    "dispute",
                    part1["job_id"],
                    "--submission",
                    submissions[part1["job_id"]],
                    "--condition",
                    "sum",
                    "--reason",
                    "CONDITION_MISMATCH",
                ],
                actor=REQUESTER_ID,
                operation_id=world.next_operation_id("dispute", "r-part-1-before"),
                action="job dispute (just before the boundary)",
            )
            before = world.observe("after dispute just before the boundary")
            assert before.job_states[part1["job_id"]] == "DISPUTED"
            assert len(before.disputes) == 1
            assert before.disputes[0]["status"] == "OPEN"
            # 異議は資金を動かさない（予約前の対象資金を保持する）
            assert before.locked_for("child_work") == 3 * CHILD_BUDGET_UNITS
            assert before.operations == []

            # 同じ Submission への 2 件目の異議は拒否（最大 1 件・再オープンなし）
            duplicate = world.run_cli(
                [
                    "job",
                    "dispute",
                    part1["job_id"],
                    "--submission",
                    submissions[part1["job_id"]],
                    "--condition",
                    "sum",
                    "--reason",
                    "CONDITION_MISMATCH",
                ],
                actor=REQUESTER_ID,
                operation_id=world.next_operation_id("dispute", "r-part-1-again"),
                expect_ok=False,
                action="job dispute (duplicate)",
            )
            assert duplicate.returncode == 2, duplicate.stdout
            assert duplicate.error_code == "INVALID_STATE", duplicate.payload

            # -- ちょうど: 窓を閉じて自動承認へ進む -------------------------
            world.advance_clock_to(boundary_us)
            closed = world.run_cli(
                [
                    "job",
                    "dispute",
                    part3["job_id"],
                    "--submission",
                    submissions[part3["job_id"]],
                    "--condition",
                    "sum",
                    "--reason",
                    "CONDITION_MISMATCH",
                ],
                actor=REQUESTER_ID,
                operation_id=world.next_operation_id("dispute", "r-part-3-at"),
                expect_ok=False,
                action="job dispute (at the boundary)",
            )
            assert closed.returncode == 2, closed.stdout
            assert closed.error_code == "DISPUTE_WINDOW_CLOSED", closed.payload
            details = closed.payload["error"]["details"]
            assert details is not None
            assert int(details["review_due_at_us"]) == boundary_us

            at_boundary = world.observe("after dispute at the boundary")
            # 拒否と同じ transaction で自動承認が確定している（第12節）
            assert at_boundary.job_states[part3["job_id"]] == "DONE"
            part3_acceptances = [
                a for a in at_boundary.acceptances if a["job_id"] == part3["job_id"]
            ]
            assert len(part3_acceptances) == 1
            assert part3_acceptances[0]["decision"] == "APPROVED"
            assert part3_acceptances[0]["decided_by"] == REQUESTER_ID
            assert [d["job_id"] for d in at_boundary.disputes] == [part1["job_id"]]

            # -- 直後: 終端状態は復活しない ---------------------------------
            world.advance_clock_to(boundary_us + 1)
            for args, label in (
                (
                    [
                        "job",
                        "dispute",
                        part3["job_id"],
                        "--submission",
                        submissions[part3["job_id"]],
                        "--condition",
                        "sum",
                        "--reason",
                        "CONDITION_MISMATCH",
                    ],
                    "r-part-3-after",
                ),
            ):
                rejected = world.run_cli(
                    args,
                    actor=REQUESTER_ID,
                    operation_id=world.next_operation_id("dispute", label),
                    expect_ok=False,
                    action=f"job dispute ({label})",
                )
                assert rejected.returncode == 2, rejected.stdout
                assert rejected.error_code == "INVALID_STATE", rejected.payload

            # part-2 は異議なしで期限を過ぎたので tick が自動承認する
            world.tick_once(label="tick --once (after the boundary)")
            after = world.observe("after tick following the boundary")
            assert after.job_states[part2["job_id"]] == "DONE"
            part2_acceptances = [
                a for a in after.acceptances if a["job_id"] == part2["job_id"]
            ]
            assert len(part2_acceptances) == 1
            assert part2_acceptances[0]["decided_by"] == harness.SYSTEM_ID
            # 有効な異議がある part-1 は承認タイマーの対象外（第12節）
            assert after.job_states[part1["job_id"]] in {"DISPUTED", "DONE"}

    world.settle()
    final = world.observe("after settlement")
    # part-1 の異議は決定的な判定器で PASS が再現され承認側へ収束する
    assert final.job_states[part1["job_id"]] == "DONE"
    assert [d["status"] for d in final.disputes] == ["RESOLVED"]
    assert final.paid_to(AGENT_B_ID) == 3 * CHILD_BUDGET_UNITS
    for child in children.values():
        acceptances = [
            a for a in final.acceptances if a["job_id"] == child["job_id"]
        ]
        assert len(acceptances) == 1, acceptances
        receipts = [r for r in final.receipts if r["job_id"] == child["job_id"]]
        assert len(receipts) == 1, receipts
    assert harness.check_payee_entitlement(final) == []
    _assert_invariants(world)

    world.report.note(
        "X09B: 検収期限の直前では異議が受理され、期限ちょうどの異議は"
        " DISPUTE_WINDOW_CLOSED となって同じ transaction で自動承認される。"
        "直後の異議・再異議は INVALID_STATE で終端状態は復活しない"
    )
    return {
        "scenario_id": "X09B",
        "terminal_job_states": {
            root_id: "LEASED",
            **{child["job_id"]: "DONE" for child in children.values()},
        },
        "paid": {AGENT_B_ID: "30.000000"},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


# ---------------------------------------------------------------------------
# X09C: 検収期限境界（approve）
# ---------------------------------------------------------------------------


async def _run_x09c(world: harness.E2EWorld) -> dict:
    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            root_id, children = await _start_three_children(
                world, agent_a, agent_b, name="x09c"
            )
            submissions = await _submit_all(world, agent_b, children)
            part1, part2, part3 = (children[key] for key in TASK_KEYS)
            boundary_us = _review_due_at(world, submissions[part1["job_id"]])

            # -- 直前: 通常の承認 ------------------------------------------
            world.advance_clock_to(boundary_us - 1)
            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": part1["job_id"],
                    "submission_id": submissions[part1["job_id"]],
                    "operation_id": world.next_operation_id("approve", "a-part-1"),
                },
            )
            before = world.observe("after approve just before the boundary")
            assert before.job_states[part1["job_id"]] == "DONE"

            # -- ちょうど: 期限ちょうどでも承認は許可される（第12節 手順6） --
            world.advance_clock_to(boundary_us)
            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": part2["job_id"],
                    "submission_id": submissions[part2["job_id"]],
                    "operation_id": world.next_operation_id("approve", "a-part-2"),
                },
            )
            at_boundary = world.observe("after approve at the boundary")
            assert at_boundary.job_states[part2["job_id"]] == "DONE"
            part2_acceptances = [
                a for a in at_boundary.acceptances if a["job_id"] == part2["job_id"]
            ]
            assert [a["decided_by"] for a in part2_acceptances] == [AGENT_A_ID]

            # -- 直後: tick の自動承認が同じ結論を出す ---------------------
            world.advance_clock_to(boundary_us + 1)
            world.tick_once(label="tick --once (after the boundary)")
            after = world.observe("after tick following the boundary")
            assert after.job_states[part3["job_id"]] == "DONE"
            part3_acceptances = [
                a for a in after.acceptances if a["job_id"] == part3["job_id"]
            ]
            assert len(part3_acceptances) == 1
            assert part3_acceptances[0]["decided_by"] == harness.SYSTEM_ID
            # 既に承認済みの part-1 / part-2 に 2 件目の Acceptance は作られない
            for child in (part1, part2):
                acceptances = [
                    a for a in after.acceptances if a["job_id"] == child["job_id"]
                ]
                assert len(acceptances) == 1, acceptances

            # 自動承認後の手動 approve は既存結果を返し、二重裁定・二重送金なし
            reapprove = await agent_a.call(
                "ojp_approve",
                {
                    "job_id": part3["job_id"],
                    "submission_id": submissions[part3["job_id"]],
                    "operation_id": world.next_operation_id(
                        "approve", "a-part-3-after-auto"
                    ),
                },
            )
            assert reapprove.ok, reapprove.payload
            after_reapprove = world.observe("after manual approve of an auto-approved job")
            assert after_reapprove.acceptances == after.acceptances
            assert len(after_reapprove.operations_for(part3["job_id"], "payout")) == 1

    world.settle()
    final = world.observe("after settlement")
    assert final.paid_to(AGENT_B_ID) == 3 * CHILD_BUDGET_UNITS
    for child in children.values():
        assert final.job_states[child["job_id"]] == "DONE"
        assert len([r for r in final.receipts if r["job_id"] == child["job_id"]]) == 1
        assert len(final.operations_for(child["job_id"], "payout")) == 1
    assert final.locked_units == 0
    assert harness.check_payee_entitlement(final) == []
    _assert_invariants(world)

    world.report.note(
        "X09C: 承認は期限直前・ちょうど・直後のいずれでも成立し、直後は tick の"
        "自動承認が同じ結論（Acceptance 1 件・payout 1 件）を出す。自動承認後の"
        "手動 approve でも Acceptance も送金も増えない"
    )
    return {
        "scenario_id": "X09C",
        "terminal_job_states": {
            root_id: "LEASED",
            **{child["job_id"]: "DONE" for child in children.values()},
        },
        "paid": {AGENT_B_ID: "30.000000"},
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


async def _start_three_children(
    world: harness.E2EWorld,
    agent_a: harness.AgentSession,
    agent_b: harness.AgentSession,
    *,
    name: str,
) -> tuple[str, dict[str, dict[str, str]]]:
    """Root 入金 100 → A が part-1/2/3 を各 10 で発注 → B が 3 件を Claim。

    Child の Lease 長・検収窓はカタログの timing（Root 公開前の事前許可）で
    与え、Root 側は境界を跨がない長さにする。3 件を同じ固定時刻で Claim する
    ため、3 つの Lease は同じ `expires_at` を持つ。
    """
    card_path = harness.write_root_card(
        world,
        name,
        subcontract_policy={
            "enabled": True,
            "max_amount": "30.000000",
            "max_ratio_bps": 10000,
            "max_children": 3,
            "max_depth": 1,
        },
        timing_policy=ROOT_TIMING,
        catalog_timing_policy={
            "lease_seconds": CHILD_LEASE_SECONDS,
            "heartbeat_seconds": 20,
            "review_window_seconds": CHILD_REVIEW_WINDOW_SECONDS,
            "dispute_window_seconds": 3600,
        },
    )
    root_id, root_version_id = harness.create_and_fund_root(
        world, card_path=card_path
    )
    world.observe("after root fund")

    claim_root = await agent_a.call(
        "ojp_claim_job",
        {
            "job_id": root_id,
            "expected_version_id": root_version_id,
            "operation_id": world.next_operation_id("claim", "a-root"),
        },
    )
    root_lease_id = str(claim_root.data["lease_id"])

    children: dict[str, dict[str, str]] = {}
    for task_key in TASK_KEYS:
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
        children[task_key] = {
            "job_id": child_id,
            "version_id": str(created.data["version_id"]),
            "task_key": task_key,
        }

    for task_key in TASK_KEYS:
        child = children[task_key]
        claimed = await agent_b.call(
            "ojp_claim_job",
            {
                "job_id": child["job_id"],
                "expected_version_id": child["version_id"],
                "operation_id": world.next_operation_id("claim", f"b-{task_key}"),
            },
        )
        child["lease_id"] = str(claimed.data["lease_id"])

    start = world.observe("after three children are claimed")
    assert start.locked_for("child_work") == 3 * CHILD_BUDGET_UNITS
    assert start.available_units == ROOT_BUDGET_UNITS - 3 * CHILD_BUDGET_UNITS
    return root_id, children


# part-1/2/3 のカタログ期待値（第11節の固定カタログ）
EXPECTED_SUMS = {"part-1": 6, "part-2": 4, "part-3": 5}


async def _submit_all(
    world: harness.E2EWorld,
    agent_b: harness.AgentSession,
    children: dict[str, dict[str, str]],
) -> dict[str, str]:
    """B が 3 件を同じ固定時刻で有効提出する（3 件の review_due_at が揃う）。"""
    submissions: dict[str, str] = {}
    for task_key in TASK_KEYS:
        child = children[task_key]
        result = await agent_b.call(
            "ojp_submit",
            {
                "job_id": child["job_id"],
                "lease_id": child["lease_id"],
                "version_id": child["version_id"],
                "artifact_json": json.dumps({"sum": EXPECTED_SUMS[task_key]}),
                "operation_id": world.next_operation_id("submit", f"b-{task_key}"),
            },
        )
        submissions[child["job_id"]] = str(result.data["submission_id"])
    world.observe("after three submissions by B")
    return submissions


def _review_due_at(world: harness.E2EWorld, submission_id: str) -> int:
    with world.read_only_connection() as conn:
        row = conn.execute(
            "SELECT review_due_at_us FROM submissions WHERE id = ?",
            (submission_id,),
        ).fetchone()
    assert row is not None, submission_id
    return int(row["review_due_at_us"])


def _lease(snapshot: harness.Snapshot, lease_id: str) -> dict:
    for lease in snapshot.leases:
        if lease["id"] == lease_id:
            return lease
    raise AssertionError(f"lease not found in snapshot: {lease_id}")


def _assert_invariants(world: harness.E2EWorld) -> None:
    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label
