"""X14: 決定的裁定 PASS・証拠付き FAIL・裁定応答なし（計画書 第18節 追加検証 X14）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X14 | 決定的裁定PASS／証拠付きFAIL／裁定応答なし | PASS→支払い、FAIL→返却、無応答→保存PASS採用。全件解決状態へ |

第12節「DISPUTED では予約前の対象資金を保持する。tick の固定判定器が保存
JSON、Claim 時 Version、元検証証跡を使って再検証する。PASS なら DONE、既存
条件への FAIL が再現された場合だけ FAILED。通常は決定的な検証器なので PASS が
再現され、**FAIL 経路は判定器不具合等を模した専用 fixture で検証する**」
「判定待ち期限まで固定判定器が応答しない場合は、有効提出時の保存済み PASS を
採用して自動承認する」を実プロセスで観測する。

3 つの独立 fixture（どれも Root 入金 100・part-1 予算 10・B の有効提出・
Root Requester による固定 condition `sum` への異議から始める）:

- **X14A（決定的裁定 PASS）**: 通常の独立 tick。保存 JSON を再検証すると PASS が
  再現され、DONE ＋ Acceptance APPROVED ＋ B へ 10 の支払い
- **X14B（証拠付き FAIL）**: 判定器不具合・成果物の完全性喪失を模した専用
  fixture（`arbiter_stored_artifact_override` で再検証へ渡す保存成果物を
  差し替える）。verdict は注入せず、差し替えたデータに対して実検証器が
  FAIL と原因 condition を導出する。異議の condition_id に起因する FAIL なので
  FAILED ＋ Acceptance REJECTED ＋ child_work の返却（B へ支払わない）
- **X14C（裁定応答なし）**: `failpoint_arbiter_unresponsive` を立てた tick。
  判定待ち期限（`disputes.due_at_us`）前は**何も書かず**異議を OPEN のまま
  残し、期限ちょうどで保存済み PASS の fallback を採用して自動承認する

どの経路でも最後に OPEN の異議が残らず（全件解決状態）、未確定の
PaymentOperation も残らないことを確認する。
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from . import failpoints, harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID, SYSTEM_ID

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000
CONDITION_ID = "sum"
# 判定器不具合を模して再検証へ渡す成果物（期待値 6 に対する不一致）
CORRUPTED_ARTIFACT = json.dumps({"sum": 999})


@dataclass
class Disputed:
    """異議が OPEN になった状態の識別子。"""

    root_id: str
    child_id: str
    submission_id: str
    dispute_id: str
    due_at_us: int


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x14_deterministic_arbitration_pass(tmp_path: Path) -> None:
    """決定的裁定で PASS が再現され、DONE ＋ B へ支払い。"""
    await _run(harness.create_world(tmp_path, "X14A"), _run_x14a)


@pytest.mark.anyio
async def test_x14_arbitration_fail_with_evidence(tmp_path: Path) -> None:
    """判定器不具合 fixture で FAIL が再現され、FAILED ＋ 返却。"""
    await _run(harness.create_world(tmp_path, "X14B"), _run_x14b)


@pytest.mark.anyio
async def test_x14_unresponsive_arbiter_uses_stored_pass(tmp_path: Path) -> None:
    """裁定応答なしは期限前に何も書かず、期限ちょうどで保存 PASS を採用。"""
    await _run(harness.create_world(tmp_path, "X14C"), _run_x14c)


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
# X14A: 決定的裁定 PASS
# ---------------------------------------------------------------------------


async def _run_x14a(world: harness.E2EWorld) -> dict:
    state = await _start_disputed(world)

    world.tick_once(label="tick --once (deterministic arbitration)")
    resolved = world.observe("after deterministic arbitration")

    assert resolved.job_states[state.child_id] == "DONE"
    resolution = _resolution(resolved, state.dispute_id)
    assert resolution["outcome"] == "PASS"
    assert resolution["reason"] == "OK"
    assert resolution["condition_id"] == CONDITION_ID
    # PASS は FAIL を再現していないので帰属は無い
    assert resolution["condition_matched"] is False
    assert resolution["failed_condition_id"] is None
    assert resolution["evidence"] is not None
    _assert_verifier_recorded(world, state, resolution)

    acceptances = [
        a for a in resolved.acceptances if a["job_id"] == state.child_id
    ]
    assert len(acceptances) == 1
    assert acceptances[0]["decision"] == "APPROVED"
    assert acceptances[0]["decided_by"] == SYSTEM_ID

    world.settle()
    final = world.observe("after settlement")
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.refunded_units == 0
    assert final.locked_units == 0
    assert final.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    assert final.submission_worker(state.child_id) == AGENT_B_ID
    assert harness.check_payee_entitlement(final) == []
    _assert_fully_resolved(world, final)

    world.report.note(
        "X14A: 決定的な固定判定器が保存 JSON・Claim 時 Version・元検証証跡から"
        " PASS を再現し、Child は DONE ＋ Acceptance APPROVED（system）となって"
        " B へ 10 が支払われた。異議は RESOLVED で全件解決"
    )
    return {
        "scenario_id": "X14A",
        "terminal_job_states": {state.root_id: "LEASED", state.child_id: "DONE"},
        "paid": {AGENT_B_ID: "10.000000"},
        "refunded": {},
        "locked_breakdown": _EMPTY_LOCKED,
    }


# ---------------------------------------------------------------------------
# X14B: 証拠付き FAIL
# ---------------------------------------------------------------------------


async def _run_x14b(world: harness.E2EWorld) -> dict:
    state = await _start_disputed(world)
    before = world.observe("before arbitration (dispute OPEN)")
    assert before.locked_for("child_work") == CHILD_BUDGET_UNITS
    assert before.operations == []

    failpoints.tick_with_corrupted_stored_artifact(
        world,
        artifact_json=CORRUPTED_ARTIFACT,
        label="arbitration FAIL fixture",
    )
    resolved = world.observe("after arbitration FAIL")

    # FAIL → FAILED ＋ Acceptance REJECTED ＋ child_work の返却
    assert resolved.job_states[state.child_id] == "FAILED"
    acceptances = [
        a for a in resolved.acceptances if a["job_id"] == state.child_id
    ]
    assert len(acceptances) == 1
    assert acceptances[0]["decision"] == "REJECTED"
    assert acceptances[0]["decided_by"] == SYSTEM_ID

    resolution = _resolution(resolved, state.dispute_id)
    assert resolution["outcome"] == "FAIL"
    assert resolution["reason"] == "ARTIFACT_VALUE_MISMATCH"
    assert resolution["condition_id"] == CONDITION_ID
    # 原因 condition は差し替えた保存データから実検証器が導出する
    assert resolution["failed_condition_id"] == CONDITION_ID
    assert resolution["condition_matched"] is True
    assert resolution["expected_value"] == 6
    assert resolution["actual_value"] == 999
    assert resolution["evidence"] is not None
    _assert_verifier_recorded(world, state, resolution)

    # 返却: child_work → available（B へは支払わない・返金予約もしない）
    assert resolved.locked_units == 0
    assert resolved.available_units == ROOT_BUDGET_UNITS
    assert resolved.journal_total("child_work", reason="return") == (
        -CHILD_BUDGET_UNITS
    )
    assert resolved.paid_units == 0
    assert resolved.refunded_units == 0
    assert resolved.operations == []
    assert resolved.receipts == []

    world.tick_once(label="tick --once (after arbitration FAIL)")
    final = world.observe("after tick following arbitration FAIL")
    assert final.job_states[state.child_id] == "FAILED"
    assert final.paid_units == 0
    assert final.receipts == []
    assert final.acceptances == resolved.acceptances
    assert final.journal == resolved.journal
    _assert_fully_resolved(world, final)

    world.report.note(
        "X14B: 判定器不具合を模して再検証へ渡す保存成果物を差し替えると、実検証器が"
        " ARTIFACT_VALUE_MISMATCH と原因 condition 'sum' を導出し、異議の"
        " condition_id に起因する FAIL として Child は FAILED ＋ Acceptance"
        " REJECTED。child_work 10 は Parent の available へ返却され B への"
        "支払いは発生しない"
    )
    return {
        "scenario_id": "X14B",
        "terminal_job_states": {state.root_id: "LEASED", state.child_id: "FAILED"},
        "paid": {},
        "refunded": {},
        "locked_breakdown": _EMPTY_LOCKED,
    }


# ---------------------------------------------------------------------------
# X14C: 裁定応答なし
# ---------------------------------------------------------------------------


async def _run_x14c(world: harness.E2EWorld) -> dict:
    state = await _start_disputed(world)
    before = world.observe("before unresponsive arbitration")
    assert world.current_clock_us() < state.due_at_us

    # -- 期限前: 何も書かない（異議は OPEN のまま） ----------------------
    failpoints.tick_with_unresponsive_arbiter(
        world, label="tick before the arbitration deadline"
    )
    pending = world.observe("after unresponsive tick before the deadline")
    assert pending.job_states[state.child_id] == "DISPUTED"
    assert [d["status"] for d in pending.disputes] == ["OPEN"]
    assert pending.disputes[0]["resolution"] is None
    assert pending.acceptances == []
    assert pending.accounts == before.accounts
    assert pending.journal == before.journal
    assert pending.operations == []
    assert pending.receipts == []
    # resolve:{dispute_id} の operations 行も残らない（transaction ごと rollback）
    assert _operation_exists(world, f"resolve:{state.dispute_id}") is False

    # -- 期限ちょうど: 保存済み PASS の fallback を採用する ----------------
    world.advance_clock_to(state.due_at_us)
    assert world.current_clock_us() == state.due_at_us
    failpoints.tick_with_unresponsive_arbiter(
        world, label="tick at the arbitration deadline"
    )
    resolved = world.observe("after unresponsive tick at the deadline")

    assert resolved.job_states[state.child_id] == "DONE"
    resolution = _resolution(resolved, state.dispute_id)
    assert resolution["outcome"] == "UNRESPONSIVE_ARBITER_STORED_PASS"
    assert resolution["reason"] == "ARBITER_UNRESPONSIVE_STORED_PASS_FALLBACK"
    assert resolution["condition_id"] == CONDITION_ID
    assert resolution["condition_matched"] is False
    # 判定器の証跡は無いので原因 condition・期待値・実値は記録されない
    assert resolution["failed_condition_id"] is None
    assert resolution["expected_value"] is None
    assert resolution["actual_value"] is None
    # 採用したのは有効提出時の保存済み検証証跡
    stored_evidence = _stored_evidence(world, state.submission_id)
    assert resolution["evidence"] == stored_evidence
    _assert_verifier_recorded(world, state, resolution)
    assert _operation_exists(world, f"resolve:{state.dispute_id}") is True

    acceptances = [
        a for a in resolved.acceptances if a["job_id"] == state.child_id
    ]
    assert len(acceptances) == 1
    assert acceptances[0]["decision"] == "APPROVED"
    assert acceptances[0]["decided_by"] == SYSTEM_ID

    world.settle()
    final = world.observe("after settlement")
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert len([r for r in final.receipts if r["job_id"] == state.child_id]) == 1
    assert final.locked_units == 0
    assert final.refunded_units == 0
    assert harness.check_payee_entitlement(final) == []
    _assert_fully_resolved(world, final)

    world.report.note(
        "X14C: 固定判定器が応答しない間、判定待ち期限前の tick は何も書かず異議を"
        " OPEN のまま残す（operations 行も作らない）。期限ちょうどで保存済み PASS の"
        " fallback を採用して DONE ＋ Acceptance APPROVED（system）となり、B へ"
        " 10 が支払われて全件解決する"
    )
    return {
        "scenario_id": "X14C",
        "terminal_job_states": {state.root_id: "LEASED", state.child_id: "DONE"},
        "paid": {AGENT_B_ID: "10.000000"},
        "refunded": {},
        "locked_breakdown": _EMPTY_LOCKED,
    }


# ---------------------------------------------------------------------------
# 共通手順
# ---------------------------------------------------------------------------


_EMPTY_LOCKED = {
    "child_payout": "0.000000",
    "child_work": "0.000000",
    "parent_payout": "0.000000",
    "refund": "0.000000",
}


async def _start_disputed(world: harness.E2EWorld) -> Disputed:
    """Root 入金 100 → part-1（10）の有効提出 → Root Requester の固定条件への異議。"""
    root_id, _ = harness.create_and_fund_root(world)
    world.observe("after root fund")

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            submit = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": start.child_id,
                    "lease_id": start.child_lease_id,
                    "version_id": start.child_version_id,
                    "artifact_json": json.dumps({"sum": 6}),
                    "operation_id": world.next_operation_id("submit", "b-child"),
                },
            )
            submission_id = str(submit.data["submission_id"])

    disputed = world.run_cli(
        [
            "job",
            "dispute",
            start.child_id,
            "--submission",
            submission_id,
            "--condition",
            CONDITION_ID,
            "--reason",
            "CONDITION_MISMATCH",
        ],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("dispute", "r-child"),
        action="job dispute (child)",
    )
    dispute_id = str(disputed.data["dispute_id"])
    due_at_us = int(disputed.data["due_at_us"])

    after_dispute = world.observe("after dispute by the root requester")
    assert after_dispute.job_states[start.child_id] == "DISPUTED"
    # DISPUTED では予約前の対象資金をそのまま保持する（第12節）
    assert after_dispute.locked_for("child_work") == CHILD_BUDGET_UNITS
    assert after_dispute.locked_for("child_payout") == 0
    assert after_dispute.operations == []
    assert after_dispute.acceptances == []
    assert [d["id"] for d in after_dispute.disputes] == [dispute_id]
    assert after_dispute.disputes[0]["opened_by"] == REQUESTER_ID
    assert after_dispute.disputes[0]["condition_id"] == CONDITION_ID

    return Disputed(
        root_id=root_id,
        child_id=start.child_id,
        submission_id=submission_id,
        dispute_id=dispute_id,
        due_at_us=due_at_us,
    )


def _resolution(snapshot: harness.Snapshot, dispute_id: str) -> dict[str, Any]:
    row = next(d for d in snapshot.disputes if d["id"] == dispute_id)
    assert row["status"] == "RESOLVED", row
    assert row["resolution"] is not None, row
    return json.loads(str(row["resolution"]))


def _assert_verifier_recorded(
    world: harness.E2EWorld, state: Disputed, resolution: dict[str, Any]
) -> None:
    """resolution の検証器の版・hash が公開 Version 由来であることを確認する。"""
    with world.read_only_connection() as conn:
        row = conn.execute(
            "SELECT v.verifier_id, v.verifier_hash FROM submissions s"
            " JOIN job_versions v ON v.id = s.version_id"
            " WHERE s.id = ?",
            (state.submission_id,),
        ).fetchone()
    assert row is not None
    assert resolution["verifier_id"] == row["verifier_id"]
    assert resolution["verifier_hash"] == row["verifier_hash"]


def _stored_evidence(world: harness.E2EWorld, submission_id: str) -> str:
    with world.read_only_connection() as conn:
        row = conn.execute(
            "SELECT verification_evidence FROM submissions WHERE id = ?",
            (submission_id,),
        ).fetchone()
    assert row is not None
    return str(row["verification_evidence"])


def _operation_exists(world: harness.E2EWorld, operation_id: str) -> bool:
    with world.read_only_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
    return row is not None


def _assert_fully_resolved(
    world: harness.E2EWorld, final: harness.Snapshot
) -> None:
    """全件解決状態: OPEN の異議も未確定の送金も残らない。"""
    assert [d["status"] for d in final.disputes] == ["RESOLVED"]
    assert world.pending_payment_count() == 0
    assert all(
        state in {"DONE", "FAILED", "EXPIRED"}
        for job_id, state in final.job_states.items()
        if job_id != final.root_id
    ), final.job_states
    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label
