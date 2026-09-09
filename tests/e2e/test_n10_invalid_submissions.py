"""N10: 無効な提出は有効 Submission も支払いも作らない
（計画書 第18節 異常系 10 / N10）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 10 / N10 | 不正JSON／誤答／古いVersion／取得不能となる保存失敗 | 有効Submission・review_due_atなし、支払いなし |

4 通りの無効な提出を**同じ Child の同じ有効 Lease**へ順に行う:

1. **不正 JSON**: parse できない成果物 → `VERIFICATION_FAILED`
   （`details.reason = ARTIFACT_NOT_JSON`）。Attempt だけが残る
2. **誤答**: `{"sum": 7}`（正解は 6） → `VERIFICATION_FAILED`
   （`details.reason = ARTIFACT_VALUE_MISMATCH`）
3. **古い Version**: Root の公開 Version ID を Child の submit に渡す
   → `INVALID_TARGET`。Attempt も残さない（権限・対象の検査で落ちる）
4. **取得不能となる保存失敗**: 検証 PASS 後の保存フェーズで seam を発火
   させ、transaction ごと rollback させる → `VERIFICATION_UNAVAILABLE`

いずれの場合も:

- `submissions` は 0 件（有効 Submission なし・review_due_at なし）
- Job は LEASED のまま、Lease は開いたまま（closed_reason は None）
- Acceptance・PaymentOperation・Receipt・口座残高・Wallet は不変
- 独立 tick を回しても支払いは発生しない

最後に**同じ Lease から正しい成果物を出せば PASS する**ことを確認し、
拒否が Lease や Job を壊していないことを示す。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import failpoints, harness, report
from .harness import AGENT_A_ID, AGENT_B_ID

SCENARIO_ID = "N10"

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000

INVALID_JSON_ARTIFACT = '{"sum": 6'  # 閉じ括弧なし
WRONG_ANSWER_ARTIFACT = json.dumps({"sum": 7})  # 正解は 6
CORRECT_ARTIFACT = json.dumps({"sum": 6})


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n10_invalid_submissions_create_nothing(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_n10(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_n10(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    world.observe("after root fund")

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

            baseline = world.observe("before invalid submissions")
            assert baseline.job_states[child_id] == "LEASED"
            assert baseline.submissions == []

            # ----------------------------------------------------------
            # 1. 不正 JSON
            # ----------------------------------------------------------
            invalid_json = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": child_id,
                    "lease_id": start.child_lease_id,
                    "version_id": start.child_version_id,
                    "artifact_json": INVALID_JSON_ARTIFACT,
                    "operation_id": world.next_operation_id("submit", "invalid-json"),
                },
                expect_ok=False,
            )
            assert invalid_json.error_code == "VERIFICATION_FAILED", (
                invalid_json.payload
            )
            assert (
                invalid_json.payload["error"]["details"]["reason"]
                == "ARTIFACT_NOT_JSON"
            )
            after_invalid_json = world.observe("after invalid JSON submit")
            _assert_nothing_persisted(after_invalid_json, baseline, child_id)
            assert len(after_invalid_json.attempts_for(child_id)) == 1
            assert after_invalid_json.attempts_for(child_id)[0]["outcome"] == "FAIL"

            # ----------------------------------------------------------
            # 2. 誤答
            # ----------------------------------------------------------
            wrong_answer = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": child_id,
                    "lease_id": start.child_lease_id,
                    "version_id": start.child_version_id,
                    "artifact_json": WRONG_ANSWER_ARTIFACT,
                    "operation_id": world.next_operation_id("submit", "wrong-answer"),
                },
                expect_ok=False,
            )
            assert wrong_answer.error_code == "VERIFICATION_FAILED", (
                wrong_answer.payload
            )
            assert (
                wrong_answer.payload["error"]["details"]["reason"]
                == "ARTIFACT_VALUE_MISMATCH"
            )
            after_wrong_answer = world.observe("after wrong answer submit")
            _assert_nothing_persisted(after_wrong_answer, baseline, child_id)
            assert len(after_wrong_answer.attempts_for(child_id)) == 2

            # ----------------------------------------------------------
            # 3. 古い Version（Child へ Root の公開 Version を指定する）
            # ----------------------------------------------------------
            stale_version = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": child_id,
                    "lease_id": start.child_lease_id,
                    "version_id": start.root_version_id,
                    "artifact_json": CORRECT_ARTIFACT,
                    "operation_id": world.next_operation_id("submit", "stale-version"),
                },
                expect_ok=False,
            )
            assert stale_version.error_code == "INVALID_TARGET", stale_version.payload
            after_stale_version = world.observe("after stale version submit")
            _assert_nothing_persisted(after_stale_version, baseline, child_id)
            # 権限・対象の検査で落ちるので Attempt も増えない
            assert len(after_stale_version.attempts_for(child_id)) == 2

            # ----------------------------------------------------------
            # 4. 取得不能となる保存失敗（検証 PASS 後の保存フェーズで rollback）
            # ----------------------------------------------------------
            storage_failure_code = failpoints.submit_with_storage_failure(
                world,
                actor=AGENT_B_ID,
                job_id=child_id,
                lease_id=start.child_lease_id,
                version_id=start.child_version_id,
                artifact_json=CORRECT_ARTIFACT,
                operation_id=world.next_operation_id("submit", "storage-failure"),
            )
            assert storage_failure_code == "VERIFICATION_UNAVAILABLE"
            after_storage_failure = world.observe("after storage-failure submit")
            _assert_nothing_persisted(after_storage_failure, baseline, child_id)
            # 保存フェーズごと rollback されるので Attempt も残らない
            assert len(after_storage_failure.attempts_for(child_id)) == 2

            # ----------------------------------------------------------
            # 独立 tick を回しても支払いは発生しない
            # ----------------------------------------------------------
            world.tick_once(label="tick --once (after invalid submissions)")
            after_tick = world.observe("after tick (no valid submission)")
            _assert_nothing_persisted(after_tick, baseline, child_id)
            assert after_tick.job_states[child_id] == "LEASED"

            # ----------------------------------------------------------
            # 同じ Lease から正しい成果物を出せば PASS する
            # ----------------------------------------------------------
            good = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": child_id,
                    "lease_id": start.child_lease_id,
                    "version_id": start.child_version_id,
                    "artifact_json": CORRECT_ARTIFACT,
                    "operation_id": world.next_operation_id("submit", "b-child-valid"),
                },
            )
            assert good.data["verification"] == "PASS"

    final = world.observe("after valid submit from the same lease")
    assert final.job_states[child_id] == "SUBMITTED"
    assert len(final.submissions) == 1
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert int(_only(final.submissions, child_id)["review_due_at_us"]) > 0
    assert final.paid_units == 0
    assert final.refunded_units == 0

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label

    world.report.note(
        "N10: 不正 JSON（ARTIFACT_NOT_JSON）・誤答（ARTIFACT_VALUE_MISMATCH）・"
        "古い Version（INVALID_TARGET）・保存失敗（VERIFICATION_UNAVAILABLE）は"
        "いずれも有効 Submission・review_due_at・支払いを作らず、"
        "同じ Lease からの正しい提出はその後 PASS する"
    )
    return {
        "scenario_id": SCENARIO_ID,
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


def _assert_nothing_persisted(
    after: harness.Snapshot, baseline: harness.Snapshot, child_id: str
) -> None:
    """有効 Submission・review_due_at・支払い・残高変化のいずれも無い。"""
    assert after.submissions == []
    assert after.acceptances == []
    assert after.operations == []
    assert after.receipts == []
    assert after.job_states == baseline.job_states
    assert after.leases == baseline.leases
    assert after.active_lease_worker(child_id) == AGENT_B_ID
    assert after.accounts == baseline.accounts
    assert after.locked_breakdown_units == baseline.locked_breakdown_units
    assert after.available_units == baseline.available_units
    assert after.wallet_units == baseline.wallet_units
    assert after.journal == baseline.journal
    assert after.paid_units == 0
    assert after.refunded_units == 0


def _only(rows: list[dict], job_id: str) -> dict:
    matching = [row for row in rows if row["job_id"] == job_id]
    assert len(matching) == 1, matching
    return matching[0]
