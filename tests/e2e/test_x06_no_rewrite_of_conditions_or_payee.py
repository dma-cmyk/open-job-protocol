"""X06: 成功条件・検証器・入力・受取人の書換えを拒否する（計画書 第18節 追加検証 X06）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X06 | A/Bが成功条件・検証器・入力・受取人を書換え | 拒否。公開Versionのhash・受取権者維持 |

計画書の根拠:

- 第11節「期待値、対象入力、検証器の版・hash は Root Requester が公開した
  Version とカタログから導出する。A/B が提出と一緒に期待結果・テスト・判定器を
  渡しても採用しない。A は条件を緩めた架空タスクを発注できない」
- 第5節「MCP 引数に actor_id・payee_id を受け付けない」
- 第20節「Worker が条件・テスト・入力・受取人をすり替えられず、Claim した
  Version に束縛される」

実プロセスで書換えの経路を一つずつ潰す:

| 書換え対象 | 試行 | 期待コード |
|---|---|---|
| 受取人 | A が B の Lease 中の part-1 を Claim | CLAIM_CONFLICT |
| 受取人 | A が B の Lease で part-1 を提出 | FORBIDDEN |
| 入力・条件・検証器の版 | B が part-1 を Root の Version で提出 | INVALID_TARGET |
| 入力・条件・検証器の版 | B が part-1 を part-2 の Version で提出 | INVALID_TARGET |
| 成功条件 | B が part-2 の期待値 `{"sum": 4}` を part-1 へ提出 | VERIFICATION_FAILED |
| 期待結果の持ち込み | B が `{"sum": 6, "expected": ...}` を提出 | VERIFICATION_FAILED |
| 判定器の持ち込み | B が `{"sum": 6, "verifier": ...}` を提出 | VERIFICATION_FAILED |
| 入力・条件 | A がカタログ外 `part-unknown` を発注 | TASK_NOT_ALLOWED |
| 受取人 | B が自分の提出を自分で承認 | FORBIDDEN |

加えて:

- MCP の 8 tools が宣言する引数は第13節の表と完全一致し、`actor_id` /
  `payee_id` / `expected` / `input_values` / `verifier_id` を受け付ける口が
  そもそも存在しない
- A の指定できる値は task_key / budget / deadline だけなので、作成された
  part-2 の公開 Version の入力・期待値・条件 hash・検証器はカタログと Root
  公開版から導出された値と一致する（A の申告は採用されない）
- 全試行の前後で Root / part-1 / part-2 の公開 Version 行が 1 バイトも
  変わらない（hash 維持）。最後に B の正当な提出を A が承認すると、
  受取権者どおり B へ 10 が支払われる
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID

SCENARIO_ID = "X06"

CHILD_BUDGET = "10.000000"
CHILD_BUDGET_UNITS = 10_000_000
CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"

# 第13節の表（共通 operation_id を含む宣言済み引数）。actor_id / payee_id /
# expected / input_values / verifier_id はどの tool にも存在しない
EXPECTED_TOOL_ARGUMENTS = {
    "ojp_list_jobs": ["cursor", "limit", "parent_id", "state"],
    "ojp_get_job": ["job_id"],
    "ojp_claim_job": ["expected_version_id", "job_id", "operation_id"],
    "ojp_heartbeat": ["generation", "job_id", "lease_id", "operation_id"],
    "ojp_create_child_job": [
        "budget",
        "deadline",
        "lease_id",
        "operation_id",
        "parent_job_id",
        "task_key",
    ],
    "ojp_submit": [
        "artifact_json",
        "job_id",
        "lease_id",
        "operation_id",
        "version_id",
    ],
    "ojp_approve": ["job_id", "operation_id", "submission_id"],
    "ojp_dispute": [
        "condition_id",
        "evidence",
        "job_id",
        "operation_id",
        "reason_code",
        "submission_id",
    ],
}

# 公開 Version の「成功条件・検証器・入力」を成す列（hash 維持の照合対象）
VERSION_COLUMNS = (
    "id",
    "job_id",
    "version",
    "budget_units",
    "asset",
    "input_json",
    "verifier_id",
    "verifier_hash",
    "conditions_json",
    "conditions_hash",
    "subcontract_policy",
    "task_catalog",
    "timing_policy",
    "artifact_access_policy",
    "deadline_us",
)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x06_conditions_verifier_input_and_payee_cannot_be_rewritten(
    tmp_path: Path,
) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_x06(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_x06(world: harness.E2EWorld) -> dict:
    root_id, root_version_id = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    agent_a_wallet_baseline = after_fund.wallet_units[AGENT_A_ID]
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            # ----------------------------------------------------------
            # 0. 引数の口がそもそも存在しない（第5節・第13節）
            # ----------------------------------------------------------
            for session in (agent_a, agent_b):
                assert await session.list_tool_argument_names() == (
                    EXPECTED_TOOL_ARGUMENTS
                )

            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            part1_id = start.child_id
            part1_version_id = start.child_version_id

            # A の指定できる値は task_key / budget / deadline だけ。part-2 の
            # 公開 Version がカタログと Root 公開版から導出されることを確認する
            create_part2 = await agent_a.call(
                "ojp_create_child_job",
                {
                    "parent_job_id": root_id,
                    "lease_id": start.root_lease_id,
                    "task_key": "part-2",
                    "budget": CHILD_BUDGET,
                    "deadline": CHILD_DEADLINE,
                    "operation_id": world.next_operation_id("create", "a-part-2"),
                },
            )
            part2_id = str(create_part2.data["child_id"])
            part2_version_id = str(create_part2.data["version_id"])
            world.child_ids.append(part2_id)
            world.observe("after part-2 create")
            _assert_child_version_derived_from_catalog(
                world,
                child_id=part2_id,
                root_version_id=root_version_id,
                task_key="part-2",
                input_values=[4],
                expected={"sum": 4},
            )
            _assert_child_version_derived_from_catalog(
                world,
                child_id=part1_id,
                root_version_id=root_version_id,
                task_key="part-1",
                input_values=[1, 2, 3],
                expected={"sum": 6},
            )

            before = world.observe("before rewrite attempts")
            versions_before = _version_rows(world, root_id)
            hashes_before = _version_hashes(versions_before)

            # ----------------------------------------------------------
            # 1. 受取人の書換え: A が part-1 を横取りしようとする
            # ----------------------------------------------------------
            stolen_claim = await agent_a.call(
                "ojp_claim_job",
                {
                    "job_id": part1_id,
                    "expected_version_id": part1_version_id,
                    "operation_id": world.next_operation_id("claim", "a-part-1"),
                },
                expect_ok=False,
            )
            assert stolen_claim.error_code == "CLAIM_CONFLICT", stolen_claim.payload

            stolen_submit = await agent_a.call(
                "ojp_submit",
                {
                    "job_id": part1_id,
                    "lease_id": start.child_lease_id,
                    "version_id": part1_version_id,
                    "artifact_json": json.dumps({"sum": 6}),
                    "operation_id": world.next_operation_id("submit", "a-part-1"),
                },
                expect_ok=False,
            )
            assert stolen_submit.error_code == "FORBIDDEN", stolen_submit.payload

            # ----------------------------------------------------------
            # 2. 入力・条件・検証器の版の書換え: 別 Version での提出
            # ----------------------------------------------------------
            for version_id, label in (
                (root_version_id, "root-version"),
                (part2_version_id, "part-2-version"),
            ):
                swapped = await agent_b.call(
                    "ojp_submit",
                    {
                        "job_id": part1_id,
                        "lease_id": start.child_lease_id,
                        "version_id": version_id,
                        "artifact_json": json.dumps({"sum": 6}),
                        "operation_id": world.next_operation_id(
                            "submit", f"b-part-1-{label}"
                        ),
                    },
                    expect_ok=False,
                )
                assert swapped.error_code == "INVALID_TARGET", swapped.payload

            # ----------------------------------------------------------
            # 3. 成功条件・期待結果・判定器を成果物に載せる試行
            # ----------------------------------------------------------
            for artifact, label, reason in (
                ({"sum": 4}, "rewritten-sum", "ARTIFACT_VALUE_MISMATCH"),
                (
                    {"sum": 6, "expected": {"sum": 4}},
                    "bundled-expected",
                    "ARTIFACT_KEY_MISMATCH",
                ),
                (
                    {"sum": 6, "verifier": "always-pass"},
                    "bundled-verifier",
                    "ARTIFACT_KEY_MISMATCH",
                ),
            ):
                rejected = await agent_b.call(
                    "ojp_submit",
                    {
                        "job_id": part1_id,
                        "lease_id": start.child_lease_id,
                        "version_id": part1_version_id,
                        "artifact_json": json.dumps(artifact),
                        "operation_id": world.next_operation_id(
                            "submit", f"b-part-1-{label}"
                        ),
                    },
                    expect_ok=False,
                )
                assert rejected.error_code == "VERIFICATION_FAILED", rejected.payload
                details = rejected.payload["error"]["details"]
                assert details is not None and details["reason"] == reason, details

            # ----------------------------------------------------------
            # 4. カタログ外タスクによる入力・条件のすり替え
            # ----------------------------------------------------------
            unknown = await agent_a.call(
                "ojp_create_child_job",
                {
                    "parent_job_id": root_id,
                    "lease_id": start.root_lease_id,
                    "task_key": "part-unknown",
                    "budget": CHILD_BUDGET,
                    "deadline": CHILD_DEADLINE,
                    "operation_id": world.next_operation_id(
                        "create", "a-part-unknown"
                    ),
                },
                expect_ok=False,
            )
            assert unknown.error_code == "TASK_NOT_ALLOWED", unknown.payload

            after_attempts = world.observe("after rewrite attempts")
            # 公開 Version は 1 バイトも変わらない（hash 維持）
            assert _version_rows(world, root_id) == versions_before
            assert _version_hashes(_version_rows(world, root_id)) == hashes_before
            # 有効 Submission も資金も動かない（FAIL の Attempt 監査行だけ増える）
            assert after_attempts.submissions == before.submissions
            assert after_attempts.acceptances == before.acceptances
            assert after_attempts.leases == before.leases
            assert after_attempts.accounts == before.accounts
            assert after_attempts.journal == before.journal
            assert after_attempts.operations == before.operations
            assert after_attempts.receipts == before.receipts
            assert after_attempts.wallet_units == before.wallet_units
            assert after_attempts.job_states == before.job_states
            failed_attempts = [
                attempt
                for attempt in after_attempts.attempts_for(part1_id)
                if attempt["outcome"] == "FAIL"
            ]
            assert len(failed_attempts) == 3, failed_attempts

            # ----------------------------------------------------------
            # 5. 受取権者は維持される: B の正当な提出を A が承認する
            # ----------------------------------------------------------
            submit_part1 = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": part1_id,
                    "lease_id": start.child_lease_id,
                    "version_id": part1_version_id,
                    "artifact_json": json.dumps({"sum": 6}),
                    "operation_id": world.next_operation_id("submit", "b-part-1"),
                },
            )
            part1_submission_id = str(submit_part1.data["submission_id"])
            world.observe("after part-1 submit by B")

            # 受取人の書換え: B が自分の提出を自分で承認しようとする
            self_approve = await agent_b.call(
                "ojp_approve",
                {
                    "job_id": part1_id,
                    "submission_id": part1_submission_id,
                    "operation_id": world.next_operation_id("approve", "b-self"),
                },
                expect_ok=False,
            )
            assert self_approve.error_code == "FORBIDDEN", self_approve.payload
            after_self_approve = world.observe("after self-approve attempt by B")
            assert after_self_approve.acceptances == []
            assert after_self_approve.operations == []

            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": part1_id,
                    "submission_id": part1_submission_id,
                    "operation_id": world.next_operation_id("approve", "a-part-1"),
                },
            )

    approved = world.observe("after part-1 approve by A")
    assert approved.job_states[part1_id] == "DONE"
    part1_payouts = approved.operations_for(part1_id, "payout")
    assert len(part1_payouts) == 1
    assert part1_payouts[0]["payee_id"] == AGENT_B_ID

    world.settle()
    final = world.observe("after settlement")

    # 公開 Version は最後まで不変（hash 維持）
    assert _version_rows(world, root_id) == versions_before
    assert _version_hashes(_version_rows(world, root_id)) == hashes_before
    # 受取権者どおり B へ 10。A は 1 単位も受け取らない
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.paid_to(AGENT_A_ID) == 0
    assert (
        final.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    assert final.wallet_units[AGENT_A_ID] == agent_a_wallet_baseline
    assert final.submission_worker(part1_id) == AGENT_B_ID
    assert [
        a["decided_by"] for a in final.acceptances if a["job_id"] == part1_id
    ] == [AGENT_A_ID]
    assert harness.check_payee_entitlement(final) == []
    assert final.locked_for("child_work") == CHILD_BUDGET_UNITS  # part-2 の 10
    assert final.refunded_units == 0

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label

    world.report.note(
        "X06: 受取人（Claim 横取り・他人 Lease での提出・自己承認）・Version"
        "すり替え・成果物への期待値/判定器の持ち込み・カタログ外タスクの"
        "いずれもコードつきで拒否され、公開 Version 行の hash は不変。"
        "受取権者どおり B へ 10 が支払われる"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {
            root_id: "LEASED",
            part1_id: "DONE",
            part2_id: "OPEN",
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


def _version_rows(
    world: harness.E2EWorld, root_id: str
) -> list[dict[str, Any]]:
    """Root 配下の全 Job の公開 Version 行（成功条件・検証器・入力を含む）。"""
    columns = ", ".join(f"v.{name}" for name in VERSION_COLUMNS)
    with world.read_only_connection() as conn:
        rows = conn.execute(
            f"SELECT {columns} FROM job_versions v"
            " JOIN jobs j ON j.id = v.job_id"
            " WHERE j.root_id = ? ORDER BY v.id",
            (root_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def _version_hashes(rows: list[dict[str, Any]]) -> dict[str, str]:
    """Version 行の内容 hash（`公開Versionのhash` の照合値）。"""
    return {
        str(row["id"]): hashlib.sha256(
            json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        for row in rows
    }


def _assert_child_version_derived_from_catalog(
    world: harness.E2EWorld,
    *,
    child_id: str,
    root_version_id: str,
    task_key: str,
    input_values: list[int],
    expected: dict[str, int],
) -> None:
    """Child の公開 Version がカタログ・Root 公開版から導出されている。

    A が指定できるのは task_key / budget / deadline だけなので、入力・期待値・
    条件 hash は指定した task_key のカタログ entry どおり、検証器は Root 公開版
    の継承値になる（第11節）。
    """
    with world.read_only_connection() as conn:
        child = conn.execute(
            "SELECT v.input_json, v.conditions_json, v.conditions_hash,"
            " v.verifier_id, v.verifier_hash, v.task_catalog, j.task_key"
            " FROM jobs j JOIN job_versions v ON v.id = j.version_id"
            " WHERE j.id = ?",
            (child_id,),
        ).fetchone()
        root_version = conn.execute(
            "SELECT verifier_id, verifier_hash FROM job_versions WHERE id = ?",
            (root_version_id,),
        ).fetchone()

    assert child["task_key"] == task_key
    assert json.loads(child["input_json"]) == input_values
    assert json.loads(child["conditions_json"]) == expected
    assert child["conditions_hash"] == hashlib.sha256(
        child["conditions_json"].encode("utf-8")
    ).hexdigest()
    assert child["verifier_id"] == root_version["verifier_id"]
    assert child["verifier_hash"] == root_version["verifier_hash"]
    # Child は再委託不可なのでカタログを持たない
    assert json.loads(child["task_catalog"]) == []
