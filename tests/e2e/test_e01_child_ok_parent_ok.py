"""E01: Child 成功・Parent 成功（計画書 第18節「指定4結果」）。

| ID | Child | Parent | A支払い | B支払い | Requester返金 | 最終Escrow |
|----|-------|--------|--------:|--------:|--------------:|-----------:|
| E01| 成功  | 成功   | 90      | 10      | 0             | 0          |

実プロセス構成（第17節 E2E 行）:
- Requester（human 役）は CLI 子プロセス（Root 作成・入金・Root 承認・台帳表示）
- A / B は別々の MCP stdio サーバープロセスへ接続した実 ClientSession
- 決済は独立 tick プロセス（`ojp tick --once`）

4結果の判定は決済完了後に行い、中間状態（locked 内訳）は別に照合する。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import harness
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "E01"

CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"
CHILD_BUDGET = "10.000000"
ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000
PARENT_PAYOUT_UNITS = 90_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_e01_child_ok_parent_ok(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        await _run_e01(world)
    except BaseException:
        failed = True
        raise
    finally:
        # テスト失敗時も、その時点までに蓄積した観測をレポートへ書き出す。
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        _assert_report_contents(report_path)


async def _run_e01(world: harness.E2EWorld) -> None:
    # ------------------------------------------------------------------
    # 1. Requester が CLI で Root を作成・入金して OPEN にする
    # ------------------------------------------------------------------
    root_id, root_version_id = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    assert after_fund.job_states[root_id] == "OPEN"
    assert after_fund.deposit_units == ROOT_BUDGET_UNITS
    assert after_fund.available_units == ROOT_BUDGET_UNITS
    assert after_fund.locked_units == 0
    assert after_fund.paid_units == 0
    assert after_fund.refunded_units == 0

    # A の Wallet 残高（自己資金引落しが無いことを全過程で照合する基準値）
    agent_a_wallet_baseline = after_fund.wallet_units[AGENT_A_ID]

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            # MCP に返金・時刻操作・任意 fail tool が無いこと（第13節の8件のみ）
            assert await agent_a.list_tool_names() == [
                "ojp_approve",
                "ojp_claim_job",
                "ojp_create_child_job",
                "ojp_dispute",
                "ojp_get_job",
                "ojp_heartbeat",
                "ojp_list_jobs",
                "ojp_submit",
            ]

            # ----------------------------------------------------------
            # 2. A が MCP で Root を Claim し、part-1 の Child を予算 10 で作成
            # ----------------------------------------------------------
            claim_root = await agent_a.call(
                "ojp_claim_job",
                {
                    "job_id": root_id,
                    "expected_version_id": root_version_id,
                    "operation_id": world.next_operation_id("claim", "a-root"),
                },
            )
            root_lease_id = str(claim_root.data["lease_id"])
            after_root_claim = world.observe("after root claim by A")
            assert after_root_claim.job_states[root_id] == "LEASED"
            assert after_root_claim.active_lease_worker(root_id) == AGENT_A_ID
            assert after_root_claim.available_units == ROOT_BUDGET_UNITS
            assert after_root_claim.locked_units == 0

            create_child = await agent_a.call(
                "ojp_create_child_job",
                {
                    "parent_job_id": root_id,
                    "lease_id": root_lease_id,
                    "task_key": "part-1",
                    "budget": CHILD_BUDGET,
                    "deadline": CHILD_DEADLINE,
                    "operation_id": world.next_operation_id("create", "a-child"),
                },
            )
            child_id = str(create_child.data["child_id"])
            child_version_id = str(create_child.data["version_id"])
            world.child_ids.append(child_id)

            # 中間状態: Child 作成後に child_work=10 が locked にある
            after_child_create = world.observe("after child create")
            assert after_child_create.locked_for("child_work") == CHILD_BUDGET_UNITS
            assert after_child_create.locked_for("child_payout") == 0
            assert after_child_create.locked_units == CHILD_BUDGET_UNITS
            assert (
                after_child_create.available_units
                == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
            )
            assert after_child_create.paid_units == 0
            # A の自己資金引落しが無い（Child 予算は Root Escrow 内部で拘束される）
            assert (
                after_child_create.wallet_units[AGENT_A_ID]
                == agent_a_wallet_baseline
            )

            # ----------------------------------------------------------
            # 3. B が MCP で Child を Claim し {"sum": 6} を submit
            # ----------------------------------------------------------
            claim_child = await agent_b.call(
                "ojp_claim_job",
                {
                    "job_id": child_id,
                    "expected_version_id": child_version_id,
                    "operation_id": world.next_operation_id("claim", "b-child"),
                },
            )
            child_lease_id = str(claim_child.data["lease_id"])
            after_child_claim = world.observe("after child claim by B")
            assert after_child_claim.active_lease_worker(child_id) == AGENT_B_ID

            submit_child = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": child_id,
                    "lease_id": child_lease_id,
                    "version_id": child_version_id,
                    "artifact_json": json.dumps({"sum": 6}),
                    "operation_id": world.next_operation_id("submit", "b-child"),
                },
            )
            child_submission_id = str(submit_child.data["submission_id"])
            after_child_submit = world.observe("after child submit by B")
            assert after_child_submit.job_states[child_id] == "SUBMITTED"
            assert after_child_submit.locked_for("child_work") == CHILD_BUDGET_UNITS
            assert after_child_submit.paid_units == 0

            # ----------------------------------------------------------
            # 4. A が MCP で Child を承認
            # ----------------------------------------------------------
            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": child_id,
                    "submission_id": child_submission_id,
                    "operation_id": world.next_operation_id("approve", "a-child"),
                },
            )
            after_child_approve = world.observe("after child approve by A")
            # 中間状態: B 向け child_payout へ移り、決済前は paid になっていない
            assert after_child_approve.job_states[child_id] == "DONE"
            assert after_child_approve.locked_for("child_work") == 0
            assert after_child_approve.locked_for("child_payout") == CHILD_BUDGET_UNITS
            assert after_child_approve.paid_units == 0
            assert after_child_approve.paid_to(AGENT_B_ID) == 0
            child_payouts = after_child_approve.operations_for(child_id, "payout")
            assert len(child_payouts) == 1
            assert child_payouts[0]["payee_id"] == AGENT_B_ID
            assert child_payouts[0]["status"] in {"PENDING", "RETRYABLE"}
            assert (
                after_child_approve.wallet_units[AGENT_A_ID]
                == agent_a_wallet_baseline
            )

            # Child の送金だけを独立 tick プロセス（--watch）で確定させる。
            # 観測条件は DB 上の Receipt 出現、待機は単調時計ベースの有限 timeout。
            world.run_tick_until(
                lambda conn: conn.execute(
                    "SELECT COUNT(*) AS n FROM payment_operations"
                    " WHERE job_id = ? AND status = 'SUCCEEDED'",
                    (child_id,),
                ).fetchone()["n"]
                == 1
            )
            after_child_payment = world.observe("after child payment (tick --watch)")
            assert after_child_payment.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
            assert after_child_payment.locked_for("child_payout") == 0
            assert after_child_payment.paid_to(AGENT_A_ID) == 0
            assert (
                after_child_payment.wallet_units[AGENT_B_ID]
                == after_child_approve.wallet_units[AGENT_B_ID] + CHILD_BUDGET_UNITS
            )

            # ----------------------------------------------------------
            # 5. A が Parent（Root）へ {"sum": 15} を submit
            # ----------------------------------------------------------
            submit_root = await agent_a.call(
                "ojp_submit",
                {
                    "job_id": root_id,
                    "lease_id": root_lease_id,
                    "version_id": root_version_id,
                    "artifact_json": json.dumps({"sum": 15}),
                    "operation_id": world.next_operation_id("submit", "a-root"),
                },
            )
            root_submission_id = str(submit_root.data["submission_id"])
            after_root_submit = world.observe("after root submit by A")
            assert after_root_submit.job_states[root_id] == "SUBMITTED"

    # ------------------------------------------------------------------
    # 6. Requester が CLI で Root を承認
    # ------------------------------------------------------------------
    world.run_cli(
        ["job", "approve", root_id, "--submission", root_submission_id],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("approve", "r-root"),
        action="job approve (root)",
    )
    after_root_approve = world.observe("after root approve by Requester")
    # 中間状態: Root 承認後に A 向けの支払いが予約される（まだ paid ではない）
    assert after_root_approve.job_states[root_id] == "DONE"
    assert after_root_approve.locked_for("parent_payout") == PARENT_PAYOUT_UNITS
    assert after_root_approve.paid_to(AGENT_A_ID) == 0
    root_payouts = after_root_approve.operations_for(root_id, "payout")
    assert len(root_payouts) == 1
    assert root_payouts[0]["payee_id"] == AGENT_A_ID
    assert root_payouts[0]["status"] in {"PENDING", "RETRYABLE"}
    assert after_root_approve.available_units == 0

    # ------------------------------------------------------------------
    # 7. 独立 tick プロセスで決済を完了させる
    # ------------------------------------------------------------------
    world.settle()
    final = world.observe("after settlement")

    # -- 4結果の判定（決済完了後） --------------------------------------
    assert final.paid_to(AGENT_A_ID) == PARENT_PAYOUT_UNITS
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.refunded_to(REQUESTER_ID) == 0
    assert final.refunded_units == 0
    assert final.escrow_units == 0
    assert final.available_units == 0
    assert final.locked_units == 0
    assert final.job_states[root_id] == "DONE"
    assert final.job_states[child_id] == "DONE"

    # 受取権者との一致（保存則とは独立に、Lease の Worker から導出して照合する）
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert final.submission_worker(root_id) == AGENT_A_ID
    entitled_payouts = {
        final.submission_worker(child_id): CHILD_BUDGET_UNITS,
        final.submission_worker(root_id): PARENT_PAYOUT_UNITS,
    }
    assert final.paid_by_payee_units == entitled_payouts
    assert final.refunded_by_payee_units == {}
    assert harness.check_payee_entitlement(final) == []

    # A の自己資金引落しが無い（受取 90 だけが増える）
    assert (
        final.wallet_units[AGENT_A_ID]
        == agent_a_wallet_baseline + PARENT_PAYOUT_UNITS
    )

    # Requester 側に Child 承認の操作要求が存在しない
    requester_actions = [
        op.action for op in world.report.operations if op.actor == REQUESTER_ID
    ]
    assert not any(
        child_id in action or "approve (child" in action
        for action in requester_actions
    )
    assert [a["decided_by"] for a in final.acceptances if a["job_id"] == child_id] == [
        AGENT_A_ID
    ]
    assert [a["decided_by"] for a in final.acceptances if a["job_id"] == root_id] == [
        REQUESTER_ID
    ]

    # 各観測点で保存則と非負が成立している
    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert snap.conservation_ok_reported, snap.label

    world.report.note("E01: Child 成功・Parent 成功。A=90 / B=10 / 返金=0 / Escrow=0")


def _assert_report_contents(report_path: Path) -> None:
    """レポートが第17節の6項目（scenario_id / operation_id / 終端 Job 状態 /
    受取人別金額 / locked 内訳 / 保存則の結果）を含むこと。"""
    data = json.loads(report_path.read_text(encoding="utf-8"))

    assert data["scenario_id"] == SCENARIO_ID
    assert data["operation_ids"], "operation_id が 1 件も記録されていない"
    assert set(data["terminal_job_states"].values()) == {"DONE"}
    assert data["amounts_by_payee"]["paid"] == {
        AGENT_A_ID: "90.000000",
        AGENT_B_ID: "10.000000",
    }
    assert data["amounts_by_payee"]["refunded"] == {}
    assert data["locked_breakdown"] == {
        "child_payout": "0.000000",
        "child_work": "0.000000",
        "parent_payout": "0.000000",
        "refund": "0.000000",
    }
    assert data["conservation_ok"] is True
    assert all(
        obs["conservation_ok"]
        and obs["accounts_non_negative"]
        and obs["payee_entitlement_ok"]
        for obs in data["conservation"]
    )
