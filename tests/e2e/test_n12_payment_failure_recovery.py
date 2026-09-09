"""N12: 送金障害・再起動・再試行からの回復（計画書 第18節 異常系 12 / N12）。

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 12 / N12 | 送金commit前失敗・commit後応答消失・再起動・再試行 | Job DONE維持、支払い1回。pendingの原資を再利用しない |

同じ Child の 1 件の送金予約に対し、3 通りの障害を**実プロセスで順に**注入
する（第17節「再起動テストでは実際にプロセスを終了し、新プロセスが同じ DB を
読んで回復することを確認する」）:

1. **決済 commit 前の失敗**（`failpoint_before_commit`）: 独立プロセスが
   異常終了する（exit 3）。T1 全体が rollback され、Receipt も台帳変化も
   一切残らない
2. **commit 後の応答消失**（`failpoint_after_commit`）: Receipt は commit
   済み・PaymentOperation は未更新の中断状態のままプロセスが終了する
3. **Receipt 確定後・status 更新前の停止**（`failpoint_after_receipt`）:
   同じ中断状態を別の注入点から作る（2 の後は既に Receipt があるため、
   ここでは 2 の中断状態を引き継いだまま新プロセスの回復を観測する）

各段階で:

- **Job は DONE のまま**（送金の失敗・中断が Job 状態を巻き戻さない）
- **pending の原資を再利用しない**: child_payout の 10 は送金が確定するまで
  他へ流れず、Parent の available へも戻らない。この状態で Child を
  abandon / 失効させようとしても失敗返却は禁止される（第9節）

最後に `ojp payment retry`（実 CLI プロセス）で再試行し、
**Receipt が 1 件だけ**・B の Wallet が +10 だけ・PaymentOperation が
SUCCEEDED へ収束することを確認する。回復後にもう一度 retry しても
Receipt は増えない（二重送金しない）。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import failpoints, harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "N12"

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_n12_payment_failures_settle_exactly_once(tmp_path: Path) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_n12(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_n12(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            start = await harness.start_root_child_part1(world, agent_a, agent_b)
            child_id = start.child_id

            submit = await agent_b.call(
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
                    "submission_id": str(submit.data["submission_id"]),
                    "operation_id": world.next_operation_id("approve", "a-child"),
                },
            )

    reserved = world.observe("after child approve (payment reserved)")
    assert reserved.job_states[child_id] == "DONE"
    payouts = reserved.operations_for(child_id, "payout")
    assert len(payouts) == 1
    payment_operation_id = str(payouts[0]["operation_id"])
    assert payouts[0]["status"] == "PENDING"
    assert reserved.locked_for("child_payout") == CHILD_BUDGET_UNITS

    # ------------------------------------------------------------------
    # 1. 決済 commit 前の失敗: 実プロセスを異常終了させ、何も残さない
    # ------------------------------------------------------------------
    failpoints.crash_payment(
        world,
        seam="failpoint_before_commit",
        payment_operation_id=payment_operation_id,
        label="settlement crash before commit",
    )
    after_before_commit = world.observe("after crash before payment commit")
    assert after_before_commit.job_states[child_id] == "DONE"
    assert after_before_commit.receipts == []
    assert after_before_commit.paid_units == 0
    assert after_before_commit.locked_for("child_payout") == CHILD_BUDGET_UNITS
    assert after_before_commit.journal == reserved.journal
    assert after_before_commit.wallet_units == reserved.wallet_units
    _assert_pending_funds_not_reusable(world, child_id, start.child_lease_id)

    # ------------------------------------------------------------------
    # 2. commit 後の応答消失: Receipt は確定・status は未更新のまま停止
    # ------------------------------------------------------------------
    failpoints.crash_payment(
        world,
        seam="failpoint_after_commit",
        payment_operation_id=payment_operation_id,
        label="settlement crash after commit (response lost)",
    )
    after_response_loss = world.observe("after crash with response lost")
    assert after_response_loss.job_states[child_id] == "DONE"
    # Receipt が正本として残っている（送金自体は 1 回だけ起きた）
    child_receipts = [r for r in after_response_loss.receipts if r["job_id"] == child_id]
    assert len(child_receipts) == 1
    receipt_id = str(child_receipts[0]["receipt_id"])
    # PaymentOperation は未更新（status は SUCCEEDED になっていない）
    interrupted = after_response_loss.operations_for(child_id, "payout")[0]
    assert interrupted["status"] != "SUCCEEDED", interrupted
    assert interrupted["receipt_id"] is None
    # Wallet と paid は Receipt に合わせて 1 回だけ動いている
    assert (
        after_response_loss.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    assert after_response_loss.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert after_response_loss.locked_for("child_payout") == 0

    # ------------------------------------------------------------------
    # 3. Receipt 確定後・status 更新前の停止でも中断状態は変わらない
    # ------------------------------------------------------------------
    failpoints.crash_payment(
        world,
        seam="failpoint_after_receipt",
        payment_operation_id=payment_operation_id,
        label="settlement crash after receipt (before status update)",
    )
    after_receipt_crash = world.observe("after crash before status update")
    assert after_receipt_crash.job_states[child_id] == "DONE"
    assert [r["receipt_id"] for r in after_receipt_crash.receipts] == [receipt_id]
    assert (
        after_receipt_crash.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    assert after_receipt_crash.journal == after_response_loss.journal

    # ------------------------------------------------------------------
    # 再起動・再試行: 新しい CLI プロセスが同じ DB を読んで収束させる
    # ------------------------------------------------------------------
    retry = world.run_cli(
        ["payment", "retry", payment_operation_id],
        actor=REQUESTER_ID,
        action="payment retry (recovery)",
    )
    assert retry.data["payment_status"] == "SUCCEEDED"
    assert retry.data["receipt_id"] == receipt_id
    after_retry = world.observe("after payment retry")
    settled = after_retry.operations_for(child_id, "payout")[0]
    assert settled["status"] == "SUCCEEDED"
    assert settled["receipt_id"] == receipt_id

    # もう一度 retry しても Receipt も Wallet も増えない（二重送金しない）
    again = world.run_cli(
        ["payment", "retry", payment_operation_id],
        actor=REQUESTER_ID,
        action="payment retry (idempotent re-run)",
    )
    assert again.data["already_succeeded"] is True
    assert again.data["receipt_id"] == receipt_id
    after_second_retry = world.observe("after second payment retry")
    assert after_second_retry.receipts == after_retry.receipts
    assert after_second_retry.wallet_units == after_retry.wallet_units
    assert after_second_retry.journal == after_retry.journal

    # 独立 tick を回しても新しい送金は起きない
    world.tick_once(label="tick --once (after recovery)")
    final = world.observe("after tick following recovery")
    assert final.receipts == after_retry.receipts
    assert final.wallet_units == after_retry.wallet_units

    # -- 最終判定: Job DONE 維持・支払い 1 回 ---------------------------
    assert final.job_states[child_id] == "DONE"
    assert len([r for r in final.receipts if r["job_id"] == child_id]) == 1
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.paid_units == CHILD_BUDGET_UNITS
    assert (
        final.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    assert final.locked_for("child_payout") == 0
    assert final.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert harness.check_payee_entitlement(final) == []

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label

    world.report.note(
        "N12: 決済 commit 前失敗・commit 後応答消失・Receipt 確定後の停止を"
        "実プロセスで注入しても Job は DONE を維持し、pending の原資は"
        "再利用されない。新プロセスの payment retry で Receipt 1 件へ収束し、"
        "再実行しても二重送金しない"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {root_id: "LEASED", child_id: "DONE"},
        "paid": {AGENT_B_ID: "10.000000"},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


def _assert_pending_funds_not_reusable(
    world: harness.E2EWorld, child_id: str, child_lease_id: str
) -> None:
    """送金待ちの child_payout を失敗返却へ流せない（第9節）。

    Child は DONE なので abandon は状態違反として拒否される。予約された 10 は
    Parent の available へ戻らず、child_payout に留まる。
    """
    rejected = world.run_cli(
        ["job", "abandon", child_id, "--lease", child_lease_id],
        actor=AGENT_B_ID,
        operation_id=world.next_operation_id("abandon", "b-child-pending"),
        expect_ok=False,
        action="job abandon (child with pending payment)",
    )
    assert rejected.returncode == 2, rejected.stdout
    assert rejected.error_code == "INVALID_STATE", rejected.payload
    after = world.observe("after rejected abandon during pending payment")
    assert after.job_states[child_id] == "DONE"
    assert after.locked_for("child_payout") == CHILD_BUDGET_UNITS
    assert after.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
