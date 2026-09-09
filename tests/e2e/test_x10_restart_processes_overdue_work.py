"""X10: 全プロセス停止・時刻更新・再起動後の過期限処理と Receipt 照会
（計画書 第18節 追加検証 X10）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X10 | 全プロセス再起動後の過期限処理とReceipt照会 | 保存期限から処理再開。二重裁定・二重送金なし |

第12節「全プロセス停止後も期限は DB に残り、再起動時に過期限分を処理する。
MCP リクエストが来なければ進まない実装にしない」、第17節「再起動テストでは
実際にプロセスを終了し、新プロセスが同じ DB を読んで回復することを確認する」
「CLI・MCP・tick が更新後の同じ時刻を観測した証拠を、記録日時と期限判定から
確認する」を実プロセスで観測する。

進行:

1. A が Root を Claim して part-1（10）を発注し、B が Claim して有効提出する
   （A / B は別々の MCP stdio サーバープロセス）
2. **A / B の MCP サーバープロセスを終了する**（全プロセス停止）。この時点で
   保存されている期限は Child の `review_due_at` と Root Lease の `expires_at`
3. アプリのプロセスが 1 つも動いていない状態で共有 Clock を Root Lease 期限の
   直後へ進める
4. **再起動 1**: 新しいプロセスが同じ DB を読んで過期限分を処理する。
   Root は EXPIRED＋未拘束 90 の返金予約、Child は保存済み PASS の自動承認
   （decided_by=system）＋ B へ 10 の payout 予約。続く送金の 1 件目で
   `failpoint_after_commit` が発火し、**Receipt は commit 済み・
   PaymentOperation は未更新**の中断状態でプロセスが実際に終了する（exit 3）
5. **再起動 2**: `ojp payment retry`（新しい CLI プロセス）が Receipt を照会し
   （Receipt が正本）、同じ `receipt_id` で SUCCEEDED へ収束する。Receipt は
   増えない
6. **再起動 3・4**: `tick --once` を新プロセスで 2 回。残りの送金が確定し、
   2 回目では Acceptance・PaymentOperation・Receipt が 1 件も増えない
   （二重裁定・二重送金なし）
7. 最終: B=10 / Requester=90 / A=0 / Escrow=0

時刻の一致は、更新後の共有 Clock 値に対して CLI（`ledger show` の `now`）・
MCP（`ojp_get_job` の `now`）・tick（自動承認の `decided_at`・失効 Event の
`at`）がすべて同じ値を観測したことで確認する。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ojp import service

from . import failpoints, harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID, SYSTEM_ID

SCENARIO_ID = "X10"

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000
REFUND_UNITS = 90_000_000


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x10_restart_resumes_overdue_work_without_double_effects(
    tmp_path: Path,
) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_x10(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_x10(world: harness.E2EWorld) -> dict:
    root_id, _ = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]
    requester_wallet_baseline = after_fund.wallet_units[REQUESTER_ID]

    # ------------------------------------------------------------------
    # 1. A / B が MCP サーバープロセス経由で提出まで進める
    # ------------------------------------------------------------------
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
            submission_id = str(submit.data["submission_id"])

    # ------------------------------------------------------------------
    # 2. 全プロセス停止（MCP サーバーは上の with を抜けて終了している）
    # ------------------------------------------------------------------
    stopped = world.observe("after all agent processes stopped")
    assert stopped.job_states[root_id] == "LEASED"
    assert stopped.job_states[child_id] == "SUBMITTED"
    assert stopped.acceptances == []
    assert stopped.operations == []
    assert stopped.receipts == []

    review_due_at_us = _review_due_at(world, submission_id)
    root_lease_expires_at_us = world.lease_expires_at_us(start.root_lease_id)
    assert review_due_at_us < root_lease_expires_at_us

    # ------------------------------------------------------------------
    # 3. プロセスが動いていない間に共有 Clock を両方の期限より後へ進める
    # ------------------------------------------------------------------
    restart_now_us = root_lease_expires_at_us + 1_000_000
    world.advance_clock_to(restart_now_us)
    assert world.current_clock_us() == restart_now_us

    # ------------------------------------------------------------------
    # 4. 再起動 1: 過期限分を処理し、送金の commit 後にプロセスが終了する
    # ------------------------------------------------------------------
    failpoints.crash_tick_payment(
        world,
        seam="failpoint_after_commit",
        label="restart tick",
    )
    resumed = world.observe("after restart tick (response lost)")

    # 保存された期限から処理が再開している
    assert resumed.job_states[root_id] == "EXPIRED"
    assert resumed.job_states[child_id] == "DONE"
    child_acceptances = [
        a for a in resumed.acceptances if a["job_id"] == child_id
    ]
    assert len(child_acceptances) == 1
    assert child_acceptances[0]["decision"] == "APPROVED"
    # A のプロセスは停止済み。判定は system の期限処理が代替した（第5節）
    assert child_acceptances[0]["decided_by"] == SYSTEM_ID
    assert int(child_acceptances[0]["decided_at_us"]) == restart_now_us
    assert review_due_at_us <= restart_now_us

    # 返金予約と payout 予約がそれぞれ 1 件（二重予約なし）
    refunds = resumed.operations_for(root_id, "refund")
    payouts = resumed.operations_for(child_id, "payout")
    assert len(refunds) == 1 and refunds[0]["amount"] == "90.000000"
    assert len(payouts) == 1 and payouts[0]["amount"] == "10.000000"
    assert payouts[0]["payee_id"] == AGENT_B_ID
    assert refunds[0]["payee_id"] == REQUESTER_ID

    # Receipt は 1 件だけ commit 済み、その PaymentOperation は未更新
    assert len(resumed.receipts) == 1, resumed.receipts
    interrupted_receipt = resumed.receipts[0]
    interrupted_operation_id = str(interrupted_receipt["operation_id"])
    interrupted = next(
        op
        for op in resumed.operations
        if op["operation_id"] == interrupted_operation_id
    )
    assert interrupted["status"] != "SUCCEEDED", interrupted
    assert interrupted["receipt_id"] is None

    # 更新後の同じ時刻を CLI・tick が観測している
    _assert_cli_observes(world, root_id, restart_now_us)
    approve_events = resumed.events_for(child_id, "approve")
    assert len(approve_events) == 1, resumed.events
    assert approve_events[0]["actor_id"] == SYSTEM_ID
    assert int(approve_events[0]["at_us"]) == restart_now_us
    # 失効処理が確定した返金予約の Journal も同じ時刻を記録している
    refund_journal = [e for e in resumed.journal if e["bucket"] == "refund"]
    assert refund_journal, resumed.journal
    assert all(
        int(entry["created_at_us"]) == restart_now_us for entry in refund_journal
    )
    # 期限判定に使った保存値（Lease の expires_at）は監査値として不変
    root_lease = next(
        lease for lease in resumed.leases if lease["id"] == start.root_lease_id
    )
    assert root_lease["closed_reason"] == "expired"
    assert int(root_lease["expires_at_us"]) == root_lease_expires_at_us

    # ------------------------------------------------------------------
    # 5. 再起動 2: Receipt 照会で SUCCEEDED へ収束する（二重送金なし）
    # ------------------------------------------------------------------
    retry = world.run_cli(
        ["payment", "retry", interrupted_operation_id],
        actor=REQUESTER_ID,
        action="payment retry (receipt lookup after restart)",
    )
    assert retry.data["payment_status"] == "SUCCEEDED"
    assert retry.data["receipt_id"] == interrupted_receipt["receipt_id"]
    after_retry = world.observe("after payment retry (receipt lookup)")
    assert len(after_retry.receipts) == 1
    assert after_retry.receipts == resumed.receipts
    reconciled = next(
        op
        for op in after_retry.operations
        if op["operation_id"] == interrupted_operation_id
    )
    assert reconciled["status"] == "SUCCEEDED"
    assert reconciled["receipt_id"] == interrupted_receipt["receipt_id"]

    # ------------------------------------------------------------------
    # 6. 再起動 3・4: 残りの送金を確定し、2 回目では何も増えない
    # ------------------------------------------------------------------
    world.tick_once(label="tick --once (restart 3)")
    settled = world.observe("after restart tick 3")
    assert len(settled.receipts) == 2, settled.receipts
    assert world.pending_payment_count() == 0

    world.tick_once(label="tick --once (restart 4)")
    final = world.observe("after restart tick 4")
    assert final.receipts == settled.receipts
    assert final.acceptances == settled.acceptances
    assert final.operations == settled.operations
    assert final.journal == settled.journal
    assert final.wallet_units == settled.wallet_units

    # 二重裁定なし: Acceptance も Dispute も増えていない
    assert len([a for a in final.acceptances if a["job_id"] == child_id]) == 1
    assert final.disputes == []

    # ------------------------------------------------------------------
    # 7. 最終判定と、MCP が更新後の同じ時刻を観測している証拠
    # ------------------------------------------------------------------
    assert final.job_states[root_id] == "EXPIRED"
    assert final.job_states[child_id] == "DONE"
    assert final.paid_to(AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert final.refunded_to(REQUESTER_ID) == REFUND_UNITS
    assert final.paid_to(AGENT_A_ID) == 0
    assert final.escrow_units == 0
    assert final.available_units == 0
    assert final.locked_units == 0
    assert (
        final.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + CHILD_BUDGET_UNITS
    )
    # baseline は入金後の残高なので、返金 90 だけが戻る
    assert (
        final.wallet_units[REQUESTER_ID]
        == requester_wallet_baseline + REFUND_UNITS
    )
    assert final.submission_worker(child_id) == AGENT_B_ID
    assert harness.check_payee_entitlement(final) == []

    # 再起動後に新しく立ち上げた MCP サーバープロセスも同じ時刻を観測する
    async with harness.agent_session(world, AGENT_B_ID) as agent_b_restarted:
        job_view = await agent_b_restarted.call(
            "ojp_get_job", {"job_id": child_id}
        )
        assert job_view.data["now"] == service.format_timestamp_us(
            world.current_clock_us()
        )
        assert job_view.data["job"]["state"] == "DONE"

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label

    world.report.note(
        "X10: 全 MCP プロセス停止中に共有 Clock を進め、新プロセスが保存された"
        " review_due_at と Lease 期限から過期限分（Root EXPIRED＋返金 90、"
        "Child の system 自動承認＋payout 10）を再開した。送金 commit 後に"
        "プロセスが終了しても Receipt 照会で SUCCEEDED へ収束し、tick を"
        "再実行しても Acceptance・PaymentOperation・Receipt は増えない"
    )
    return {
        "scenario_id": SCENARIO_ID,
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


def _review_due_at(world: harness.E2EWorld, submission_id: str) -> int:
    with world.read_only_connection() as conn:
        row = conn.execute(
            "SELECT review_due_at_us FROM submissions WHERE id = ?",
            (submission_id,),
        ).fetchone()
    assert row is not None, submission_id
    return int(row["review_due_at_us"])


def _assert_cli_observes(
    world: harness.E2EWorld, root_id: str, expected_now_us: int
) -> None:
    """CLI（実プロセス）が更新後の共有 Clock を観測していることを確認する。"""
    result = world.run_cli(
        ["ledger", "show", root_id],
        actor=REQUESTER_ID,
        action="ledger show (clock observation)",
    )
    assert result.data["now"] == service.format_timestamp_us(expected_now_us)
