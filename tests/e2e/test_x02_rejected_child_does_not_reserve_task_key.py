"""X02: child_payout も U に含み、拒否された作成は task_key を確保しない
（計画書 第18節 追加検証 X02）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X02 | max_amount=30、max_ratio_bps=10000、max_children=3、max_depth=1。part-1を20で承認し送金commit前で停止、part-2を20で作成試行後、別operation_idで同じpart-2を10で作成 | 20はPOLICY_LIMIT/MAX_AMOUNT、10は成功。拒否された20の試行はJobもtask_keyも確保しないため、同じpart-2の10は重複扱いにならない。前者のU=20・available=80を維持し、後者はU=30・available=70。child_payoutもUに含む |

policy だけを Root 公開前に差し替えた新しい Root（入金 100）で:

1. A が part-1 を **20** で発注し、B の有効提出を A が承認する。10 ではなく
   20 を使うのは、承認後の `child_payout` だけで max_amount=30 の残りが
   10 しか無い状態を作るため（第11節「N07・X02 等の競合検証では 20 を指定」）
2. part-1 の送金を **commit 前で停止**させる（`failpoint_before_commit` を
   立てた独立プロセスが exit 3 で異常終了する）。Receipt も台帳変化も残らず、
   20 は `child_payout` に留まる
3. この状態で part-2 を **20** で作成しようとすると、U(=child_payout 20) +
   20 > max_amount 30 なので `POLICY_LIMIT`/`MAX_AMOUNT`。**child_payout が
   U に含まれていなければこの拒否は起きない**
4. 拒否された作成は transaction ごと rollback され Job も task_key も確保
   しないため、**別 operation_id で同じ part-2 を 10** で作ると
   `TASK_CONFLICT` にならず成功する
5. U と available は各段階で `ledger show` の `subcontract_usage` と、
   テスト側で独立に導出した「Child 支払い済み + Σ(child_work +
   child_payout)」の両方で照合する（拒否前後で U=20・available=80 を維持、
   成功後は U=30・available=70）
6. 停止していた送金を復旧させても U=30・available=70 は変わらず、B への
   支払いは 1 回だけ（`child_payout` 20 が支払い済み 20 に移るだけ）
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import failpoints, harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

SCENARIO_ID = "X02"

ROOT_BUDGET_UNITS = 100_000_000
PART1_BUDGET = "20.000000"
PART1_BUDGET_UNITS = 20_000_000
PART2_BUDGET = "10.000000"
PART2_BUDGET_UNITS = 10_000_000
CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"

# max_amount=30 / max_ratio_bps=10000 / max_children=3 / max_depth=1（X02 の設定）
SUBCONTRACT_POLICY = {
    "enabled": True,
    "max_amount": "30.000000",
    "max_ratio_bps": 10000,
    "max_children": 3,
    "max_depth": 1,
}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_x02_child_payout_counts_and_rejection_frees_task_key(
    tmp_path: Path,
) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = await _run_x02(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


async def _run_x02(world: harness.E2EWorld) -> dict:
    card_path = harness.write_root_card(
        world, SCENARIO_ID.lower(), subcontract_policy=SUBCONTRACT_POLICY
    )
    root_id, root_version_id = harness.create_and_fund_root(
        world, card_path=card_path
    )
    after_fund = world.observe("after root fund")
    assert after_fund.available_units == ROOT_BUDGET_UNITS
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]
    _assert_usage(world, after_fund, in_use_units=0, available_units=ROOT_BUDGET_UNITS)

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            # ----------------------------------------------------------
            # 1. A が Root を Claim し、part-1 を 20 で発注する
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
            world.observe("after root claim by A")

            create_part1 = await agent_a.call(
                "ojp_create_child_job",
                {
                    "parent_job_id": root_id,
                    "lease_id": root_lease_id,
                    "task_key": "part-1",
                    "budget": PART1_BUDGET,
                    "deadline": CHILD_DEADLINE,
                    "operation_id": world.next_operation_id("create", "a-part-1"),
                },
            )
            part1_id = str(create_part1.data["child_id"])
            part1_version_id = str(create_part1.data["version_id"])
            world.child_ids.append(part1_id)
            after_part1_create = world.observe("after part-1 create (20)")
            assert after_part1_create.locked_for("child_work") == PART1_BUDGET_UNITS
            assert after_part1_create.locked_for("child_payout") == 0
            _assert_usage(
                world,
                after_part1_create,
                in_use_units=PART1_BUDGET_UNITS,
                available_units=ROOT_BUDGET_UNITS - PART1_BUDGET_UNITS,
            )

            # ----------------------------------------------------------
            # 2. B が提出し、A が承認する（child_work 20 → child_payout 20）
            # ----------------------------------------------------------
            claim_part1 = await agent_b.call(
                "ojp_claim_job",
                {
                    "job_id": part1_id,
                    "expected_version_id": part1_version_id,
                    "operation_id": world.next_operation_id("claim", "b-part-1"),
                },
            )
            submit_part1 = await agent_b.call(
                "ojp_submit",
                {
                    "job_id": part1_id,
                    "lease_id": str(claim_part1.data["lease_id"]),
                    "version_id": part1_version_id,
                    "artifact_json": json.dumps({"sum": 6}),
                    "operation_id": world.next_operation_id("submit", "b-part-1"),
                },
            )
            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": part1_id,
                    "submission_id": str(submit_part1.data["submission_id"]),
                    "operation_id": world.next_operation_id("approve", "a-part-1"),
                },
            )

            approved = world.observe("after part-1 approve by A")
            assert approved.job_states[part1_id] == "DONE"
            assert approved.locked_for("child_work") == 0
            assert approved.locked_for("child_payout") == PART1_BUDGET_UNITS
            payouts = approved.operations_for(part1_id, "payout")
            assert len(payouts) == 1
            payment_operation_id = str(payouts[0]["operation_id"])
            assert payouts[0]["status"] == "PENDING"
            assert approved.paid_units == 0

            # ------------------------------------------------------------------
            # 3. part-1 の送金を commit 前で停止させる（Receipt も台帳変化も残らない）
            # ------------------------------------------------------------------
            failpoints.crash_payment(
                world,
                seam="failpoint_before_commit",
                payment_operation_id=payment_operation_id,
                label="part-1 settlement crash before commit",
            )
            stopped = world.observe("after part-1 settlement crash before commit")
            assert stopped.receipts == []
            assert stopped.paid_units == 0
            assert stopped.journal == approved.journal
            assert stopped.wallet_units == approved.wallet_units
            assert stopped.locked_for("child_payout") == PART1_BUDGET_UNITS
            # child_payout だけで U=20・available=80（child_payout も U に含む）
            _assert_usage(
                world,
                stopped,
                in_use_units=PART1_BUDGET_UNITS,
                available_units=ROOT_BUDGET_UNITS - PART1_BUDGET_UNITS,
            )

            # ------------------------------------------------------------------
            # 4. part-2 を 20 で作成 → POLICY_LIMIT/MAX_AMOUNT（U+20 > 30）
            # ------------------------------------------------------------------
            rejected = await agent_a.call(
                "ojp_create_child_job",
                {
                    "parent_job_id": root_id,
                    "lease_id": root_lease_id,
                    "task_key": "part-2",
                    "budget": PART1_BUDGET,
                    "deadline": CHILD_DEADLINE,
                    "operation_id": world.next_operation_id(
                        "create", "a-part-2-rejected"
                    ),
                },
                expect_ok=False,
            )
            assert rejected.error_code == "POLICY_LIMIT", rejected.payload
            details = rejected.payload["error"]["details"]
            assert details is not None and details["reason"] == "MAX_AMOUNT", details

            after_rejection = world.observe("after rejected part-2 create (20)")
            # 拒否された作成は Job も task_key も確保しない
            assert set(after_rejection.job_states) == {root_id, part1_id}
            assert _child_rows_for_task(world, root_id, "part-2") == []
            assert after_rejection.journal == stopped.journal
            assert after_rejection.wallet_units == stopped.wallet_units
            assert after_rejection.receipts == []
            # 前者の U=20・available=80 を維持する
            _assert_usage(
                world,
                after_rejection,
                in_use_units=PART1_BUDGET_UNITS,
                available_units=ROOT_BUDGET_UNITS - PART1_BUDGET_UNITS,
            )

            # ------------------------------------------------------------------
            # 5. 別 operation_id で同じ part-2 を 10 で作成 → 成功（重複扱いにならない）
            # ------------------------------------------------------------------
            create_part2 = await agent_a.call(
                "ojp_create_child_job",
                {
                    "parent_job_id": root_id,
                    "lease_id": root_lease_id,
                    "task_key": "part-2",
                    "budget": PART2_BUDGET,
                    "deadline": CHILD_DEADLINE,
                    "operation_id": world.next_operation_id("create", "a-part-2"),
                },
            )
            part2_id = str(create_part2.data["child_id"])
            world.child_ids.append(part2_id)

    accepted = world.observe("after part-2 create (10)")
    assert accepted.job_states[part2_id] == "OPEN"
    assert accepted.child_budget_units[part2_id] == PART2_BUDGET_UNITS
    assert accepted.locked_for("child_work") == PART2_BUDGET_UNITS
    assert accepted.locked_for("child_payout") == PART1_BUDGET_UNITS
    # 後者は U=30・available=70
    _assert_usage(
        world,
        accepted,
        in_use_units=PART1_BUDGET_UNITS + PART2_BUDGET_UNITS,
        available_units=ROOT_BUDGET_UNITS
        - PART1_BUDGET_UNITS
        - PART2_BUDGET_UNITS,
    )
    # 累計 Child 数は 2（拒否された作成は件数枠も消費しない）
    assert (
        harness.subcontract_usage(world, root_id, label="child count")["child_count"]
        == 2
    )

    # ------------------------------------------------------------------
    # 6. 停止していた送金を復旧させても U=30・available=70 は変わらない
    # ------------------------------------------------------------------
    retry = world.run_cli(
        ["payment", "retry", payment_operation_id],
        actor=REQUESTER_ID,
        action="payment retry (part-1 recovery)",
    )
    assert retry.data["payment_status"] == "SUCCEEDED"
    final = world.observe("after part-1 payment recovery")
    assert final.paid_to(AGENT_B_ID) == PART1_BUDGET_UNITS
    assert len([r for r in final.receipts if r["job_id"] == part1_id]) == 1
    assert (
        final.wallet_units[AGENT_B_ID]
        == agent_b_wallet_baseline + PART1_BUDGET_UNITS
    )
    assert final.locked_for("child_payout") == 0
    assert final.locked_for("child_work") == PART2_BUDGET_UNITS
    # 支払い済み 20 も U に残るため U=30・available=70 のまま
    _assert_usage(
        world,
        final,
        in_use_units=PART1_BUDGET_UNITS + PART2_BUDGET_UNITS,
        available_units=ROOT_BUDGET_UNITS
        - PART1_BUDGET_UNITS
        - PART2_BUDGET_UNITS,
    )
    assert final.submission_worker(part1_id) == AGENT_B_ID
    assert harness.check_payee_entitlement(final) == []

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label

    world.report.note(
        "X02: 送金 commit 前で停止した part-1 の child_payout 20 が U に含まれる"
        "ため part-2 の 20 は POLICY_LIMIT/MAX_AMOUNT。拒否は Job も task_key も"
        "確保しないので同じ part-2 の 10 は TASK_CONFLICT にならず成功し、"
        "U=20/available=80 → U=30/available=70 へ進む"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {
            root_id: "LEASED",
            part1_id: "DONE",
            part2_id: "OPEN",
        },
        "paid": {AGENT_B_ID: "20.000000"},
        "refunded": {},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "10.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


def _assert_usage(
    world: harness.E2EWorld,
    snapshot: harness.Snapshot,
    *,
    in_use_units: int,
    available_units: int,
) -> None:
    """U と available を、アプリの導出とテスト側の独立導出の両方で照合する。

    テスト側の U は「Child への確定 payout の累計 + Σ(child_work +
    child_payout)」として Receipt と locked 内訳から独立に導出する
    （child_payout が U に含まれることをここで固定する）。
    """
    child_paid = sum(
        int(receipt["amount_units"])
        for receipt in snapshot.receipts
        if receipt["kind"] == "payout" and receipt["job_id"] != snapshot.root_id
    )
    derived_in_use = (
        child_paid
        + snapshot.locked_for("child_work")
        + snapshot.locked_for("child_payout")
    )
    assert derived_in_use == in_use_units, snapshot.label
    assert snapshot.available_units == available_units, snapshot.label

    usage = harness.subcontract_usage(world, snapshot.root_id, label=snapshot.label)
    assert harness.parse_amount_string(usage["in_use"]) == in_use_units, usage
    assert harness.parse_amount_string(usage["available"]) == available_units, usage


def _child_rows_for_task(
    world: harness.E2EWorld, root_id: str, task_key: str
) -> list:
    with world.read_only_connection() as conn:
        return conn.execute(
            "SELECT id FROM jobs WHERE parent_id = ? AND task_key = ?",
            (root_id, task_key),
        ).fetchall()
