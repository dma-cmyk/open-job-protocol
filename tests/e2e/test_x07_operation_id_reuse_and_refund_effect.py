"""X07: operation_id 流用の拒否と、業務キーによる一度だけの返金効果
（計画書 第18節 追加検証 X07）。

| ID | シナリオ | 期待結果 |
|---|---|---|
| X07 | 同一operation_idに別金額・別Actor、異なるIDで同じ返金効果 | ID流用拒否、業務キーでも効果は一度だけ |

第16節「二段階の一意性」を実プロセスで観測する:

1. **再送 ID**（`operation_id` UNIQUE ＋ actor/kind/payload_hash 照合）
   - 同一 operation_id・**別金額**（`child create` の budget を 10 → 20）は
     `IDEMPOTENCY_CONFLICT`
   - 同一 operation_id・**別 Actor**（A → B、Requester → system）は
     `IDEMPOTENCY_CONFLICT`
   - 同一 operation_id・**別対象**（`job refund` の対象を Root → Child）も
     `IDEMPOTENCY_CONFLICT`（対象違反 `INVALID_TARGET` より ID 照合が先）
   - 同一 operation_id・同一 Actor・同一 payload の再送だけが `replayed=true`
     で保存済み結果を返す
2. **業務上一意な効果**（UNIQUE business_key）
   - Parent 失敗で作られた `refund:{root}:terminal`（90）を、**異なる
     operation_id の `job refund ROOT` で何度呼んでも**返金は 1 回だけ
   - その後の Child 失敗で `refund:{root}:child-return:{child}`（10）が
     加わり、返金累計は 100。Receipt も返金予約も各 1 件のまま
   - `job refund` は**新しい返金予約を作らない**（第14節）

Requester の Wallet は合計 100 だけ戻り、受取権者（Root Requester）以外へは
1 単位も流れない。
"""

from __future__ import annotations

from pathlib import Path

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID, SYSTEM_ID

SCENARIO_ID = "X07"

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET = "10.000000"
CHILD_BUDGET_UNITS = 10_000_000
FIRST_REFUND_UNITS = 90_000_000
CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"


def test_x07_operation_id_reuse_is_rejected_and_refund_applies_once(
    tmp_path: Path,
) -> None:
    world = harness.create_world(tmp_path, SCENARIO_ID)
    failed = False
    try:
        expected = _run_x07(world)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


def _run_x07(world: harness.E2EWorld) -> dict:
    root_id, root_version_id = harness.create_and_fund_root(world)
    after_fund = world.observe("after root fund")
    requester_wallet_baseline = after_fund.wallet_units[REQUESTER_ID]
    agent_a_wallet_baseline = after_fund.wallet_units[AGENT_A_ID]
    agent_b_wallet_baseline = after_fund.wallet_units[AGENT_B_ID]

    claim_root = world.run_cli(
        ["job", "claim", root_id, "--version", root_version_id],
        actor=AGENT_A_ID,
        operation_id=world.next_operation_id("claim", "a-root"),
        action="job claim (root)",
    )
    root_lease_id = str(claim_root.data["lease_id"])
    world.observe("after root claim by A")

    # ------------------------------------------------------------------
    # 1. 再送 ID: Child 作成の operation_id を別金額・別 Actor で流用する
    # ------------------------------------------------------------------
    create_operation_id = world.next_operation_id("create", "a-part-1")

    def child_create_args(budget: str) -> list[str]:
        return [
            "child",
            "create",
            root_id,
            "--lease",
            root_lease_id,
            "--task",
            "part-1",
            "--budget",
            budget,
            "--deadline",
            CHILD_DEADLINE,
        ]

    created = world.run_cli(
        child_create_args(CHILD_BUDGET),
        actor=AGENT_A_ID,
        operation_id=create_operation_id,
        action="child create (part-1)",
    )
    child_id = str(created.data["child_id"])
    child_version_id = str(created.data["version_id"])
    world.child_ids.append(child_id)
    after_create = world.observe("after part-1 create")
    assert after_create.locked_for("child_work") == CHILD_BUDGET_UNITS

    # 別金額（10 → 20）での流用
    other_amount = world.run_cli(
        child_create_args("20.000000"),
        actor=AGENT_A_ID,
        operation_id=create_operation_id,
        expect_ok=False,
        action="child create (same operation_id, other amount)",
    )
    assert other_amount.returncode == 2, other_amount.stdout
    assert other_amount.error_code == "IDEMPOTENCY_CONFLICT", other_amount.payload

    # 別 Actor（A → B）での流用
    other_actor = world.run_cli(
        child_create_args(CHILD_BUDGET),
        actor=AGENT_B_ID,
        operation_id=create_operation_id,
        expect_ok=False,
        action="child create (same operation_id, other actor)",
    )
    assert other_actor.returncode == 2, other_actor.stdout
    assert other_actor.error_code == "IDEMPOTENCY_CONFLICT", other_actor.payload

    # 同一 Actor・同一 payload の再送だけが replay になる
    replay = world.run_cli(
        child_create_args(CHILD_BUDGET),
        actor=AGENT_A_ID,
        operation_id=create_operation_id,
        action="child create (same operation_id replay)",
    )
    assert replay.payload is not None and replay.payload["replayed"] is True
    assert replay.data == created.data

    after_reuse = world.observe("after create operation_id reuse attempts")
    _assert_no_fund_movement(after_create, after_reuse)
    assert set(after_reuse.job_states) == {root_id, child_id}
    assert after_reuse.locked_for("child_work") == CHILD_BUDGET_UNITS
    assert after_reuse.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS

    # ------------------------------------------------------------------
    # 2. Parent 失敗で refund:{root}:terminal（90）が確定する
    # ------------------------------------------------------------------
    claim_child = world.run_cli(
        ["job", "claim", child_id, "--version", child_version_id],
        actor=AGENT_B_ID,
        operation_id=world.next_operation_id("claim", "b-part-1"),
        action="job claim (part-1)",
    )
    child_lease_id = str(claim_child.data["lease_id"])
    world.observe("after part-1 claim by B")

    world.run_cli(
        ["job", "abandon", root_id, "--lease", root_lease_id],
        actor=AGENT_A_ID,
        operation_id=world.next_operation_id("abandon", "a-root"),
        action="job abandon (root)",
    )
    after_parent_fail = world.observe("after root abandon by A")
    assert after_parent_fail.job_states[root_id] == "FAILED"
    assert after_parent_fail.locked_for("refund") == FIRST_REFUND_UNITS
    assert after_parent_fail.locked_for("child_work") == CHILD_BUDGET_UNITS
    terminal_key = f"refund:{root_id}:terminal"
    assert _refund_business_keys(world, root_id) == [terminal_key]

    # ------------------------------------------------------------------
    # 3. 異なる operation_id で同じ返金効果を要求しても 1 回だけ
    # ------------------------------------------------------------------
    refund_operation_id = world.next_operation_id("refund", "r-root-1")
    first_refund = world.run_cli(
        ["job", "refund", root_id],
        actor=REQUESTER_ID,
        operation_id=refund_operation_id,
        action="job refund (first)",
    )
    assert first_refund.data["refund_count"] == 1
    after_first_refund = world.observe("after first refund")
    assert after_first_refund.refunded_to(REQUESTER_ID) == FIRST_REFUND_UNITS
    assert (
        after_first_refund.wallet_units[REQUESTER_ID]
        == requester_wallet_baseline + FIRST_REFUND_UNITS
    )
    assert len(after_first_refund.receipts) == 1

    # 別 operation_id での再要求 → 新しい送金も新しい予約も作らない
    again = world.run_cli(
        ["job", "refund", root_id],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("refund", "r-root-2"),
        action="job refund (different operation_id)",
    )
    assert again.data["refund_count"] == 1
    assert again.data["refunds"][0]["already_succeeded"] is True
    after_again = world.observe("after refund with a different operation_id")
    _assert_no_fund_movement(after_first_refund, after_again)
    assert _refund_business_keys(world, root_id) == [terminal_key]

    # 同一 operation_id の再送は replay
    replayed_refund = world.run_cli(
        ["job", "refund", root_id],
        actor=REQUESTER_ID,
        operation_id=refund_operation_id,
        action="job refund (same operation_id replay)",
    )
    assert replayed_refund.payload is not None
    assert replayed_refund.payload["replayed"] is True

    # 同一 operation_id・別 Actor（Requester → system）
    refund_other_actor = world.run_cli(
        ["job", "refund", root_id],
        actor=SYSTEM_ID,
        operation_id=refund_operation_id,
        expect_ok=False,
        action="job refund (same operation_id, other actor)",
    )
    assert refund_other_actor.returncode == 2, refund_other_actor.stdout
    assert refund_other_actor.error_code == "IDEMPOTENCY_CONFLICT", (
        refund_other_actor.payload
    )

    # 同一 operation_id・別対象（Root → Child）。ID 照合が INVALID_TARGET より先
    refund_other_target = world.run_cli(
        ["job", "refund", child_id],
        actor=REQUESTER_ID,
        operation_id=refund_operation_id,
        expect_ok=False,
        action="job refund (same operation_id, other target)",
    )
    assert refund_other_target.returncode == 2, refund_other_target.stdout
    assert refund_other_target.error_code == "IDEMPOTENCY_CONFLICT", (
        refund_other_target.payload
    )

    after_refund_reuse = world.observe("after refund operation_id reuse attempts")
    _assert_no_fund_movement(after_first_refund, after_refund_reuse)
    assert after_refund_reuse.refunded_to(REQUESTER_ID) == FIRST_REFUND_UNITS

    # ------------------------------------------------------------------
    # 4. Child 失敗で追加返金 10 が加わり、累計 100 で止まる
    # ------------------------------------------------------------------
    world.run_cli(
        ["job", "abandon", child_id, "--lease", child_lease_id],
        actor=AGENT_B_ID,
        operation_id=world.next_operation_id("abandon", "b-part-1"),
        action="job abandon (part-1)",
    )
    after_child_fail = world.observe("after part-1 abandon by B")
    assert after_child_fail.job_states[child_id] == "FAILED"
    child_return_key = f"refund:{root_id}:child-return:{child_id}"
    assert _refund_business_keys(world, root_id) == sorted(
        [terminal_key, child_return_key]
    )

    for label in ("r-root-3", "r-root-4"):
        processed = world.run_cli(
            ["job", "refund", root_id],
            actor=REQUESTER_ID,
            operation_id=world.next_operation_id("refund", label),
            action=f"job refund ({label})",
        )
        assert processed.data["refund_count"] == 2
        snapshot = world.observe(f"after refund {label}")
        assert snapshot.refunded_to(REQUESTER_ID) == ROOT_BUDGET_UNITS
        assert snapshot.refunded_units == ROOT_BUDGET_UNITS
        assert len(snapshot.receipts) == 2

    final = world.observe("after refund effect settles")
    # 業務キーごとに 1 件だけ。返金累計は 100 で止まる
    assert _refund_business_keys(world, root_id) == sorted(
        [terminal_key, child_return_key]
    )
    assert sorted(
        op["amount"] for op in final.operations if op["kind"] == "refund"
    ) == ["10.000000", "90.000000"]
    assert all(
        op["status"] == "SUCCEEDED"
        for op in final.operations
        if op["kind"] == "refund"
    )
    assert final.refunded_to(REQUESTER_ID) == ROOT_BUDGET_UNITS
    assert final.refunded_by_payee_units == {REQUESTER_ID: ROOT_BUDGET_UNITS}
    assert (
        final.wallet_units[REQUESTER_ID]
        == requester_wallet_baseline + ROOT_BUDGET_UNITS
    )
    assert final.paid_units == 0
    assert final.wallet_units[AGENT_A_ID] == agent_a_wallet_baseline
    assert final.wallet_units[AGENT_B_ID] == agent_b_wallet_baseline
    assert final.escrow_units == 0
    assert final.available_units == 0
    assert final.locked_units == 0
    assert harness.check_payee_entitlement(final) == []

    # 独立 tick を回しても返金は増えない
    world.tick_once(label="tick --once (after refunds)")
    after_tick = world.observe("after tick following refunds")
    _assert_no_fund_movement(final, after_tick)

    for snap in world.snapshots:
        assert harness.check_conservation(snap) == [], snap.label
        assert harness.check_no_double_counting(snap) == [], snap.label

    world.report.note(
        "X07: operation_id の別金額・別 Actor・別対象での流用はすべて"
        " IDEMPOTENCY_CONFLICT。異なる operation_id で同じ返金効果を"
        "何度要求しても business_key（refund:{root}:terminal /"
        " refund:{root}:child-return:{child}）ごとに 1 回だけ実行され、"
        "返金累計は 100 で止まる"
    )
    return {
        "scenario_id": SCENARIO_ID,
        "terminal_job_states": {root_id: "FAILED", child_id: "FAILED"},
        "paid": {},
        "refunded": {REQUESTER_ID: "100.000000"},
        "locked_breakdown": {
            "child_payout": "0.000000",
            "child_work": "0.000000",
            "parent_payout": "0.000000",
            "refund": "0.000000",
        },
    }


def _refund_business_keys(world: harness.E2EWorld, root_id: str) -> list[str]:
    """Root の返金 PaymentOperation の business_key 一覧（重複がないことの正本）。"""
    with world.read_only_connection() as conn:
        rows = conn.execute(
            "SELECT business_key FROM payment_operations"
            " WHERE root_id = ? AND kind = 'refund' ORDER BY business_key",
            (root_id,),
        ).fetchall()
    return [str(row["business_key"]) for row in rows]


def _assert_no_fund_movement(
    before: harness.Snapshot, after: harness.Snapshot
) -> None:
    """口座・Journal・Wallet・PaymentOperation・Receipt が不変。"""
    assert after.accounts == before.accounts
    assert after.journal == before.journal
    assert after.wallet_units == before.wallet_units
    assert after.operations == before.operations
    assert after.receipts == before.receipts
    assert after.deposit_units == before.deposit_units
    assert after.available_units == before.available_units
    assert after.locked_breakdown_units == before.locked_breakdown_units
    assert after.paid_units == before.paid_units
    assert after.refunded_units == before.refunded_units
