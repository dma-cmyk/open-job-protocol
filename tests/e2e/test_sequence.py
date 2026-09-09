"""Sequence: 固定 seed の生成操作列と参照会計モデルの照合（計画書 第17節）。

| 層 | 検証対象 | 手法 |
|---|---|---|
| Sequence | 複数Child、失敗・再試行・期限の順序組合せ | 固定seedの生成操作列を小さな参照会計モデルと比較。各commit後に保存則と権限不変条件を確認 |

固定 seed から**操作列そのもの**を生成し、それを実プロセス（Human CLI・A/B の
MCP stdio・独立 tick）へ流しながら、同じ操作列を Python 内のインメモリ会計
状態機（`ReferenceLedger`）へ適用する。両者を**各 commit の直後**に突き合わせる。

- 生成される操作列は「複数 Child・失敗（提出前 abandon）・期限（Lease 失効）・
  再試行（失敗した task_key の再発注）・Parent 終端と Child 判定の順序」の
  組合せになる。生成器は参照モデルの `can_create_child`（第10節の
  `U + 新Child予算 <= L` / `<= available` / 累計件数 / task_key 生存）だけを
  使って合法な操作だけを選ぶので、拒否コードの検証（N07・X02・X05・X13）とは
  役割を分ける。
- 参照モデルは第9節「100→10の操作一覧」と第8節「Parent終了時」の表だけを
  写したもので、実装のコードには一切依存しない。各 commit 後に
  deposit / available / locked 4 口座 / 受取人別 paid / 受取人別 refunded /
  全 Job 状態を完全一致で照合する。第10節の U と累計 Child 数は
  `ojp ledger show` の `subcontract_usage` と照合する。
- **commit 単位の観測**: 送金は `ojp payment retry OPERATION`（第14節）で
  1 件ずつ確定させるので、送金 commit ごとに観測と照合が入る。tick が複数
  commit に分かれる期限処理では、想定した commit 件数だけが起きたことを
  固定したうえで、`observe()` が呼ぶ `harness.check_commit_history` が
  `journal_transactions` の commit 列を再生して**中間 commit も含めた全
  commit 直後**の保存則・非負・受取権者一致・親子二重計上なしを検査する。
  結果は `conservation_ok` / `no_double_counting_ok` / `commits_ok` として
  レポートへ残る。

seed を固定しているため操作列は再現可能で、`test_sequence_plans_are_reproducible`
が同じ seed から同じ操作列が出ることと、採用した seed 群が Parent 成功・失敗の
両方を含むことを固定する。
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from . import harness, report
from .harness import AGENT_A_ID, AGENT_B_ID, REQUESTER_ID

ROOT_BUDGET_UNITS = 100_000_000
CHILD_BUDGET_UNITS = 10_000_000
CHILD_BUDGET = harness.format_amount_units(CHILD_BUDGET_UNITS)
CHILD_DEADLINE = "2027-01-16T00:00:00.000000Z"

# 第11節の固定カタログ（task_key・入力・期待 JSON は公開後変更できない）。
TASK_KEYS = ("part-1", "part-2", "part-3")
TASK_EXPECTED_SUM = {"part-1": 6, "part-2": 4, "part-3": 5}
ROOT_EXPECTED_SUM = 15

# 再試行（失敗した task_key の再発注）を複数回観測するため、累計件数枠だけを
# カタログの 3 件より広く取る。金額・比率は第10節の例（L=30）のまま。
MAX_AMOUNT_UNITS = 30_000_000
MAX_RATIO_BPS = 3000
MAX_CHILDREN = 6
SUBCONTRACT_POLICY = {
    "enabled": True,
    "max_amount": harness.format_amount_units(MAX_AMOUNT_UNITS),
    "max_ratio_bps": MAX_RATIO_BPS,
    "max_children": MAX_CHILDREN,
    "max_depth": 1,
}

FAILURE_STATES = frozenset({"FAILED", "EXPIRED"})

# Child の判定結果（生成器が選ぶ操作列の要素）
SUCCESS = "success"
ABANDON = "abandon"
LEASE_EXPIRY = "lease_expiry"
CHILD_OUTCOMES = (SUCCESS, ABANDON, LEASE_EXPIRY)

PARENT_SUCCESS = "success"
PARENT_FAIL = "fail"

# 採用する固定 seed。Parent 成功・失敗の両方を含むことを
# test_sequence_plans_are_reproducible が固定する。
SEEDS = (20260901, 20260902, 20260903)

_MAX_PLAN_DRAWS = 200
_MAX_SETTLE_ROUNDS = 10
_MAX_CLOCK_STEPS = 20


# ---------------------------------------------------------------------------
# 参照会計モデル（第9節・第8節・第10節の表だけを写したもの）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _PendingTransfer:
    """予約済みで未確定の資金移動（PaymentOperation 相当）。"""

    bucket: str
    payee_id: str
    amount_units: int
    is_child_payout: bool


@dataclass
class ReferenceLedger:
    """小さなインメモリ会計状態機。

    残高口座は `available` と locked の 4 口座だけ。`paid` / `refunded` は
    残高ではなく確定済み資金移動の累計で、送金処理（tick）が走るまで増えない
    （第9節「APPROVEDやDONEになっただけではpaidを増やさない」）。
    """

    root_id: str
    requester_id: str
    max_amount_units: int
    max_ratio_bps: int
    max_children: int
    deposit_units: int = 0
    available_units: int = 0
    locked_units: dict[str, int] = field(
        default_factory=lambda: {
            "child_work": 0,
            "child_payout": 0,
            "parent_payout": 0,
            "refund": 0,
        }
    )
    paid_units: dict[str, int] = field(default_factory=dict)
    refunded_units: dict[str, int] = field(default_factory=dict)
    pending: list[_PendingTransfer] = field(default_factory=list)
    states: dict[str, str] = field(default_factory=dict)
    child_task_keys: dict[str, str] = field(default_factory=dict)
    child_budget_units: dict[str, int] = field(default_factory=dict)
    child_created_count: int = 0
    child_paid_units: int = 0

    # -- 導出値 -----------------------------------------------------------

    @property
    def escrow_units(self) -> int:
        return self.available_units + sum(self.locked_units.values())

    @property
    def in_use_units(self) -> int:
        """U = Child支払い済み総額 + Σ(child_work + child_payout)（第10節）。"""
        return (
            self.child_paid_units
            + self.locked_units["child_work"]
            + self.locked_units["child_payout"]
        )

    @property
    def limit_units(self) -> int:
        """L = min(max_amount_units, floor(D × max_ratio_bps / 10000))（第10節）。"""
        return min(
            self.max_amount_units,
            self.deposit_units * self.max_ratio_bps // 10_000,
        )

    def task_key_is_free(self, task_key: str) -> bool:
        """同じ task_key に生存中・成功済み Child がないこと（第8節）。"""
        return all(
            self.states[job_id] in FAILURE_STATES
            for job_id, key in self.child_task_keys.items()
            if key == task_key
        )

    def can_create_child(self, task_key: str, budget_units: int) -> bool:
        """第10節の作成可能条件（depth=0・enabled=true は fixture 側で固定）。"""
        return (
            self.states.get(self.root_id) == "LEASED"
            and self.task_key_is_free(task_key)
            and self.child_created_count + 1 <= self.max_children
            and self.in_use_units + budget_units <= self.limit_units
            and budget_units <= self.available_units
        )

    # -- 遷移 -------------------------------------------------------------

    def fund(self, amount_units: int) -> None:
        """Root fund: Escrow available +D（第9節）。Root は OPEN になる。"""
        self.deposit_units += amount_units
        self.available_units += amount_units
        self.states[self.root_id] = "OPEN"

    def claim(self, job_id: str) -> None:
        """Claim: 資金移動なし（第9節）。"""
        self.states[job_id] = "LEASED"

    def submit(self, job_id: str) -> None:
        """有効提出: 資金移動なし。検収タイマーだけが動く。"""
        self.states[job_id] = "SUBMITTED"

    def create_child(self, job_id: str, task_key: str, budget_units: int) -> None:
        """Child create: available -b / child_work +b（第9節）。"""
        self.available_units -= budget_units
        self.locked_units["child_work"] += budget_units
        self.child_created_count += 1
        self.child_task_keys[job_id] = task_key
        self.child_budget_units[job_id] = budget_units
        self.states[job_id] = "OPEN"

    def approve_child(self, job_id: str, payee_id: str) -> None:
        """Child approval: child_work -b / child_payout +b。送金はまだない。"""
        budget_units = self.child_budget_units[job_id]
        self.locked_units["child_work"] -= budget_units
        self.locked_units["child_payout"] += budget_units
        self.states[job_id] = "DONE"
        self.pending.append(
            _PendingTransfer("child_payout", payee_id, budget_units, True)
        )

    def fail_child(self, job_id: str, state: str) -> None:
        """Child failure。Parent 終端後は同じ transaction で追加返金へ移す。

        - Parent 生存中: child_work -b / available +b（第9節）
        - Parent 終端後: child_work -b / available ±0 / refund +b（第8節・第9節）
        """
        budget_units = self.child_budget_units[job_id]
        self.locked_units["child_work"] -= budget_units
        if self.states[self.root_id] in FAILURE_STATES:
            self.locked_units["refund"] += budget_units
            self.pending.append(
                _PendingTransfer("refund", self.requester_id, budget_units, False)
            )
        else:
            self.available_units += budget_units
        self.states[job_id] = state

    def approve_root(self, payee_id: str) -> None:
        """Parent approval: available -R / parent_payout +R（第9節）。"""
        amount_units = self.available_units
        self.available_units = 0
        self.locked_units["parent_payout"] += amount_units
        self.states[self.root_id] = "DONE"
        if amount_units:
            self.pending.append(
                _PendingTransfer("parent_payout", payee_id, amount_units, False)
            )

    def fail_root(self, state: str) -> None:
        """Parent failure: 未拘束額だけを available -R / refund +R（第9節）。"""
        amount_units = self.available_units
        self.available_units = 0
        self.locked_units["refund"] += amount_units
        self.states[self.root_id] = state
        if amount_units:
            self.pending.append(
                _PendingTransfer("refund", self.requester_id, amount_units, False)
            )

    def _apply_transfer(self, transfer: _PendingTransfer) -> None:
        self.locked_units[transfer.bucket] -= transfer.amount_units
        totals = (
            self.refunded_units if transfer.bucket == "refund" else self.paid_units
        )
        totals[transfer.payee_id] = (
            totals.get(transfer.payee_id, 0) + transfer.amount_units
        )
        if transfer.is_child_payout:
            self.child_paid_units += transfer.amount_units

    def settle_operation(
        self, *, job_id: str, kind: str, payee_id: str, amount_units: int
    ) -> None:
        """確定した送金 1 件（commit 1 件ぶん）を反映する。

        予約していない受取人・金額・原資の送金が現れたらその場で失敗させる。
        """
        if kind == "refund":
            bucket = "refund"
        elif job_id == self.root_id:
            bucket = "parent_payout"
        else:
            bucket = "child_payout"
        wanted = (bucket, payee_id, amount_units)
        for index, transfer in enumerate(self.pending):
            if (
                transfer.bucket,
                transfer.payee_id,
                transfer.amount_units,
            ) == wanted:
                self._apply_transfer(self.pending.pop(index))
                return
        raise AssertionError(
            f"予約していない送金が確定した: job={job_id} kind={kind}"
            f" payee={payee_id} amount={amount_units} pending={self.pending}"
        )

    def settle_pending(self) -> None:
        """予約済みの送金をすべて確定させる（独立 tick 1 回ぶん）。"""
        for transfer in self.pending:
            self._apply_transfer(transfer)
        self.pending.clear()

    # -- 照合 -------------------------------------------------------------

    def assert_matches(self, snapshot: harness.Snapshot) -> None:
        """実プロセスから観測した状態が参照モデルと完全一致することを検証する。"""
        where = snapshot.label
        assert snapshot.deposit_units == self.deposit_units, where
        assert snapshot.available_units == self.available_units, (
            f"{where}: available"
            f" observed={snapshot.available_units} model={self.available_units}"
        )
        assert snapshot.locked_breakdown_units == self.locked_units, (
            f"{where}: locked_breakdown"
            f" observed={snapshot.locked_breakdown_units} model={self.locked_units}"
        )
        assert snapshot.locked_units == sum(self.locked_units.values()), where
        assert snapshot.escrow_units == self.escrow_units, where
        assert snapshot.paid_by_payee_units == _nonzero(self.paid_units), (
            f"{where}: paid"
            f" observed={snapshot.paid_by_payee_units} model={self.paid_units}"
        )
        assert snapshot.refunded_by_payee_units == _nonzero(self.refunded_units), (
            f"{where}: refunded"
            f" observed={snapshot.refunded_by_payee_units}"
            f" model={self.refunded_units}"
        )
        assert snapshot.job_states == self.states, (
            f"{where}: job states"
            f" observed={snapshot.job_states} model={self.states}"
        )


def _nonzero(totals: dict[str, int]) -> dict[str, int]:
    return {payee: units for payee, units in totals.items() if units}


def _new_reference_ledger(root_id: str) -> ReferenceLedger:
    return ReferenceLedger(
        root_id=root_id,
        requester_id=REQUESTER_ID,
        max_amount_units=MAX_AMOUNT_UNITS,
        max_ratio_bps=MAX_RATIO_BPS,
        max_children=MAX_CHILDREN,
    )


def _apply_lease_expiry(model: ReferenceLedger, child_id: str) -> None:
    """独立 tick 1 回分の効果。

    `scheduler.tick_once` は失効処理 → 自動承認 → 裁定 → 返金予約 → 送金処理を
    同じ 1 回で行うため、Lease 失効で作られた予約はその tick で確定する。
    """
    model.fail_child(child_id, "EXPIRED")
    model.settle_pending()


# ---------------------------------------------------------------------------
# 固定 seed からの操作列生成
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ChildStep:
    """1 件の Child 発注とその判定までの操作。"""

    task_key: str
    outcome: str
    # tail（Parent 終端を跨ぐ Child）で、Parent 終端より前に Claim するか
    claim_before_parent_terminal: bool
    # 検収の操作面（A の MCP tool か Human CLI か）
    approve_via_cli: bool
    # 判定直後に送金まで進めるか（false なら child_payout を拘束したまま次へ）
    settle_after: bool


@dataclass(frozen=True)
class SequencePlan:
    """固定 seed から決まる操作列。"""

    seed: int
    pre_parent: tuple[ChildStep, ...]
    parent_outcome: str
    tail: tuple[ChildStep, ...]
    settle_after_parent_terminal: bool

    @property
    def signature(self) -> str:
        parts = [f"{step.task_key}:{step.outcome}" for step in self.pre_parent]
        parts.append(f"parent:{self.parent_outcome}")
        parts += [
            f"tail:{step.task_key}:{step.outcome}:"
            f"{'pre' if step.claim_before_parent_terminal else 'post'}claim"
            for step in self.tail
        ]
        return " ".join(parts)


def generate_plan(seed: int) -> SequencePlan:
    """固定 seed から、複数 Child・失敗・再試行・期限を含む操作列を生成する。

    生成候補は参照モデルの `can_create_child` を通ったものだけで、さらに
    「Child 成功と失敗の両方を含む」「Lease 失効を 1 件以上含む」
    「Parent 失敗なら Parent 終端を跨ぐ Child を持つ」を満たすまで引き直す。
    """
    rng = random.Random(seed)
    for _ in range(_MAX_PLAN_DRAWS):
        plan = _draw_plan(seed, rng)
        if _is_covering(plan):
            return plan
    raise AssertionError(f"seed={seed} から条件を満たす操作列を生成できなかった")


def _draw_plan(seed: int, rng: random.Random) -> SequencePlan:
    model = _new_reference_ledger("plan-root")
    model.fund(ROOT_BUDGET_UNITS)
    model.claim("plan-root")

    pre_parent: list[ChildStep] = []
    for index in range(rng.randint(2, 4)):
        step = _draw_child_step(rng, model, allow_expiry=True, tail=False)
        if step is None:
            break
        job_id = f"plan-pre-{index}"
        model.create_child(job_id, step.task_key, CHILD_BUDGET_UNITS)
        model.claim(job_id)
        if step.outcome == SUCCESS:
            model.submit(job_id)
            model.approve_child(job_id, AGENT_B_ID)
        elif step.outcome == ABANDON:
            model.fail_child(job_id, "FAILED")
        else:
            _apply_lease_expiry(model, job_id)
        if step.settle_after:
            model.settle_pending()
        pre_parent.append(step)

    parent_outcome = rng.choice((PARENT_SUCCESS, PARENT_FAIL))
    tail: list[ChildStep] = []
    if parent_outcome == PARENT_FAIL:
        tail_count = rng.randint(1, 2)
        for index in range(tail_count):
            # Lease 失効は共有 Clock を進めるので、他の tail Child が生存して
            # いる間には置かない（最後の 1 件だけに許す）。
            step = _draw_child_step(
                rng, model, allow_expiry=index == tail_count - 1, tail=True
            )
            if step is None:
                break
            job_id = f"plan-tail-{index}"
            model.create_child(job_id, step.task_key, CHILD_BUDGET_UNITS)
            if step.claim_before_parent_terminal:
                model.claim(job_id)
            tail.append(step)

    return SequencePlan(
        seed=seed,
        pre_parent=tuple(pre_parent),
        parent_outcome=parent_outcome,
        tail=tuple(tail),
        settle_after_parent_terminal=rng.choice((True, False)),
    )


def _draw_child_step(
    rng: random.Random,
    model: ReferenceLedger,
    *,
    allow_expiry: bool,
    tail: bool,
) -> ChildStep | None:
    task_keys = [
        task_key
        for task_key in TASK_KEYS
        if model.can_create_child(task_key, CHILD_BUDGET_UNITS)
    ]
    if not task_keys:
        return None
    outcomes = CHILD_OUTCOMES if allow_expiry else (SUCCESS, ABANDON)
    return ChildStep(
        task_key=rng.choice(task_keys),
        outcome=rng.choice(outcomes),
        claim_before_parent_terminal=rng.choice((True, False)) if tail else True,
        approve_via_cli=rng.choice((True, False)),
        settle_after=rng.choice((True, False)),
    )


def _is_covering(plan: SequencePlan) -> bool:
    outcomes = [step.outcome for step in plan.pre_parent + plan.tail]
    return (
        len(plan.pre_parent) >= 2
        and SUCCESS in outcomes
        and any(outcome != SUCCESS for outcome in outcomes)
        and LEASE_EXPIRY in outcomes
        and (plan.parent_outcome == PARENT_SUCCESS or len(plan.tail) >= 1)
    )


# ---------------------------------------------------------------------------
# テスト
# ---------------------------------------------------------------------------


def test_sequence_plans_are_reproducible() -> None:
    """同じ seed は同じ操作列を出し、採用 seed 群は Parent 成功・失敗を含む。"""
    plans = [generate_plan(seed) for seed in SEEDS]
    assert [plan.signature for plan in plans] == [
        generate_plan(seed).signature for seed in SEEDS
    ]
    assert len({plan.signature for plan in plans}) == len(SEEDS)
    assert {plan.parent_outcome for plan in plans} == {PARENT_SUCCESS, PARENT_FAIL}
    # 再試行（同じ task_key の再発注）が少なくとも 1 つの seed で起きている
    assert any(
        len([step.task_key for step in plan.pre_parent + plan.tail])
        > len({step.task_key for step in plan.pre_parent + plan.tail})
        for plan in plans
    )


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
@pytest.mark.parametrize("seed", SEEDS)
async def test_sequence_matches_reference_accounting_model(
    tmp_path: Path, seed: int
) -> None:
    """固定 seed の操作列を実プロセスへ流し、各 commit で参照モデルと一致する。"""
    plan = generate_plan(seed)
    world = harness.create_world(tmp_path, f"SEQ-{seed}")
    failed = False
    try:
        expected = await _run_sequence(world, plan)
    except BaseException:
        failed = True
        raise
    finally:
        report_path = world.report.write()
        assert report_path.exists()
    if not failed:
        report.assert_report_contents(report_path, **expected)


# ---------------------------------------------------------------------------
# 操作列の実行
# ---------------------------------------------------------------------------


async def _run_sequence(world: harness.E2EWorld, plan: SequencePlan) -> dict:
    card_path = harness.write_root_card(
        world, f"seq-{plan.seed}", subcontract_policy=SUBCONTRACT_POLICY
    )
    root_id, root_version_id = harness.create_and_fund_root(
        world, card_path=card_path
    )
    model = _new_reference_ledger(root_id)
    model.fund(ROOT_BUDGET_UNITS)
    model.assert_matches(world.observe("after root fund"))
    _assert_usage(world, model, root_id, label="after root fund")

    async with harness.agent_session(world, AGENT_A_ID) as agent_a:
        async with harness.agent_session(world, AGENT_B_ID) as agent_b:
            claimed = await agent_a.call(
                "ojp_claim_job",
                {
                    "job_id": root_id,
                    "expected_version_id": root_version_id,
                    "operation_id": world.next_operation_id("claim", "a-root"),
                },
            )
            root_lease_id = str(claimed.data["lease_id"])
            model.claim(root_id)
            model.assert_matches(world.observe("after root claim by A"))

            for index, step in enumerate(plan.pre_parent, start=1):
                label = f"pre-{index}"
                child_id, version_id = await _create_child(
                    world, model, agent_a, root_id, root_lease_id, step, label
                )
                lease_id = await _claim_child(
                    world, model, agent_b, child_id, version_id, label
                )
                await _resolve_child(
                    world,
                    model,
                    agent_a,
                    agent_b,
                    root_id=root_id,
                    root_lease_id=root_lease_id,
                    child_id=child_id,
                    version_id=version_id,
                    lease_id=lease_id,
                    step=step,
                    label=label,
                )

            if plan.parent_outcome == PARENT_SUCCESS:
                await _finish_parent_success(
                    world, model, agent_a, root_id, root_version_id, root_lease_id
                )
            else:
                await _finish_parent_failure(
                    world,
                    model,
                    agent_a,
                    agent_b,
                    root_id=root_id,
                    root_lease_id=root_lease_id,
                    plan=plan,
                )

    _settle(world, model, "final settlement")
    # 全件解決後は独立 tick を回しても新しい commit が 1 件も起きない
    settled = world.observe("after final settlement")
    model.assert_matches(settled)
    world.tick_once(label="tick --once (quiescence check)")
    final = world.observe("final")
    model.assert_matches(final)
    assert _commit_count(final) == _commit_count(settled), (
        "全件解決後の tick が新しい会計 commit を作っている"
    )
    assert world.pending_payment_count() == 0
    _assert_usage(world, model, root_id, label="final")

    # 決済完了後の判定（第17節「4結果は決済完了後に判定する」と同じ観測点）
    assert final.escrow_units == 0, final.to_amount_strings()
    assert final.paid_units + final.refunded_units == ROOT_BUDGET_UNITS
    assert final.paid_to(AGENT_A_ID) == model.paid_units.get(AGENT_A_ID, 0)
    assert final.paid_to(AGENT_B_ID) == model.paid_units.get(AGENT_B_ID, 0)
    assert final.refunded_to(REQUESTER_ID) == model.refunded_units.get(
        REQUESTER_ID, 0
    )
    assert all(
        state in harness.TERMINAL_JOB_STATES
        for state in final.job_states.values()
    ), final.job_states
    for snapshot in world.snapshots:
        assert harness.check_conservation(snapshot) == [], snapshot.label
        assert harness.check_payee_entitlement(snapshot) == [], snapshot.label
        assert harness.check_no_double_counting(snapshot) == [], snapshot.label

    world.report.note(
        f"Sequence(seed={plan.seed}): 生成操作列 [{plan.signature}] を実プロセスへ"
        f" 流し、{len(world.snapshots)} 個の観測点すべてで参照会計モデルと"
        " deposit / available / locked 4口座 / 受取人別 paid・refunded /"
        " 全 Job 状態が一致した"
    )
    return {
        "scenario_id": world.scenario_id,
        "terminal_job_states": dict(model.states),
        "paid": _formatted(model.paid_units),
        "refunded": _formatted(model.refunded_units),
        "locked_breakdown": {
            bucket: harness.format_amount_units(units)
            for bucket, units in sorted(model.locked_units.items())
        },
    }


def _formatted(totals: dict[str, int]) -> dict[str, str]:
    return {
        payee: harness.format_amount_units(units)
        for payee, units in sorted(totals.items())
        if units
    }


async def _create_child(
    world: harness.E2EWorld,
    model: ReferenceLedger,
    agent_a: harness.AgentSession,
    root_id: str,
    root_lease_id: str,
    step: ChildStep,
    label: str,
) -> tuple[str, str]:
    assert model.can_create_child(step.task_key, CHILD_BUDGET_UNITS), (
        f"{label}: 生成器が第10節の作成可能条件を満たさない操作を選んだ"
    )
    created = await agent_a.call(
        "ojp_create_child_job",
        {
            "parent_job_id": root_id,
            "lease_id": root_lease_id,
            "task_key": step.task_key,
            "budget": CHILD_BUDGET,
            "deadline": CHILD_DEADLINE,
            "operation_id": world.next_operation_id("create", f"a-{label}"),
        },
    )
    child_id = str(created.data["child_id"])
    version_id = str(created.data["version_id"])
    world.child_ids.append(child_id)
    model.create_child(child_id, step.task_key, CHILD_BUDGET_UNITS)
    model.assert_matches(
        world.observe(f"after {label} create ({step.task_key})")
    )
    _assert_usage(world, model, root_id, label=f"{label} create")
    return child_id, version_id


async def _claim_child(
    world: harness.E2EWorld,
    model: ReferenceLedger,
    agent_b: harness.AgentSession,
    child_id: str,
    version_id: str,
    label: str,
) -> str:
    claimed = await agent_b.call(
        "ojp_claim_job",
        {
            "job_id": child_id,
            "expected_version_id": version_id,
            "operation_id": world.next_operation_id("claim", f"b-{label}"),
        },
    )
    model.claim(child_id)
    model.assert_matches(world.observe(f"after {label} claim by B"))
    return str(claimed.data["lease_id"])


async def _resolve_child(
    world: harness.E2EWorld,
    model: ReferenceLedger,
    agent_a: harness.AgentSession,
    agent_b: harness.AgentSession,
    *,
    root_id: str,
    root_lease_id: str,
    child_id: str,
    version_id: str,
    lease_id: str,
    step: ChildStep,
    label: str,
) -> None:
    if step.outcome == SUCCESS:
        submitted = await agent_b.call(
            "ojp_submit",
            {
                "job_id": child_id,
                "lease_id": lease_id,
                "version_id": version_id,
                "artifact_json": json.dumps(
                    {"sum": TASK_EXPECTED_SUM[step.task_key]}
                ),
                "operation_id": world.next_operation_id("submit", f"b-{label}"),
            },
        )
        model.submit(child_id)
        model.assert_matches(world.observe(f"after {label} submit by B"))

        submission_id = str(submitted.data["submission_id"])
        if step.approve_via_cli:
            world.run_cli(
                ["job", "approve", child_id, "--submission", submission_id],
                actor=AGENT_A_ID,
                operation_id=world.next_operation_id("approve", f"a-{label}"),
                action=f"job approve ({label})",
            )
        else:
            await agent_a.call(
                "ojp_approve",
                {
                    "job_id": child_id,
                    "submission_id": submission_id,
                    "operation_id": world.next_operation_id(
                        "approve", f"a-{label}"
                    ),
                },
            )
        model.approve_child(child_id, AGENT_B_ID)
        model.assert_matches(world.observe(f"after {label} approve by A"))
    elif step.outcome == ABANDON:
        world.run_cli(
            ["job", "abandon", child_id, "--lease", lease_id],
            actor=AGENT_B_ID,
            operation_id=world.next_operation_id("abandon", f"b-{label}"),
            action=f"job abandon ({label})",
        )
        model.fail_child(child_id, "FAILED")
        snapshot = world.observe(f"after {label} abandon by B")
        model.assert_matches(snapshot)
        assert snapshot.job_states[child_id] == "FAILED"
    else:
        await _expire_child_lease(
            world,
            model,
            agent_a,
            root_id=root_id,
            root_lease_id=root_lease_id,
            child_id=child_id,
            child_lease_id=lease_id,
            label=label,
        )

    if step.settle_after:
        _settle(world, model, f"{label} settlement")


async def _expire_child_lease(
    world: harness.E2EWorld,
    model: ReferenceLedger,
    agent_a: harness.AgentSession,
    *,
    root_id: str,
    root_lease_id: str,
    child_id: str,
    child_lease_id: str,
    label: str,
) -> None:
    """提出前 Child の Lease 期限だけを越えて独立 tick で失効させる。"""
    child_due_us = world.lease_expires_at_us(child_lease_id)
    target_us = child_due_us + 1_000_000
    if model.states[root_id] == "LEASED":
        # Root と Child の Lease は同じ長さなので、A が Root を heartbeat して
        # 延長しながら進める（Parent 生存中の Child 失効だけを観測する）。
        await _advance_keeping_root_lease(
            world, model, agent_a, root_id, root_lease_id, target_us
        )
        assert world.lease_expires_at_us(root_lease_id) > world.current_clock_us()
    else:
        world.advance_clock_to(target_us)
    assert world.current_clock_us() >= child_due_us

    # tick は「失効処理の確定」と「予約済み送金 1 件ごと」を別 commit で
    # 確定する。commit ごとの不変条件は observe() の check_commit_history が
    # Journal の commit 列を再生して検査するため、ここでは想定した件数の
    # commit だけが起きたことを固定する。
    parent_terminal = model.states[root_id] in harness.TERMINAL_JOB_STATES
    expected_commits = 1 + len(model.pending) + (1 if parent_terminal else 0)
    before_commits = _commit_count(world.snapshots[-1])

    world.tick_once(label=f"tick --once ({label} lease expiry)")
    _apply_lease_expiry(model, child_id)
    snapshot = world.observe(f"after {label} lease expiry")
    model.assert_matches(snapshot)
    assert _commit_count(snapshot) - before_commits == expected_commits, (
        f"{label}: tick の commit 件数が想定と異なる"
        f" observed={_commit_count(snapshot) - before_commits}"
        f" expected={expected_commits}"
    )
    assert snapshot.job_states[child_id] == "EXPIRED"
    child_leases = [
        lease for lease in snapshot.leases if lease["job_id"] == child_id
    ]
    assert [lease["closed_reason"] for lease in child_leases] == ["expired"]


async def _advance_keeping_root_lease(
    world: harness.E2EWorld,
    model: ReferenceLedger,
    agent_a: harness.AgentSession,
    root_id: str,
    root_lease_id: str,
    target_us: int,
) -> None:
    """Root Lease を heartbeat で延長しながら共有 Clock を target まで進める。"""
    for _ in range(_MAX_CLOCK_STEPS):
        await _heartbeat_root(world, model, agent_a, root_id, root_lease_id)
        root_due_us = world.lease_expires_at_us(root_lease_id)
        if target_us < root_due_us:
            world.advance_clock_to(target_us)
            return
        world.advance_clock_to(root_due_us - 1_000_000)
    raise AssertionError(
        f"Root Lease を延長しながら {target_us} まで進められなかった"
    )


async def _heartbeat_root(
    world: harness.E2EWorld,
    model: ReferenceLedger,
    agent_a: harness.AgentSession,
    root_id: str,
    root_lease_id: str,
) -> None:
    await agent_a.call(
        "ojp_heartbeat",
        {
            "job_id": root_id,
            "lease_id": root_lease_id,
            "generation": _lease_generation(world, root_lease_id),
            "operation_id": world.next_operation_id("heartbeat", "a-root"),
        },
    )
    # heartbeat は資金も Job 状態も動かさない（第9節「Claim: なし」と同じ）
    model.assert_matches(world.observe("after root heartbeat by A"))


async def _finish_parent_success(
    world: harness.E2EWorld,
    model: ReferenceLedger,
    agent_a: harness.AgentSession,
    root_id: str,
    root_version_id: str,
    root_lease_id: str,
) -> None:
    """全 Child 判定後に A が Root を提出し、Root Requester が承認する。"""
    submitted = await agent_a.call(
        "ojp_submit",
        {
            "job_id": root_id,
            "lease_id": root_lease_id,
            "version_id": root_version_id,
            "artifact_json": json.dumps({"sum": ROOT_EXPECTED_SUM}),
            "operation_id": world.next_operation_id("submit", "a-root"),
        },
    )
    model.submit(root_id)
    model.assert_matches(world.observe("after root submit by A"))

    world.run_cli(
        [
            "job",
            "approve",
            root_id,
            "--submission",
            str(submitted.data["submission_id"]),
        ],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("approve", "r-root"),
        action="job approve (root)",
    )
    model.approve_root(AGENT_A_ID)
    snapshot = world.observe("after root approve by Requester")
    model.assert_matches(snapshot)
    assert snapshot.job_states[root_id] == "DONE"


async def _finish_parent_failure(
    world: harness.E2EWorld,
    model: ReferenceLedger,
    agent_a: harness.AgentSession,
    agent_b: harness.AgentSession,
    *,
    root_id: str,
    root_lease_id: str,
    plan: SequencePlan,
) -> None:
    """Parent 終端を跨ぐ Child を残したまま A が Root を abandon し、後で判定する。"""
    pending_children: list[tuple[ChildStep, str, str, str | None]] = []
    for index, step in enumerate(plan.tail, start=1):
        label = f"tail-{index}"
        child_id, version_id = await _create_child(
            world, model, agent_a, root_id, root_lease_id, step, label
        )
        lease_id: str | None = None
        if step.claim_before_parent_terminal:
            lease_id = await _claim_child(
                world, model, agent_b, child_id, version_id, label
            )
        pending_children.append((step, child_id, version_id, lease_id))

    world.run_cli(
        ["job", "abandon", root_id, "--lease", root_lease_id],
        actor=AGENT_A_ID,
        operation_id=world.next_operation_id("abandon", "a-root"),
        action="job abandon (root)",
    )
    model.fail_root("FAILED")
    snapshot = world.observe("after root abandon by A")
    model.assert_matches(snapshot)
    assert snapshot.job_states[root_id] == "FAILED"
    # Parent 終端後も既存 Child の原資は保護される（第8節）
    assert snapshot.locked_for("child_work") == CHILD_BUDGET_UNITS * len(
        pending_children
    )

    if plan.settle_after_parent_terminal:
        _settle(world, model, "prior refund settlement")

    for index, (step, child_id, version_id, lease_id) in enumerate(
        pending_children, start=1
    ):
        label = f"tail-{index}"
        if lease_id is None:
            # Parent 終端後でも OPEN Child は各自の条件で継続する（第8節）
            lease_id = await _claim_child(
                world, model, agent_b, child_id, version_id, label
            )
        await _resolve_child(
            world,
            model,
            agent_a,
            agent_b,
            root_id=root_id,
            root_lease_id=root_lease_id,
            child_id=child_id,
            version_id=version_id,
            lease_id=lease_id,
            step=step,
            label=label,
        )


# ---------------------------------------------------------------------------
# helper
# ---------------------------------------------------------------------------


def _settle(world: harness.E2EWorld, model: ReferenceLedger, label: str) -> int:
    """予約済みの送金を 1 件ずつ確定させ、**各 commit 直後**に照合する。

    独立 tick（`ojp tick --once`）は予約ごとに `process_single_payment` を
    呼ぶため 1 回の tick が複数 commit になり、commit の間を観測できない。
    ここでは既存の `ojp payment retry OPERATION`（第14節）を使って
    1 commit ずつ確定させ、その都度スナップショットと参照モデルを突き合わせる。
    """
    processed = 0
    while True:
        operations = _pending_payment_operations(world)
        if not operations:
            return processed
        if processed >= _MAX_SETTLE_ROUNDS:
            raise AssertionError(
                f"{label}: {_MAX_SETTLE_ROUNDS} 件処理しても決済が確定しなかった"
            )
        operation = operations[0]
        before_commits = _commit_count(world.snapshots[-1])
        world.run_cli(
            ["payment", "retry", str(operation["operation_id"])],
            actor=harness.SYSTEM_ID,
            action=f"payment retry ({label})",
        )
        model.settle_operation(
            job_id=str(operation["job_id"]),
            kind=str(operation["kind"]),
            payee_id=str(operation["payee_id"]),
            amount_units=int(operation["amount_units"]),
        )
        snapshot = world.observe(f"after {label} payment {processed + 1}")
        model.assert_matches(snapshot)
        # 送金 1 件は commit 1 件（locked 減額・Wallet 増額・Journal・Receipt を
        # 1 transaction で確定する。第9節 手順3）
        assert _commit_count(snapshot) - before_commits == 1, snapshot.label
        processed += 1


def _pending_payment_operations(world: harness.E2EWorld) -> list[dict[str, Any]]:
    """未確定（PENDING / RETRYABLE）の PaymentOperation を作成順に読む。"""
    with world.read_only_connection() as conn:
        rows = conn.execute(
            "SELECT operation_id, job_id, kind, payee_id, amount_units"
            " FROM payment_operations"
            " WHERE root_id = ? AND status <> 'SUCCEEDED' ORDER BY rowid",
            (world.root_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def _commit_count(snapshot: harness.Snapshot) -> int:
    """そのスナップショット時点までに確定した会計 commit の件数。"""
    return len(harness.replay_commits(snapshot))


def _assert_usage(
    world: harness.E2EWorld,
    model: ReferenceLedger,
    root_id: str,
    *,
    label: str,
) -> None:
    """第10節の U・available・累計 Child 数を参照モデルと照合する。"""
    usage = harness.subcontract_usage(world, root_id, label=label)
    assert harness.parse_amount_string(usage["in_use"]) == model.in_use_units, usage
    assert (
        harness.parse_amount_string(usage["available"]) == model.available_units
    ), usage
    assert usage["child_count"] == model.child_created_count, usage


def _lease_generation(world: harness.E2EWorld, lease_id: str) -> int:
    with world.read_only_connection() as conn:
        row = conn.execute(
            "SELECT generation FROM leases WHERE id = ?", (lease_id,)
        ).fetchone()
    assert row is not None, f"lease not found: {lease_id}"
    return int(row["generation"])
