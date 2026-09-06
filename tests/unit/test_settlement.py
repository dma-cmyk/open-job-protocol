"""送金 settlement の unit テスト: 保存則・Receipt 正本性・冪等性・再試行境界。

観察可能な契約（D = available + locked + paid + refunded、Receipt がある時だけ
paid/refunded が増える、バックオフ 1/2/4/8/30…、終端へ落ちない）を検証する。
障害注入・別プロセスからの回復は tests/integration/test_settlement_recovery.py。
"""

from __future__ import annotations

import pytest

from ojp import ledger, service
from ojp.domain import ErrorCode, OjpError, PaymentStatus
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    CHILD_BUDGET_UNITS,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    TEST_T0_US,
    insert_child_job,
    setup_ledger_demo_world,
)

ROOT_ID = "job-root-1"
CHILD_ID = "job-child-1"
SECOND = 1_000_000


def _view(handle):
    return ledger.get_root_ledger_view(handle.conn, ROOT_ID)


def _wallet(conn, participant_id):
    row = conn.execute(
        "SELECT balance_units FROM mock_wallets WHERE participant_id = ?",
        (participant_id,),
    ).fetchone()
    return int(row["balance_units"])


def _setup_funded_child(test_db):
    """100 入金 + Child 拘束 10 までの world を作る。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID, seed_workers=True)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    service.fund_root(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
    )
    service.allocate_child_work(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
    )


def _reserve_child_payout(test_db, operation_id="payout:child-1"):
    """コマンドの operation_id で予約を確定する。

    PaymentOperation の ID は派生 ID `<operation_id>:payment` になる
    （PAYMENT_OP_ID を使って参照する）。
    """
    return service.reserve_child_payout(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        payee_id=AGENT_B_ID,
        operation_id=operation_id,
    )


PAYMENT_OP_ID = "payout:child-1:payment"


class FlakyEscrow(ledger.MockEscrow):
    """指定回数だけ retryable な失敗を投げ、その後は本物の Mock と同じ挙動。"""

    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0

    def transfer(self, conn, operation_id, payload):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ledger.PaymentFailedError(
                f"injected transient failure #{self.calls}", kind="retryable"
            )
        return super().transfer(conn, operation_id, payload)


# ---------------------------------------------------------------------------
# 送金成功（完了条件1・7）
# ---------------------------------------------------------------------------


def test_payout_success_conservation_and_wallet(test_db):
    """payout 送金成功: D = available + locked + paid + refunded、B の Wallet が
    増え、paid は Receipt がある時だけ増える。"""
    _setup_funded_child(test_db)
    reserved = _reserve_child_payout(test_db)
    assert reserved.data["payment_status"] == PaymentStatus.PENDING.value
    # 予約時点では paid は 0（Receipt が無い）
    assert _view(test_db).paid_units == 0

    results = service.process_payments(test_db.conn)
    assert len(results) == 1
    assert results[0].data["payment_status"] == PaymentStatus.SUCCEEDED.value

    view = _view(test_db)
    assert view.deposit_units == ROOT_BUDGET_UNITS
    assert view.paid_units == CHILD_BUDGET_UNITS
    assert view.refunded_units == 0
    assert view.locked_breakdown_units["child_payout"] == 0
    assert view.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    assert _wallet(test_db.conn, AGENT_B_ID) == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_refund_success_conservation_and_wallet(test_db):
    """refund 送金成功: refunded は Receipt がある時だけ増え、Requester の
    Wallet へ戻る。保存則と Journal 合計 0 が成立する。"""
    _setup_funded_child(test_db)
    reserved = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        payee_id=REQUESTER_ID,
        operation_id="refund:terminal-1",
    )
    assert reserved.data["payment_status"] == PaymentStatus.PENDING.value
    assert _view(test_db).refunded_units == 0  # 予約時点では 0

    results = service.process_payments(test_db.conn)
    assert len(results) == 1
    assert results[0].data["payment_status"] == PaymentStatus.SUCCEEDED.value

    view = _view(test_db)
    assert view.refunded_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    assert view.paid_units == 0
    assert view.locked_breakdown_units["refund"] == 0
    # Requester は fund で 100 引落し済み（Wallet 0）→ 90 の返金
    assert _wallet(test_db.conn, REQUESTER_ID) == (
        ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    )
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_journal_zero_sum_holds_after_transfers(test_db):
    """完了条件7: MockWallet の入金前残高を含めた全口座間の Journal 合計が
    送金後も 0（各操作単位でも累計でも）。"""
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        payee_id=REQUESTER_ID,
        operation_id="refund:terminal-1",
    )
    service.process_payments(test_db.conn)
    assert ledger.check_journal_zero_sum(test_db.conn) == []
    assert ledger.check_cumulative_journal_zero_sum(test_db.conn) == 0
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


# ---------------------------------------------------------------------------
# Receipt 正本性・冪等（完了条件3・4）
# ---------------------------------------------------------------------------


def test_settlement_after_receipt_is_reconciled_without_double_count(test_db):
    """Receipt 作成後に settlement を再実行しても、既存 Receipt で照合されて
    Wallet も Journal も増えない。"""
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    service.process_payments(test_db.conn)
    wallet_after_first = _wallet(test_db.conn, AGENT_B_ID)
    journal_count = test_db.conn.execute(
        "SELECT COUNT(*) AS c FROM journal_transactions"
    ).fetchone()["c"]

    # SUCCEEDED 済みの再処理（retry_payment 経路）
    again = service.retry_payment(test_db.conn, operation_id=PAYMENT_OP_ID)
    assert again.replayed is True
    assert again.data["already_succeeded"] is True
    # 期限到来分の再処理（process_payments は SUCCEEDED を拾わないので 0 件）
    assert service.process_payments(test_db.conn) == []
    assert _wallet(test_db.conn, AGENT_B_ID) == wallet_after_first
    assert (
        test_db.conn.execute(
            "SELECT COUNT(*) AS c FROM journal_transactions"
        ).fetchone()["c"]
        == journal_count
    )
    assert _view(test_db).paid_units == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_receipt_mismatch_with_reservation_is_rejected(test_db):
    """Receipt の金額・受取人・原資が予約と食い違う場合は拒否される。

    transfer_receipts.operation_id は UNIQUE なので、食い違い Receipt の
    存在自体が異常事態。Mock.transfer は照合して拒否し、Wallet・Journal・
    paid をさらに増やさない。
    """
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None

    # 予約と食い違う既存 Receipt を直接 SQL で混入させる（異常事態の再現。
    # アプリの経路からは作れないため、台帳の防御を直接検証する）。
    from ojp import db as dbmod

    with dbmod.transaction(test_db.conn, immediate=True):
        wallet_account = ledger._get_wallet_ledger_account(test_db.conn, AGENT_B_ID)
        ledger.apply_journal(
            test_db.conn,
            operation_id="payout:fake-receipt-journal",
            reason="pay",
            now_us=TEST_T0_US,
            entries=[
                (payment.source_account_id, -1),  # 予約と違う金額
                (wallet_account.id, 1),
            ],
        )
        test_db.conn.execute(
            "INSERT INTO transfer_receipts"
            " (receipt_id, operation_id, amount_units, payee_id, asset,"
            "  source_account_id)"
            " VALUES (?, ?, ?, ?, 'mock-USDC', ?)",
            (
                f"receipt:{PAYMENT_OP_ID}",
                PAYMENT_OP_ID,
                1,
                AGENT_B_ID,
                payment.source_account_id,
            ),
        )
    wallet_before = _wallet(test_db.conn, AGENT_B_ID)
    with pytest.raises(OjpError) as exc_info:
        service.process_payments(test_db.conn)
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert "does not match" in exc_info.value.message
    # 照合で拒否され、settlement は Wallet・Journal を増やしていない。
    # 照合違反は「送金の一時障害」ではなく予約と正本の矛盾なので、失敗は
    # RETRYABLE へ記録されず呼出側へ伝播する（永続障害として可視化される）。
    assert _wallet(test_db.conn, AGENT_B_ID) == wallet_before
    assert _view(test_db).paid_units == 1  # 混入分だけ
    # 予約は残り、PaymentOperation は終端へ落ちていない（未 SUCCEEDED のまま）
    payment_after = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment_after is not None
    assert payment_after.status != PaymentStatus.SUCCEEDED
    assert payment_after.attempt_count == 0  # 送金失敗としては記録されない
    assert _view(test_db).locked_breakdown_units["child_payout"] == (
        CHILD_BUDGET_UNITS - 1
    )
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_same_business_effect_with_different_operation_ids_pays_once(test_db):
    """完了条件4: 同じ業務効果（同じ Child への payout、同じ原資の refund）が
    異なる operation_id でも 1 回だけ送金される。"""
    _setup_funded_child(test_db)
    first = _reserve_child_payout(test_db, operation_id="payout:first")
    second = service.reserve_child_payout(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        payee_id=AGENT_B_ID,
        operation_id="payout:second",
    )
    assert second.replayed is True
    assert second.data["payment_operation_id"] == first.data["payment_operation_id"]
    # 同じ原資の refund も同様
    refund_first = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        payee_id=REQUESTER_ID,
        operation_id="refund:first",
    )
    refund_second = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        payee_id=REQUESTER_ID,
        operation_id="refund:second",
    )
    assert refund_second.replayed is True
    assert (
        refund_second.data["payment_operation_id"]
        == refund_first.data["payment_operation_id"]
    )

    results = service.process_payments(test_db.conn)
    assert len(results) == 2  # payout 1 + refund 1（別 ID 分は作られていない）
    view = _view(test_db)
    assert view.paid_units == CHILD_BUDGET_UNITS
    assert view.refunded_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    # Wallet も 1 回分だけ
    assert _wallet(test_db.conn, AGENT_B_ID) == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


# ---------------------------------------------------------------------------
# 再試行（完了条件5）
# ---------------------------------------------------------------------------


def test_retry_backoff_sequence_and_no_terminal_fallback(test_db):
    """再試行間隔は 1 / 2 / 4 / 8 / 30 / 30… 秒。失敗が続いても
    PaymentOperation は SUCCEEDED 以外の終端（返金切替・FAILED）へ落ちない。
    最終的に復旧すれば 1 回だけ送金される。"""
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    escrow = FlakyEscrow(fail_times=6)

    from ojp import clock as clockmod

    expected_delays = [1, 2, 4, 8, 30, 30]
    current_us = TEST_T0_US
    for i, delay in enumerate(expected_delays):
        result = service.process_payments(test_db.conn, escrow=escrow)
        assert len(result) == 1
        data = result[0].data
        assert data["payment_status"] == PaymentStatus.RETRYABLE.value
        assert data["attempt_count"] == i + 1
        # next_retry_at_us は失敗を記録した transaction の now が基準
        assert data["next_retry_at_us"] == current_us + delay * SECOND
        assert data["last_error"]
        # 失敗が続いても locked は予約のまま（返金への切替なし、paid も増えない）
        view = _view(test_db)
        assert view.locked_breakdown_units["child_payout"] == CHILD_BUDGET_UNITS
        assert view.paid_units == 0
        ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)
        # 次の再試行期限まで時計を進める（共有 Clock が境界を決める）
        current_us = current_us + delay * SECOND
        clockmod.set_test_now(test_db.conn, current_us)

    # attempt_count=6 の次も 30 秒上限（compute 自体の検証）
    assert (
        ledger.compute_next_retry_at_us(TEST_T0_US, 7) == TEST_T0_US + 30 * SECOND
    )
    assert (
        ledger.compute_next_retry_at_us(TEST_T0_US, 100) == TEST_T0_US + 30 * SECOND
    )

    # 復旧後の再試行で 1 回だけ送金される（時計は既に期限まで進んでいる）
    result = service.process_payments(test_db.conn, escrow=escrow)
    assert len(result) == 1
    assert result[0].data["payment_status"] == PaymentStatus.SUCCEEDED.value
    assert _view(test_db).paid_units == CHILD_BUDGET_UNITS
    assert _wallet(test_db.conn, AGENT_B_ID) == CHILD_BUDGET_UNITS
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.receipt_id is not None
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_retry_not_due_is_not_processed(test_db):
    """next_retry_at_us 未到来の RETRYABLE は settlement 対象にならない。"""
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    escrow = FlakyEscrow(fail_times=1)
    service.process_payments(test_db.conn, escrow=escrow)
    # まだ t0 のまま（next_retry_at_us = t0+1s）→ 対象外
    assert service.process_payments(test_db.conn, escrow=escrow) == []
    # 手動 retry は期限を無視して即時再試行できる
    result = service.retry_payment(test_db.conn, operation_id=PAYMENT_OP_ID)
    assert result.data["payment_status"] == PaymentStatus.SUCCEEDED.value
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


# ---------------------------------------------------------------------------
# 予約確定の契約（残額 0 の no-op）
# ---------------------------------------------------------------------------


def test_zero_amount_reserve_creates_no_payment_operation(test_db):
    """残額 0 なら支払い Operation を作らず正常な no-op（計画書 第16節）。"""
    _setup_funded_child(test_db)
    result = service.reserve_child_payout(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=0,
        payee_id=AGENT_B_ID,
        operation_id="payout:zero",
    )
    assert result.data.get("no_op") is True
    assert "payment_operation_id" not in result.data
    assert ledger.get_payment_operation(test_db.conn, "payout:zero") is None
    assert service.process_payments(test_db.conn) == []
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_payment_operation_fields_are_fixed_at_creation(test_db):
    """原資・受取人は作成時に固定される（attempt しても変わらない）。"""
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.payee_id == AGENT_B_ID
    assert payment.amount_units == CHILD_BUDGET_UNITS
    assert payment.kind.value == "payout"
    assert payment.business_key == f"payout:{CHILD_ID}"
    assert payment.status == PaymentStatus.PENDING
    assert payment.attempt_count == 0
    assert payment.receipt_id is None
    # 原資は child_payout 口座（locked）に固定される
    source = test_db.conn.execute(
        "SELECT bucket FROM budget_accounts WHERE id = ?",
        (payment.source_account_id,),
    ).fetchone()
    assert source["bucket"] == "child_payout"


# ---------------------------------------------------------------------------
# Escrow port の fund が実行経路であることの検証（計画書 第15節）
# ---------------------------------------------------------------------------


def test_fund_root_executes_through_escrow_port(test_db):
    """service.fund_root の資金移動は EscrowPort.fund 経由で実行される。

    注入した port が受け取る immutable payload に、ドメインが決めた
    「誰の Wallet から、どの Root へ、いくら、何を根拠に」が固定されて
    渡り、port の実行だけで Journal・口座・Wallet が動くことを確認する
    （呼出し回数だけでなく、実際の資金移動の結果を見る）。
    """

    class RecordingEscrow(ledger.MockEscrow):
        def __init__(self) -> None:
            self.payloads: list[ledger.FundPayload] = []

        def fund(self, conn, operation_id, payload):
            self.payloads.append(payload)
            return super().fund(conn, operation_id, payload)

    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    escrow = RecordingEscrow()
    result = service.fund_root(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id="fund:via-port",
        escrow=escrow,
    )
    assert result.replayed is False
    # port が受け取った payload はドメインの確定値で固定されている
    assert [p for p in escrow.payloads] == [
        ledger.FundPayload(
            operation_id="fund:via-port",
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
            asset="mock-USDC",
            business_key=f"fund:{ROOT_ID}",
        )
    ]
    # port の実行だけで Journal・口座・Wallet が動いている
    view = _view(test_db)
    assert view.deposit_units == ROOT_BUDGET_UNITS
    assert view.available_units == ROOT_BUDGET_UNITS
    assert _wallet(test_db.conn, REQUESTER_ID) == 0
    assert result.data["amount_units"] == ROOT_BUDGET_UNITS
    row = test_db.conn.execute(
        "SELECT reason FROM journal_transactions WHERE operation_id = ?",
        ("fund:via-port",),
    ).fetchone()
    assert row["reason"] == "fund"
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_fund_root_with_failing_port_rolls_back_everything(test_db):
    """fund が失敗する port を注入すると funding が拒否され、Journal・口座・
    Wallet・operations がまったく動かない（transaction 全体が rollback）。

    「port のメソッドが呼ばれた」ことだけを見るのではなく、port を通らない
    確定経路が存在しないこと（port が失敗すれば何も確定されない）を検証する。
    """

    class FailingFundEscrow(ledger.MockEscrow):
        def fund(self, conn, operation_id, payload):
            raise ledger.PaymentFailedError("escrow funding unavailable")

    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    with pytest.raises(ledger.PaymentFailedError):
        service.fund_root(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
            operation_id="fund:port-fail",
            escrow=FailingFundEscrow(),
        )
    # Journal・口座・Wallet・operations のいずれも動いていない
    assert (
        test_db.conn.execute(
            "SELECT COUNT(*) AS c FROM journal_transactions"
        ).fetchone()["c"]
        == 0
    )
    assert (
        test_db.conn.execute(
            "SELECT COUNT(*) AS c FROM operations WHERE operation_id = ?",
            ("fund:port-fail",),
        ).fetchone()["c"]
        == 0
    )
    assert (
        test_db.conn.execute(
            "SELECT COUNT(*) AS c FROM budget_accounts"
        ).fetchone()["c"]
        == 0
    )
    assert _wallet(test_db.conn, REQUESTER_ID) == ROOT_BUDGET_UNITS
    assert _view(test_db).deposit_units == 0


def test_mock_fund_rejects_unknown_asset(test_db):
    """Mock の fund port は未知の asset を拒否する。

    本番 Escrow 用の chain_id・署名・x402・外部 HTTP の空実装は存在しない。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
            escrow=RejectingAssetEscrow(),
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    # 拒否後は何も動いていない
    assert _view(test_db).deposit_units == 0
    assert _wallet(test_db.conn, REQUESTER_ID) == ROOT_BUDGET_UNITS


class RejectingAssetEscrow(ledger.MockEscrow):
    """asset を mock-USDC 以外に書き換えて super().fund へ渡す検証用 port。"""

    def fund(self, conn, operation_id, payload):
        bad = ledger.FundPayload(
            operation_id=payload.operation_id,
            root_id=payload.root_id,
            requester_id=payload.requester_id,
            expected_amount_units=payload.expected_amount_units,
            amount_units=payload.amount_units,
            asset="other-asset",
            business_key=payload.business_key,
        )
        return super().fund(conn, operation_id, bad)


def test_permanent_payment_failure_is_not_retried(test_db):
    """PaymentFailedError(kind='permanent') は将来の本番アダプター契約の型。

    永続障害は再試行で解消しないため RETRYABLE へ記録せず呼出側へ伝播する
    （予約は locked に残り可視化される）。Mock 自身は permanent を発生させない。
    """

    class PermanentFailEscrow(ledger.MockEscrow):
        def transfer(self, conn, operation_id, payload):
            raise ledger.PaymentFailedError("permanent failure", kind="permanent")

    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    with pytest.raises(ledger.PaymentFailedError):
        service.process_payments(test_db.conn, escrow=PermanentFailEscrow())
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status == PaymentStatus.PENDING  # RETRYABLE にすらならない
    assert payment.attempt_count == 0
    assert _view(test_db).locked_breakdown_units["child_payout"] == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)
