"""送金 settlement の unit テスト: 保存則・Receipt 正本性・冪等性・再試行境界。

観察可能な契約（D = available + locked + paid + refunded、Receipt がある時だけ
paid/refunded が増える、バックオフ 1/2/4/8/30…、終端へ落ちない）を検証する。
障害注入・別プロセスからの回復は tests/integration/test_settlement_recovery.py。
"""

from __future__ import annotations

import pytest

from ojp import ledger, service
from ojp.domain import ErrorCode, JobState, OjpError, PaymentStatus
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    CHILD_BUDGET_UNITS,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    TEST_T0_US,
    force_job_state,
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
    """指定回数だけ失敗を投げ、その後は本物の Mock と同じ挙動。"""

    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0

    def transfer(self, conn, operation_id, payload):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ledger.PaymentFailedError(
                f"injected transfer failure #{self.calls}"
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


def _inject_mismatched_receipt(handle, *, amount: int = 1) -> None:
    """予約と食い違う既存 Receipt を直接 SQL で混入させる（異常事態の再現。

    アプリの経路からは作れないため、台帳の防御を直接検証する。「金額の
    食い違う Receipt だけが正本として存在する」状態を作る（Wallet の実
    残高は動かさない。locked 残高と Receipt 集計は別の導出なので保存則は
    保たれる）。
    """
    payment = ledger.get_payment_operation(handle.conn, PAYMENT_OP_ID)
    assert payment is not None
    from ojp import db as dbmod

    with dbmod.transaction(handle.conn, immediate=True):
        # 保存則を壊さない形で混入するため、食い違い金額分の Wallet 裏付けを
        # 整合する Journal で作る（locked は予約のまま動かさず、Wallet 台帳
        # 口座と予約口座の間で付け替える。実残高・locked は変わらない）。
        wallet_account = ledger._get_wallet_ledger_account(handle.conn, AGENT_B_ID)
        ledger.apply_journal(
            handle.conn,
            operation_id="payout:fake-receipt-journal",
            reason="pay",
            now_us=TEST_T0_US,
            entries=[
                (payment.source_account_id, -amount),
                (wallet_account.id, amount),
            ],
        )
        handle.conn.execute(
            "INSERT INTO transfer_receipts"
            " (receipt_id, operation_id, amount_units, payee_id, asset,"
            "  source_account_id)"
            " VALUES (?, ?, ?, ?, 'mock-USDC', ?)",
            (
                f"receipt:{PAYMENT_OP_ID}",
                PAYMENT_OP_ID,
                amount,
                AGENT_B_ID,
                payment.source_account_id,
            ),
        )


def test_receipt_mismatch_with_reservation_is_recorded(test_db):
    """Receipt の金額・受取人・原資が予約と食い違う場合、黙って成功にせず、
    例外でバッチを落とすのでもなく、理由を last_error に記録して可視化する。

    transfer_receipts.operation_id は UNIQUE なので、食い違い Receipt の
    存在自体が異常事態。Mock.transfer は照合して拒否し、Wallet・Journal・
    paid をさらに増やさない。失敗は RETRYABLE として記録され、予約は
    locked に残る（返金への切替・Job FAILED 化は行わない）。
    """
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    _inject_mismatched_receipt(test_db, amount=1)

    results = service.process_payments(test_db.conn)
    assert len(results) == 1
    data = results[0].data
    # 例外でバッチが落ちるのではなく、件ごとの結果として失敗が返る
    assert data["payment_status"] == PaymentStatus.RETRYABLE.value
    assert data["attempt_count"] == 1
    assert "does not match" in data["last_error"]
    assert data["next_retry_at_us"] is not None
    # 照合で拒否され、settlement は Wallet・Journal を増やしていない
    assert _wallet(test_db.conn, AGENT_B_ID) == 0
    assert _view(test_db).paid_units == 1  # 混入分だけ
    # 失敗理由と予約金が読み取れる（永続障害の可視化の土台）
    failure = ledger.get_payment_failure_view(test_db.conn, PAYMENT_OP_ID)
    assert failure is not None
    assert failure.last_error is not None
    assert "does not match" in failure.last_error
    assert failure.attempt_count == 1
    assert failure.source_amount_units == CHILD_BUDGET_UNITS - 1  # 予約の残り
    # PaymentOperation は終端へ落ちていない（未 SUCCEEDED のまま）
    payment_after = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment_after is not None
    assert payment_after.status != PaymentStatus.SUCCEEDED
    assert _view(test_db).locked_breakdown_units["child_payout"] == (
        CHILD_BUDGET_UNITS - 1
    )
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_retry_of_mismatched_receipt_records_again_without_success(test_db):
    """食い違い Receipt が残る限り、再試行しても成功にはならず、試行のたびに
    記録が積み上がる（黙って成功にしない）。"""
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    _inject_mismatched_receipt(test_db, amount=1)

    first = service.retry_payment(test_db.conn, operation_id=PAYMENT_OP_ID)
    assert first.data["payment_status"] == PaymentStatus.RETRYABLE.value
    assert first.data["attempt_count"] == 1
    second = service.retry_payment(test_db.conn, operation_id=PAYMENT_OP_ID)
    assert second.data["payment_status"] == PaymentStatus.RETRYABLE.value
    assert second.data["attempt_count"] == 2
    assert "does not match" in second.data["last_error"]
    # SUCCEEDED へは収束せず、Wallet 実残高・Journal は増えない
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status != PaymentStatus.SUCCEEDED
    assert _wallet(test_db.conn, AGENT_B_ID) == 0
    assert _view(test_db).paid_units == 1  # 混入分だけ
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


# ---------------------------------------------------------------------------
# ゼロ no-op は業務キーを消費しない（business_key=NULL の主 Operation だけ残す）
# ---------------------------------------------------------------------------


def _op_business_key(conn, operation_id):
    row = conn.execute(
        "SELECT business_key FROM operations WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()
    assert row is not None, f"operation not found: {operation_id}"
    return row["business_key"]


def _op_exists(conn, operation_id):
    return (
        conn.execute(
            "SELECT 1 FROM operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        is not None
    )


def _bk_count(conn, business_key):
    return int(
        conn.execute(
            "SELECT COUNT(*) AS c FROM operations WHERE business_key = ?",
            (business_key,),
        ).fetchone()["c"]
    )


def _journal_count(conn):
    return int(
        conn.execute(
            "SELECT COUNT(*) AS c FROM journal_transactions"
        ).fetchone()["c"]
    )


def _payment_count(conn, business_key=None):
    if business_key is None:
        sql = "SELECT COUNT(*) AS c FROM payment_operations"
        params = ()
    else:
        sql = "SELECT COUNT(*) AS c FROM payment_operations WHERE business_key = ?"
        params = (business_key,)
    return int(conn.execute(sql, params).fetchone()["c"])


def test_reserve_child_payout_zero_noop_keeps_payout_business_key(test_db):
    """reserve_child_payout(amount_units=0) は正常 no-op で payout:{child_id} を
    消費しない。

    - 主 Operation は business_key=NULL、派生 Operation（payout:zero:payment）
      も PaymentOperation も作らない
    - Journal・残高は不変
    - 同一ゼロ ID の再送は replayed=True で同じ結果
    - その後の別 ID 正額予約が成功し、payout:{child_id} と PaymentOperation は 1 件
    - 正額への別 ID 再確定は既存 PaymentOperation の結果を返す（二重効果なし）
    - 正額効果が既にある状態から別 ID のゼロを呼ぶと正常なゼロ no-op
    - ゼロ→正額への同一 ID 流用は IDEMPOTENCY_CONFLICT
    - 負数は INVALID_ARGUMENT で何も残さない
    """
    _setup_funded_child(test_db)
    journal_before = _journal_count(test_db.conn)
    view_before = _view(test_db)

    zero = service.reserve_child_payout(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=0,
        payee_id=AGENT_B_ID,
        operation_id="payout:zero",
    )
    assert zero.data.get("no_op") is True
    assert "payment_operation_id" not in zero.data
    assert _op_business_key(test_db.conn, "payout:zero") is None
    # 派生 Operation（<operation_id>:payment）も PaymentOperation も作らない
    assert not _op_exists(test_db.conn, "payout:zero:payment")
    assert _payment_count(test_db.conn) == 0
    assert _journal_count(test_db.conn) == journal_before
    assert _view(test_db) == view_before
    assert _bk_count(test_db.conn, f"payout:{CHILD_ID}") == 0

    # 同一ゼロ ID の再送は同じ結果を replay する
    replay = service.reserve_child_payout(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=0,
        payee_id=AGENT_B_ID,
        operation_id="payout:zero",
    )
    assert replay.replayed is True
    assert replay.data == zero.data

    # 負数は INVALID_ARGUMENT で何も残さない
    with pytest.raises(OjpError) as exc_info:
        service.reserve_child_payout(
            test_db.conn,
            actor_id=AGENT_A_ID,
            root_id=ROOT_ID,
            child_id=CHILD_ID,
            amount_units=-1,
            payee_id=AGENT_B_ID,
            operation_id="payout:neg",
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert not _op_exists(test_db.conn, "payout:neg")

    # 別 ID の正額予約が成功する（ゼロが業務キーを消費していないことの反例）
    real = _reserve_child_payout(test_db, operation_id="payout:real")
    assert real.data["payment_operation_id"] == "payout:real:payment"
    assert _bk_count(test_db.conn, f"payout:{CHILD_ID}") == 1
    assert _payment_count(test_db.conn, f"payout:{CHILD_ID}") == 1
    view = _view(test_db)
    assert view.locked_breakdown_units["child_work"] == 0
    assert view.locked_breakdown_units["child_payout"] == CHILD_BUDGET_UNITS

    # 正額への別 ID 再確定は既存 PaymentOperation の結果を返す（二重効果なし）
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
    assert second.data["payment_operation_id"] == "payout:real:payment"
    assert _payment_count(test_db.conn, f"payout:{CHILD_ID}") == 1

    # 正額効果が既にある状態から別 ID のゼロを呼ぶと、例外でも既存正額結果の
    # 再利用でもなく、正常なゼロ no-op になる
    zero_after = service.reserve_child_payout(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=0,
        payee_id=AGENT_B_ID,
        operation_id="payout:zero-after",
    )
    assert zero_after.data.get("no_op") is True
    assert "payment_operation_id" not in zero_after.data
    assert _op_business_key(test_db.conn, "payout:zero-after") is None

    # ゼロで使った ID を正額へ流用すると IDEMPOTENCY_CONFLICT
    with pytest.raises(OjpError) as exc_info:
        service.reserve_child_payout(
            test_db.conn,
            actor_id=AGENT_A_ID,
            root_id=ROOT_ID,
            child_id=CHILD_ID,
            amount_units=CHILD_BUDGET_UNITS,
            payee_id=AGENT_B_ID,
            operation_id="payout:zero",
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_reserve_parent_refund_with_payee_zero_noop_keeps_refund_business_key(test_db):
    """reserve_parent_refund(amount_units=0, payee_id=Root Requester) は正常
    no-op で refund:{root_id}:terminal を消費しない（PaymentOperation 付き経路）。

    - 主 Operation は business_key=NULL、派生 Operation も PaymentOperation も無い
    - Journal・残高は不変
    - 同一ゼロ ID の再送は replayed=True
    - その後の別 ID 正額予約が成功し、PaymentOperation は 1 件だけ
    - 正額への別 ID 再確定は既存 PaymentOperation の結果を返す（二重効果なし）
    - 正額効果が既にある状態から別 ID のゼロを呼ぶと正常なゼロ no-op
    - ゼロ→正額への同一 ID 流用は IDEMPOTENCY_CONFLICT
    - 負数は INVALID_ARGUMENT で何も残さない
    """
    _setup_funded_child(test_db)
    journal_before = _journal_count(test_db.conn)
    view_before = _view(test_db)

    zero = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=0,
        payee_id=REQUESTER_ID,
        operation_id="refund:zero",
    )
    assert zero.data.get("no_op") is True
    assert "payment_operation_id" not in zero.data
    assert _op_business_key(test_db.conn, "refund:zero") is None
    # 派生 Operation（<operation_id>:payment）も PaymentOperation も作らない
    assert not _op_exists(test_db.conn, "refund:zero:payment")
    assert _payment_count(test_db.conn) == 0
    assert _journal_count(test_db.conn) == journal_before
    assert _view(test_db) == view_before
    assert _bk_count(test_db.conn, f"refund:{ROOT_ID}:terminal") == 0

    # 同一ゼロ ID の再送は同じ結果を replay する
    replay = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=0,
        payee_id=REQUESTER_ID,
        operation_id="refund:zero",
    )
    assert replay.replayed is True
    assert replay.data == zero.data

    # 負数は INVALID_ARGUMENT で何も残さない
    with pytest.raises(OjpError) as exc_info:
        service.reserve_parent_refund(
            test_db.conn,
            actor_id=SYSTEM_ID,
            root_id=ROOT_ID,
            amount_units=-1,
            payee_id=REQUESTER_ID,
            operation_id="refund:neg",
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert not _op_exists(test_db.conn, "refund:neg")

    # 別 ID の正額予約が成功する
    real = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        payee_id=REQUESTER_ID,
        operation_id="refund:real",
    )
    assert real.data["payment_operation_id"] == "refund:real:payment"
    assert _bk_count(test_db.conn, f"refund:{ROOT_ID}:terminal") == 1
    assert _payment_count(test_db.conn, f"refund:{ROOT_ID}:terminal") == 1
    view = _view(test_db)
    assert view.available_units == 0
    assert view.locked_breakdown_units["child_work"] == CHILD_BUDGET_UNITS
    assert view.locked_breakdown_units["refund"] == (
        ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    )

    # 正額への別 ID 再確定は既存 PaymentOperation の結果を返す（二重効果なし）
    second = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        payee_id=REQUESTER_ID,
        operation_id="refund:second",
    )
    assert second.replayed is True
    assert second.data["payment_operation_id"] == "refund:real:payment"
    assert _payment_count(test_db.conn, f"refund:{ROOT_ID}:terminal") == 1

    # 正額効果が既にある状態から別 ID のゼロを呼ぶと正常なゼロ no-op
    zero_after = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=0,
        payee_id=REQUESTER_ID,
        operation_id="refund:zero-after",
    )
    assert zero_after.data.get("no_op") is True
    assert "payment_operation_id" not in zero_after.data
    assert _op_business_key(test_db.conn, "refund:zero-after") is None

    # ゼロで使った ID を正額へ流用すると IDEMPOTENCY_CONFLICT
    with pytest.raises(OjpError) as exc_info:
        service.reserve_parent_refund(
            test_db.conn,
            actor_id=SYSTEM_ID,
            root_id=ROOT_ID,
            amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
            payee_id=REQUESTER_ID,
            operation_id="refund:zero",
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_reserve_parent_payout_zero_noop_then_real_creates_one_payment(test_db):
    """Parent payout のゼロ no-op → 別 ID 正額の遷移で、PaymentOperation は
    正額 1 件だけ作られる（settlement は 1 回だけ送金する）。

    Parent payout の PaymentOperation 確認: ゼロ時に派生 Operation・
    PaymentOperation が作られず、正額時にちょうど 1 件作られる。
    """
    _setup_funded_child(test_db)
    # Child を成功扱いにして残額 90 の Parent を DONE にする
    _reserve_child_payout(test_db)
    force_job_state(test_db.conn, ROOT_ID, JobState.DONE)

    zero = service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=0,
        payee_id=AGENT_A_ID,
        operation_id="payout:parent-zero",
    )
    assert zero.data.get("no_op") is True
    assert not _op_exists(test_db.conn, "payout:parent-zero:payment")
    assert _payment_count(test_db.conn) == 1  # Child payout のみ

    real = service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        payee_id=AGENT_A_ID,
        operation_id="payout:parent-real",
    )
    assert real.data["payment_operation_id"] == "payout:parent-real:payment"
    assert _payment_count(test_db.conn, f"payout:{ROOT_ID}") == 1

    # settlement は 2 件（Child payout + Parent payout）を 1 回ずつ送金する
    results = service.process_payments(test_db.conn)
    assert len(results) == 2
    assert all(
        r.data["payment_status"] == PaymentStatus.SUCCEEDED.value for r in results
    )
    view = _view(test_db)
    assert view.paid_units == ROOT_BUDGET_UNITS
    assert _wallet(test_db.conn, AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert _wallet(test_db.conn, AGENT_A_ID) == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
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


def test_every_transfer_failure_is_recorded_regardless_of_kind(test_db):
    """送金失敗は種類にかかわらず attempt_count・last_error を記録し、
    status=RETRYABLE・next_retry_at_us をバックオフで設定する。

    失敗種別の分岐は存在しない（計画書 第9節: 有限回で打ち切って返金へ
    切り替えず、失敗理由と予約金を可視化する）。Mock 自身は永久障害を
    発生させないが、port が失敗を返し続けても返金への切替・Job FAILED 化
    は起きない。
    """

    class AlwaysFailEscrow(ledger.MockEscrow):
        def transfer(self, conn, operation_id, payload):
            raise ledger.PaymentFailedError("escrow permanently unavailable")

    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    results = service.process_payments(test_db.conn, escrow=AlwaysFailEscrow())
    assert len(results) == 1
    data = results[0].data
    assert data["payment_status"] == PaymentStatus.RETRYABLE.value
    assert data["attempt_count"] == 1
    assert data["last_error"] == "escrow permanently unavailable"
    assert data["next_retry_at_us"] == TEST_T0_US + SECOND
    # 予約は locked に残り、返金への切替・終端への落下は起きない
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status == PaymentStatus.RETRYABLE
    assert _view(test_db).locked_breakdown_units["child_payout"] == CHILD_BUDGET_UNITS
    assert _view(test_db).paid_units == 0
    # 失敗理由と予約金が読み取れる（永続障害の可視化の土台）
    failure = ledger.get_payment_failure_view(test_db.conn, PAYMENT_OP_ID)
    assert failure is not None
    assert failure.last_error == "escrow permanently unavailable"
    assert failure.amount_units == CHILD_BUDGET_UNITS
    assert failure.source_amount_units == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_batch_continues_after_one_failure_and_records_reason(test_db):
    """2 件の PaymentOperation のうち 1 件が失敗しても、process_payments は
    他方の処理を止めない（他方は SUCCEEDED になる）。失敗側には last_error と
    next_retry_at_us が残り、返金へは切り替わらない。"""
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    # ※ Phase 4 の approve 経路が入るまでの暫定的な組み立てとして Parent を
    #   DONE にしてから予約する（S3 で reserve_parent_payout は Parent DONE
    #   のときだけ許される。計画書 第8節の表）。
    force_job_state(test_db.conn, ROOT_ID, JobState.DONE)
    service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        payee_id=AGENT_A_ID,
        operation_id="payout:parent-1",
    )
    parent_payment_op_id = "payout:parent-1:payment"

    class FailChildPayoutEscrow(ledger.MockEscrow):
        """child payout（operation_id 昇順で先に処理される 1 件目）だけ失敗させる。"""

        def transfer(self, conn, operation_id, payload):
            if operation_id == PAYMENT_OP_ID:
                raise ledger.PaymentFailedError("child payout rail unavailable")
            return super().transfer(conn, operation_id, payload)

    results = service.process_payments(
        test_db.conn, escrow=FailChildPayoutEscrow()
    )
    assert len(results) == 2
    by_id = {r.operation_id: r for r in results}
    # 失敗側: RETRYABLE + last_error + next_retry_at_us が残る
    failed = by_id[PAYMENT_OP_ID]
    assert failed.data["payment_status"] == PaymentStatus.RETRYABLE.value
    assert failed.data["last_error"] == "child payout rail unavailable"
    assert failed.data["attempt_count"] == 1
    assert failed.data["next_retry_at_us"] == TEST_T0_US + SECOND
    # 成功側: 1 件の失敗に道連れにならず SUCCEEDED になる
    succeeded = by_id[parent_payment_op_id]
    assert succeeded.data["payment_status"] == PaymentStatus.SUCCEEDED.value
    assert _wallet(test_db.conn, AGENT_A_ID) == (
        ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    )
    # 失敗側は返金へ切り替わらず locked 予約のまま
    view = _view(test_db)
    assert view.locked_breakdown_units["child_payout"] == CHILD_BUDGET_UNITS
    assert view.paid_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    assert view.refunded_units == 0
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)

    # 障害が取り除かれれば失敗側も 1 回だけ送金される
    from ojp import clock as clockmod

    clockmod.set_test_now(test_db.conn, TEST_T0_US + SECOND)
    retried = service.process_payments(test_db.conn)
    assert len(retried) == 1
    assert retried[0].data["payment_status"] == PaymentStatus.SUCCEEDED.value
    assert _wallet(test_db.conn, AGENT_B_ID) == CHILD_BUDGET_UNITS
    assert _view(test_db).paid_units == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


# ---------------------------------------------------------------------------
# SUCCEEDED の再処理は Receipt の存在と 3 属性の一致を必ず照合する（Receipt 正本）
# ---------------------------------------------------------------------------


def test_succeeded_reprocess_fails_when_receipt_is_missing(test_db):
    """SUCCEEDED だが Receipt が無い状態では、再処理は成功を返さない。

    Receipt が正本という原則に反する状態を成功扱いせず、理由を last_error に
    記録して整合性エラーとして扱う。既に SUCCEEDED へ確定した整合状態は
    後から書き換えないため status は据え置き、成功（already_succeeded）は
    返さない。
    """
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    service.process_payments(test_db.conn)
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status == PaymentStatus.SUCCEEDED

    # Receipt（正本）を直接削除して「SUCCEEDED だが Receipt が無い」異常状態を
    # 作る（アプリの経路からは作れないため、再処理の防御を直接検証する）。
    # payment_operations.receipt_id の FK 参照を外してから削除する。
    from ojp import db as dbmod

    with dbmod.transaction(test_db.conn, immediate=True):
        test_db.conn.execute(
            "UPDATE payment_operations SET receipt_id = NULL WHERE operation_id = ?",
            (PAYMENT_OP_ID,),
        )
        test_db.conn.execute(
            "DELETE FROM transfer_receipts WHERE operation_id = ?", (PAYMENT_OP_ID,)
        )

    result = service.retry_payment(test_db.conn, operation_id=PAYMENT_OP_ID)
    # 成功としては返さない（already_succeeded は返らず、整合性エラーが返る）
    assert "already_succeeded" not in result.data
    assert result.data["consistency_error"] is True
    assert "missing" in result.data["last_error"]
    # 既に SUCCEEDED へ確定した整合状態は書き換えない（status・attempt 据え置き）
    payment_after = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment_after is not None
    assert payment_after.status == PaymentStatus.SUCCEEDED
    assert payment_after.attempt_count == 0
    assert payment_after.last_error is not None
    assert "missing" in payment_after.last_error
    # 失敗理由が読み取れる
    failure = ledger.get_payment_failure_view(test_db.conn, PAYMENT_OP_ID)
    assert failure is not None
    assert failure.last_error is not None
    assert "missing" in failure.last_error


def test_succeeded_reprocess_detects_receipt_amount_mismatch(test_db):
    """SUCCEEDED の再処理で、Receipt の金額・受取人・原資が予約と食い違う
    場合も成功として返さない。"""
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    service.process_payments(test_db.conn)
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status == PaymentStatus.SUCCEEDED

    # Receipt の金額を直接書き換えて食い違い状態を作る
    test_db.conn.execute(
        "UPDATE transfer_receipts SET amount_units = amount_units - 1"
        " WHERE operation_id = ?",
        (PAYMENT_OP_ID,),
    )
    result = service.retry_payment(test_db.conn, operation_id=PAYMENT_OP_ID)
    assert "already_succeeded" not in result.data
    assert result.data["consistency_error"] is True
    assert "does not match" in result.data["last_error"]
    payment_after = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment_after is not None
    assert payment_after.status == PaymentStatus.SUCCEEDED
    assert payment_after.last_error is not None
    assert "does not match" in payment_after.last_error


def test_payment_failure_view_exposes_reason_and_locked_reservation(test_db):
    """失敗理由と予約金が読み取れる問い合わせ（Phase 5 の `ojp ledger show` /
    `ojp payment retry` が使う土台）: last_error / attempt_count /
    next_retry_at_us / locked 残高を返す。"""
    _setup_funded_child(test_db)
    _reserve_child_payout(test_db)
    escrow = FlakyEscrow(fail_times=1)
    service.process_payments(test_db.conn, escrow=escrow)

    failure = ledger.get_payment_failure_view(test_db.conn, PAYMENT_OP_ID)
    assert failure is not None
    assert failure.operation_id == PAYMENT_OP_ID
    assert failure.status == PaymentStatus.RETRYABLE
    assert failure.attempt_count == 1
    assert failure.last_error == "injected transfer failure #1"
    assert failure.next_retry_at_us == TEST_T0_US + SECOND
    # locked 予約金: 原資口座・予約額・現在の原資残高
    assert failure.amount_units == CHILD_BUDGET_UNITS
    assert failure.source_amount_units == CHILD_BUDGET_UNITS
    assert failure.payee_id == AGENT_B_ID
    assert failure.kind.value == "payout"
    # 存在しない operation は None
    assert ledger.get_payment_failure_view(test_db.conn, "payout:ghost") is None
