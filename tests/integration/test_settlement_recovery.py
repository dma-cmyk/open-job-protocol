"""送金の障害注入・応答消失・再起動からの回復の integration テスト。

3 種の障害注入（(a) 決済 commit 前の失敗、(b) commit 後の応答消失、
(c) Receipt 確定後・アプリの status 更新前の停止）から、実ファイル SQLite・
新しい接続・新しいプロセス（subprocess）で SUCCEEDED へ収束し、MockWallet も
Journal も二重加算しないことを確認する。failpoint は test mode の DB でのみ
有効で、realtime mode の経路からは有効化できないことも検証する。
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

from ojp import clock, db, ledger, service
from ojp.domain import ErrorCode, OjpError, PaymentStatus
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    CHILD_BUDGET_UNITS,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    TEST_T0_US,
    insert_child_job,
    setup_ledger_demo_world,
)

ROOT_ID = "job-root-1"
CHILD_ID = "job-child-1"
COMMAND_OP_ID = "payout:child-1"
PAYMENT_OP_ID = "payout:child-1:payment"


@pytest.fixture(autouse=True)
def _clear_failpoints():
    """failpoint seam はモジュール属性なので、テスト後は必ず元に戻す。"""
    yield
    ledger.failpoint_before_commit = None
    ledger.failpoint_after_commit = None
    ledger.failpoint_after_receipt = None


def _setup_reserved_payout(handle):
    """100 入金 + Child 拘束 10 + 送金予約確定（PENDING）までの world。"""
    setup_ledger_demo_world(handle.conn, ROOT_ID, seed_workers=True)
    insert_child_job(handle.conn, ROOT_ID, CHILD_ID)
    service.fund_root(
        handle.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
    )
    service.allocate_child_work(
        handle.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
    )
    service.reserve_child_payout(
        handle.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        payee_id=AGENT_B_ID,
        operation_id=COMMAND_OP_ID,
    )


def _wallet(conn, participant_id):
    row = conn.execute(
        "SELECT balance_units FROM mock_wallets WHERE participant_id = ?",
        (participant_id,),
    ).fetchone()
    return int(row["balance_units"])


def _journal_count(conn):
    return conn.execute(
        "SELECT COUNT(*) AS c FROM journal_transactions"
    ).fetchone()["c"]


def _assert_converged(conn, *, payee_id=AGENT_B_ID):
    """SUCCEEDED への収束・二重加算なし・保存則を一括で検証する。"""
    payment = ledger.get_payment_operation(conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.receipt_id is not None
    receipt = ledger.lookup_transfer_receipt(conn, PAYMENT_OP_ID)
    assert receipt is not None
    assert receipt.receipt_id == payment.receipt_id
    assert receipt.amount_units == CHILD_BUDGET_UNITS
    assert receipt.payee_id == payee_id
    assert receipt.source_account_id == payment.source_account_id
    assert _wallet(conn, payee_id) == CHILD_BUDGET_UNITS  # 二重加算なし
    view = ledger.get_root_ledger_view(conn, ROOT_ID)
    assert view.paid_units == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(conn, ROOT_ID)


# ---------------------------------------------------------------------------
# (a) 決済 commit 前の失敗 → rollback → 新しい接続で収束
# ---------------------------------------------------------------------------


def test_failpoint_before_commit_rolls_back_and_recovers_with_new_connection(test_db):
    """(a) 決済 commit 前の失敗: transaction 全体が rollback され、Receipt も
    台帳も残らない。注入を外して新しい接続で処理すると 1 回だけ送金される。"""
    _setup_reserved_payout(test_db)

    def boom(name):
        raise RuntimeError(f"injected failure at {name}")

    ledger.failpoint_before_commit = boom
    journal_before = _journal_count(test_db.conn)
    with pytest.raises(RuntimeError, match="before_commit"):
        service.process_payments(test_db.conn)
    ledger.failpoint_before_commit = None

    # rollback: PaymentOperation は PENDING のまま、Receipt なし、台帳不変
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status == PaymentStatus.PENDING
    assert ledger.lookup_transfer_receipt(test_db.conn, PAYMENT_OP_ID) is None
    assert _journal_count(test_db.conn) == journal_before
    assert _wallet(test_db.conn, AGENT_B_ID) == 0

    # 新しい接続で処理し直すと 1 回だけ送金される
    fresh = test_db.fresh_conn()
    try:
        results = service.process_payments(fresh)
        assert len(results) == 1
        assert results[0].data["payment_status"] == PaymentStatus.SUCCEEDED.value
        _assert_converged(fresh)
    finally:
        fresh.close()
    # 元の接続からも収束が見える
    _assert_converged(test_db.conn)


# ---------------------------------------------------------------------------
# (b) commit 後の応答消失 → 別プロセスが Receipt を正本に収束
# ---------------------------------------------------------------------------

_SETTLE_SEND_SCRIPT = textwrap.dedent(
    """
    import sys
    from ojp import clock, db, ledger
    from ojp.domain import ClockMode

    path, payment_op_id = sys.argv[1], sys.argv[2]
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(sys.argv[3]))
    # transfer を独立した transaction で確定する（本番 Escrow の外部確定を模す）
    escrow = ledger.MockEscrow()
    with db.transaction(conn, immediate=True):
        payment = ledger.get_payment_operation(conn, payment_op_id)
        payload = ledger.TransferPayload(
            operation_id=payment.operation_id,
            amount_units=payment.amount_units,
            payee_id=payment.payee_id,
            source_account_id=payment.source_account_id,
            business_key=payment.business_key,
        )
        receipt = escrow.transfer(conn, payment_op_id, payload)
        print(f"receipt committed: {receipt.receipt_id}", flush=True)
    conn.close()
    """
)

_PROCESS_SCRIPT = textwrap.dedent(
    """
    import sys
    from ojp import clock, service
    from ojp.domain import ClockMode

    path, payment_op_id = sys.argv[1], sys.argv[2]
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(sys.argv[3]))
    result = service.process_single_payment(conn, operation_id=payment_op_id)
    print(f"status={result.data['payment_status']}", flush=True)
    conn.close()
    """
)


def _run_script(script: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", script, *args],
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_failpoint_after_commit_response_loss_recovers_from_new_process(test_db):
    """(b) commit 後の応答消失: 送金結果は commit 済みだが、呼出側は結果を
    記録する前に応答を失う（failpoint で transaction を中断＝rollback）。
    Receipt が正本なので、新しいプロセスが lookup で照会して SUCCEEDED へ
    収束させ、Wallet も Journal も二重加算しない。"""
    _setup_reserved_payout(test_db)
    # 子プロセス A: transfer を独立 transaction で commit（Receipt・台帳は確定）
    send = _run_script(
        _SETTLE_SEND_SCRIPT, str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US)
    )
    assert send.returncode == 0, send.stderr
    assert "receipt committed" in send.stdout

    # 呼出側は応答消失: status 更新 transaction の直前で failpoint が発火し、
    # PaymentOperation は未更新（PENDING のまま）。ここでは同一プロセス内で
    # 応答消失を再現する（commit 前の中断なので台帳は変わらない）。
    def lost_response(name):
        raise RuntimeError(f"response lost at {name}")

    ledger.failpoint_after_commit = lost_response
    with pytest.raises(RuntimeError, match="after_commit"):
        service.process_single_payment(test_db.conn, operation_id=PAYMENT_OP_ID)
    ledger.failpoint_after_commit = None
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status == PaymentStatus.PENDING  # 応答消失で未更新

    # 新しいプロセス B: 同じ DB を処理し、Receipt を正本に照合して収束する
    recover = _run_script(
        _PROCESS_SCRIPT, str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US)
    )
    assert recover.returncode == 0, recover.stderr
    assert "status=SUCCEEDED" in recover.stdout

    # 新しい接続で最終状態を検証（Wallet も Journal も二重加算なし）
    fresh = test_db.fresh_conn()
    try:
        _assert_converged(fresh)
        # pay の journal は 1 操作分だけ（再処理で増えていない）
        rows = fresh.execute(
            "SELECT COUNT(*) AS c FROM journal_transactions WHERE reason = 'pay'"
        ).fetchone()
        assert rows["c"] == 1
    finally:
        fresh.close()


# ---------------------------------------------------------------------------
# (c) Receipt 確定後・アプリの status 更新前の停止 → 新しいプロセスで収束
# ---------------------------------------------------------------------------


def test_failpoint_after_receipt_restart_recovers_from_new_process(test_db):
    """(c) Receipt 確定後・アプリの status 更新前の停止: transfer（Receipt・
    台帳）は独立 transaction で commit 済み、status 更新は未確定の状態で
    プロセスが停止する。新しいプロセスが lookup で Receipt を照会して
    SUCCEEDED へ収束させ、Wallet も Journal も二重加算しない。"""
    _setup_reserved_payout(test_db)

    # プロセス A: transfer を独立 transaction で確定し、そのまま終了
    # （アプリの status 更新を行わない = status 更新前の停止）
    send = _run_script(
        _SETTLE_SEND_SCRIPT, str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US)
    )
    assert send.returncode == 0, send.stderr
    # PaymentOperation は未更新（PENDING のまま停止した状態）
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status == PaymentStatus.PENDING
    # Receipt は正本として commit 済み
    receipt = ledger.lookup_transfer_receipt(test_db.conn, PAYMENT_OP_ID)
    assert receipt is not None

    # プロセス B（再起動）: lookup で Receipt を照会して SUCCEEDED へ収束
    recover = _run_script(
        _PROCESS_SCRIPT, str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US)
    )
    assert recover.returncode == 0, recover.stderr
    assert "status=SUCCEEDED" in recover.stdout

    fresh = test_db.fresh_conn()
    try:
        _assert_converged(fresh)
        rows = fresh.execute(
            "SELECT COUNT(*) AS c FROM journal_transactions WHERE reason = 'pay'"
        ).fetchone()
        assert rows["c"] == 1
    finally:
        fresh.close()


def test_failpoint_after_receipt_fires_in_test_mode(test_db):
    """(c) の seam 自体: Mock.transfer の Receipt INSERT 直後に発火する。

    発火して例外を投げれば Receipt・台帳を含む transaction 全体が rollback
    される（status 更新前の停止を単一 transaction 内で再現した場合）。
    その後、注入を外して再処理すると 1 回だけ送金される。
    """
    _setup_reserved_payout(test_db)
    fired: list[str] = []

    def crash(name):
        fired.append(name)
        raise RuntimeError("crash after receipt")

    ledger.failpoint_after_receipt = crash
    journal_before = _journal_count(test_db.conn)
    with pytest.raises(RuntimeError, match="crash after receipt"):
        service.process_payments(test_db.conn)
    assert fired == ["after_receipt"]
    # rollback: Receipt も台帳も残らず、PENDING のまま
    assert ledger.lookup_transfer_receipt(test_db.conn, PAYMENT_OP_ID) is None
    assert _journal_count(test_db.conn) == journal_before
    payment = ledger.get_payment_operation(test_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status == PaymentStatus.PENDING

    ledger.failpoint_after_receipt = None
    results = service.process_payments(test_db.conn)
    assert len(results) == 1
    _assert_converged(test_db.conn)


# ---------------------------------------------------------------------------
# failpoint は test mode のみ（realtime 経路からは有効化できない）
# ---------------------------------------------------------------------------


def test_failpoints_are_rejected_in_realtime_mode(realtime_db):
    """failpoint は realtime mode の DB では代入済みでも発火せず拒否される。

    通常経路（realtime）から障害注入を有効化できないことを、(a)(b)(c) の
    各 seam と ledger._fire_failpoint のモード検査で確認する。
    """
    _setup_reserved_payout(realtime_db)

    def boom(name):
        raise RuntimeError(f"must not fire in realtime: {name}")

    # (a) の seam: realtime では代入済みでも発火前に拒否される
    ledger.failpoint_before_commit = boom
    with pytest.raises(OjpError) as exc_info:
        service.process_payments(realtime_db.conn)
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert "test mode" in exc_info.value.message
    ledger.failpoint_before_commit = None

    # (b) の seam も同様
    ledger.failpoint_after_commit = boom
    with pytest.raises(OjpError, match="test mode"):
        service.process_single_payment(realtime_db.conn, operation_id=PAYMENT_OP_ID)
    ledger.failpoint_after_commit = None

    # (c) の seam も同様（注入を外して正常に処理できることを最後に確認）
    ledger.failpoint_after_receipt = boom
    with pytest.raises(OjpError, match="test mode"):
        service.process_single_payment(realtime_db.conn, operation_id=PAYMENT_OP_ID)
    ledger.failpoint_after_receipt = None

    # 注入を外せば realtime でも通常通り 1 回だけ送金される
    results = service.process_payments(realtime_db.conn)
    assert len(results) == 1
    _assert_converged(realtime_db.conn)


# ---------------------------------------------------------------------------
# 同じ DB を別プロセスが順に処理しても 1 回だけ送金される（冪等の越境確認）
# ---------------------------------------------------------------------------


def test_two_processes_settle_same_payment_only_once(test_db):
    """同じ PaymentOperation を別プロセスが順に処理しても送金は 1 回だけ。

    先に commit したプロセスの Receipt が正本となり、後続プロセスは照合で
    no-op になる。Wallet も Journal も二重加算しない。
    """
    _setup_reserved_payout(test_db)
    first = _run_script(
        _PROCESS_SCRIPT, str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US)
    )
    assert first.returncode == 0, first.stderr
    assert "status=SUCCEEDED" in first.stdout
    second = _run_script(
        _PROCESS_SCRIPT, str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US)
    )
    assert second.returncode == 0, second.stderr
    assert "status=SUCCEEDED" in second.stdout

    fresh = test_db.fresh_conn()
    try:
        _assert_converged(fresh)
        rows = fresh.execute(
            "SELECT COUNT(*) AS c FROM journal_transactions WHERE reason = 'pay'"
        ).fetchone()
        assert rows["c"] == 1
    finally:
        fresh.close()
