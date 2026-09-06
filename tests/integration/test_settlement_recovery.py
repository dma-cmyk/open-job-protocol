"""送金の障害注入・応答消失・再起動からの回復の integration テスト。

完了条件3: 3 種の障害注入（(a) 決済 commit 前の失敗、(b) commit 後の応答
消失、(c) Receipt 確定後・アプリの status 更新前の停止）から、実ファイル
SQLite・新しい接続・新しいプロセス（subprocess）で SUCCEEDED へ収束し、
MockWallet も Journal も二重加算しないことを確認する。

(a)(b)(c) はいずれも failpoint を実際に別プロセス内で発火させて異常終了
させ、その後の新しいプロセスが同じ実ファイル DB を処理して収束することを
検証する。failpoint は test mode の DB でのみ有効で、realtime mode の経路
からは有効化できないことも検証する。
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

from ojp import db, ledger, service
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


def _pay_journal_count(conn):
    return conn.execute(
        "SELECT COUNT(*) AS c FROM journal_transactions WHERE reason = 'pay'"
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
    # pay の journal と Wallet 増加は 1 回分だけ（二重加算なし）
    assert _pay_journal_count(conn) == 1
    assert _wallet(conn, payee_id) == CHILD_BUDGET_UNITS
    view = ledger.get_root_ledger_view(conn, ROOT_ID)
    assert view.paid_units == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(conn, ROOT_ID)


# ---------------------------------------------------------------------------
# 別プロセスで failpoint を発火させて異常終了させるスクリプト
# ---------------------------------------------------------------------------


def _crash_script(failpoint_attr: str) -> str:
    """指定 failpoint を発火させて process_single_payment を異常終了させる。"""
    return textwrap.dedent(
        f"""
        import sys
        from ojp import clock, ledger, service
        from ojp.domain import ClockMode

        path, payment_op_id, now_us = sys.argv[1], sys.argv[2], int(sys.argv[3])
        conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=now_us)

        def boom(name):
            raise RuntimeError(f"injected crash at {{name}}")

        ledger.{failpoint_attr} = boom
        try:
            service.process_single_payment(conn, operation_id=payment_op_id)
        except RuntimeError:
            # 障害注入による異常終了（commit しないまま終わる）
            sys.exit(3)
        print("unexpected success", flush=True)
        sys.exit(0)
        """
    )


# (c) の再現: transfer（Receipt・locked 減額・Wallet 増額・Journal）を
# 独立した transaction で commit してから、status 更新を行わないまま
# 異常終了する（「Receipt は確定済み・status 更新は未確定」の中断状態）。
_SETTLE_AND_CRASH_BEFORE_STATUS_SCRIPT = textwrap.dedent(
    """
    import sys
    from ojp import clock, db, ledger
    from ojp.domain import ClockMode

    path, payment_op_id, now_us = sys.argv[1], sys.argv[2], int(sys.argv[3])
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=now_us)
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
    # status 更新 transaction を開始する前に異常終了する
    sys.exit(3)
    """
)

_PROCESS_SCRIPT = textwrap.dedent(
    """
    import sys
    from ojp import clock, service
    from ojp.domain import ClockMode

    path, payment_op_id, now_us = sys.argv[1], sys.argv[2], int(sys.argv[3])
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=now_us)
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


def _assert_crash(result: subprocess.CompletedProcess[str]) -> None:
    """failpoint が発火してプロセスが異常終了したことを確認する。"""
    assert result.returncode == 3, (
        f"expected crash exit code 3, got {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


# ---------------------------------------------------------------------------
# (a) 決済 commit 前の失敗 → 台帳・Receipt が一切残らない → 新プロセスで収束
# ---------------------------------------------------------------------------


def test_failpoint_before_commit_crash_leaves_nothing_and_recovers(test_db):
    """(a) 決済 commit 前の失敗: 別プロセスで failpoint を発火させて異常終了
    させる。transaction 全体が rollback されるため Receipt も台帳も一切
    残らず、新しいプロセスで処理し直すと 1 回だけ送金される。"""
    _setup_reserved_payout(test_db)
    journal_before = test_db.conn.execute(
        "SELECT COUNT(*) AS c FROM journal_transactions"
    ).fetchone()["c"]

    crash = _run_script(
        _crash_script("failpoint_before_commit"),
        str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US),
    )
    _assert_crash(crash)

    # rollback: PaymentOperation は PENDING のまま、Receipt なし、台帳不変
    fresh = test_db.fresh_conn()
    try:
        payment = ledger.get_payment_operation(fresh, PAYMENT_OP_ID)
        assert payment is not None
        assert payment.status == PaymentStatus.PENDING
        assert payment.attempt_count == 0
        assert ledger.lookup_transfer_receipt(fresh, PAYMENT_OP_ID) is None
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM journal_transactions"
            ).fetchone()["c"]
            == journal_before
        )
        assert _wallet(fresh, AGENT_B_ID) == 0
    finally:
        fresh.close()

    # 新しいプロセスが同じ実ファイル DB を処理して SUCCEEDED へ収束する
    recover = _run_script(
        _PROCESS_SCRIPT, str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US)
    )
    assert recover.returncode == 0, recover.stderr
    assert "status=SUCCEEDED" in recover.stdout

    fresh2 = test_db.fresh_conn()
    try:
        _assert_converged(fresh2)
    finally:
        fresh2.close()


# ---------------------------------------------------------------------------
# (b) commit 後の応答消失 → Receipt が正本として残る → 新プロセスで収束
# ---------------------------------------------------------------------------


def test_failpoint_after_commit_response_loss_recovers_from_new_process(test_db):
    """(b) commit 後の応答消失: 送金結果（Receipt・台帳）は独立 transaction
    で commit 済みだが、呼出側の status 更新 transaction の commit 前に
    failpoint を発火させてプロセスを異常終了させる（応答消失）。
    Receipt が正本なので、新しいプロセスが lookup で照会して SUCCEEDED へ
    収束させ、Wallet も Journal も二重加算しない。"""
    _setup_reserved_payout(test_db)

    # プロセス A: transfer を独立 transaction で commit（Receipt は正本として残る）
    send = _run_script(
        _SETTLE_AND_CRASH_BEFORE_STATUS_SCRIPT,
        str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US),
    )
    _assert_crash(send)
    assert "receipt committed" in send.stdout

    # プロセス B: 応答消失。status 更新 transaction の commit 前に
    # after_commit failpoint が発火して異常終了し、PaymentOperation は
    # 未更新（PENDING）のまま残る
    lost = _run_script(
        _crash_script("failpoint_after_commit"),
        str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US),
    )
    _assert_crash(lost)
    fresh = test_db.fresh_conn()
    try:
        payment = ledger.get_payment_operation(fresh, PAYMENT_OP_ID)
        assert payment is not None
        assert payment.status == PaymentStatus.PENDING  # 応答消失で未更新
        # Receipt は正本として commit 済みで残っている
        receipt = ledger.lookup_transfer_receipt(fresh, PAYMENT_OP_ID)
        assert receipt is not None
    finally:
        fresh.close()

    # プロセス C（再起動）: 新しいプロセスが Receipt を正本に照合して収束する
    recover = _run_script(
        _PROCESS_SCRIPT, str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US)
    )
    assert recover.returncode == 0, recover.stderr
    assert "status=SUCCEEDED" in recover.stdout

    fresh2 = test_db.fresh_conn()
    try:
        _assert_converged(fresh2)
    finally:
        fresh2.close()


# ---------------------------------------------------------------------------
# (c) Receipt 確定後・status 更新前の停止 → status だけ未確定 → 新プロセスで収束
# ---------------------------------------------------------------------------


def test_failpoint_after_receipt_restart_recovers_from_new_process(test_db):
    """(c) Receipt 確定後・アプリの status 更新前の停止: transfer（Receipt・
    locked 減額・Wallet 増額・Journal）は独立 transaction で commit 済み、
    status 更新だけが未確定の状態でプロセスが異常終了する。新しいプロセスが
    lookup で Receipt を照会して SUCCEEDED へ収束させ、Wallet も Journal も
    二重加算しない。"""
    _setup_reserved_payout(test_db)

    # プロセス A: transfer を独立 transaction で確定し、status 更新を
    # 行わないまま異常終了する
    send = _run_script(
        _SETTLE_AND_CRASH_BEFORE_STATUS_SCRIPT,
        str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US),
    )
    _assert_crash(send)
    assert "receipt committed" in send.stdout

    fresh = test_db.fresh_conn()
    try:
        # status だけ未確定（PENDING のまま）
        payment = ledger.get_payment_operation(fresh, PAYMENT_OP_ID)
        assert payment is not None
        assert payment.status == PaymentStatus.PENDING
        # Receipt は正本として commit 済みで、台帳・Wallet も確定済み
        receipt = ledger.lookup_transfer_receipt(fresh, PAYMENT_OP_ID)
        assert receipt is not None
        assert _pay_journal_count(fresh) == 1
        assert _wallet(fresh, AGENT_B_ID) == CHILD_BUDGET_UNITS
    finally:
        fresh.close()

    # プロセス B（再起動）: lookup で Receipt を照会して SUCCEEDED へ収束
    recover = _run_script(
        _PROCESS_SCRIPT, str(test_db.path), PAYMENT_OP_ID, str(TEST_T0_US)
    )
    assert recover.returncode == 0, recover.stderr
    assert "status=SUCCEEDED" in recover.stdout

    fresh2 = test_db.fresh_conn()
    try:
        _assert_converged(fresh2)
    finally:
        fresh2.close()


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

    # (a) の seam: realtime では代入済みでも発火前に拒否される。
    # この拒否（OjpError）も送金失敗として記録される契約（計画書 第9節
    # 「失敗理由と予約金を可視化する」）であり、PaymentOperation は
    # 終端へ落ちず locked 予約は残る。
    ledger.failpoint_before_commit = boom
    result = service.process_payments(realtime_db.conn)
    assert len(result) == 1
    assert result[0].data["payment_status"] == PaymentStatus.RETRYABLE.value
    assert "test mode" in result[0].data["last_error"]
    payment = ledger.get_payment_operation(realtime_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.status == PaymentStatus.RETRYABLE
    assert payment.attempt_count == 1
    assert payment.next_retry_at_us is not None
    ledger.failpoint_before_commit = None

    # (b) の seam: この拒否は status 更新 transaction 内・failpoint 発火時点で
    # 伝播する（発火自体の不許可であり送金試行の失敗ではないため、
    # attempt_count は増えない）
    ledger.failpoint_after_commit = boom
    with pytest.raises(OjpError, match="test mode"):
        service.retry_payment(realtime_db.conn, operation_id=PAYMENT_OP_ID)
    ledger.failpoint_after_commit = None
    payment = ledger.get_payment_operation(realtime_db.conn, PAYMENT_OP_ID)
    assert payment is not None
    assert payment.attempt_count == 1

    # (c) の seam も同様に拒否され、記録として残る
    ledger.failpoint_after_receipt = boom
    rejected = service.retry_payment(realtime_db.conn, operation_id=PAYMENT_OP_ID)
    assert rejected.data["payment_status"] == PaymentStatus.RETRYABLE.value
    assert "test mode" in rejected.data["last_error"]
    assert rejected.data["attempt_count"] == 2
    ledger.failpoint_after_receipt = None

    # 注入を外せば realtime でも通常通り 1 回だけ送金される
    recovered = service.retry_payment(realtime_db.conn, operation_id=PAYMENT_OP_ID)
    assert recovered.data["payment_status"] == PaymentStatus.SUCCEEDED.value
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
    finally:
        fresh.close()
