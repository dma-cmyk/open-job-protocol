"""台帳の integration テスト: 実ファイル SQLite・新しい接続での永続確認・
冪等性・business_key・Parent 失敗後の Child 失敗（計画書 N13 / X08 の会計部分）。

メモリ DB だけで済ませず、実ファイル SQLite と新しい接続（DbHandle.fresh_conn()）
で永続結果を確認する。
"""

from __future__ import annotations

import pytest

from ojp import ledger, service
from ojp.domain import ErrorCode, OjpError
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    CHILD_BUDGET_UNITS,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    insert_child_job,
    insert_root_job,
    setup_ledger_demo_world,
)

ROOT_ID = "job-root-1"
CHILD_ID = "job-child-1"


def _view(conn, root_id=ROOT_ID):
    return ledger.get_root_ledger_view(conn, root_id)


def _fund(test_db, operation_id=None):
    return service.fund_root(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id=operation_id,
    )


def _allocate(test_db, operation_id=None):
    return service.allocate_child_work(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        operation_id=operation_id,
    )


# ---------------------------------------------------------------------------
# 実ファイル SQLite + 新しい接続での永続確認（完了条件7）
# ---------------------------------------------------------------------------


def test_persistence_with_fresh_connection(test_db):
    """実ファイル SQLite に commit 後、新しい接続で永続結果を確認する。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund(test_db)
    _allocate(test_db)

    # 新しい接続で永続結果を確認する
    fresh = test_db.fresh_conn()
    try:
        view = _view(fresh)
        assert view.deposit_units == ROOT_BUDGET_UNITS
        assert view.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
        assert view.locked_breakdown_units["child_work"] == CHILD_BUDGET_UNITS
        ledger.assert_ledger_invariants(fresh, ROOT_ID)
        # Journal も永続されている
        rows = fresh.execute(
            "SELECT COUNT(*) AS c FROM journal_transactions"
        ).fetchone()
        assert rows["c"] == 2  # fund + allocate
    finally:
        fresh.close()


# ---------------------------------------------------------------------------
# 冪等性（完了条件2）
# ---------------------------------------------------------------------------


def test_same_operation_id_replay_has_single_effect(test_db):
    """同一 operation_id の再送は 1 回分の効果しか生まない（replayed が分かる）。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    op_id = "fund:op-1"
    first = _fund(test_db, operation_id=op_id)
    assert first.replayed is False

    replay = _fund(test_db, operation_id=op_id)
    assert replay.replayed is True
    assert replay.data == first.data

    # 効果は 1 回分だけ（available は 100 のまま、二重入金されない）
    view = _view(test_db.conn)
    assert view.available_units == ROOT_BUDGET_UNITS
    assert view.deposit_units == ROOT_BUDGET_UNITS
    rows = test_db.conn.execute(
        "SELECT COUNT(*) AS c FROM operations WHERE operation_id = ?", (op_id,)
    ).fetchone()
    assert rows["c"] == 1
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_same_operation_id_different_payload_is_conflict(test_db):
    """同一 operation_id・別 payload は IDEMPOTENCY_CONFLICT。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    op_id = "fund:op-2"
    _fund(test_db, operation_id=op_id)
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS - 1,  # 別 payload
            operation_id=op_id,
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    assert _view(test_db.conn).available_units == ROOT_BUDGET_UNITS


def test_same_operation_id_different_actor_is_conflict(test_db):
    """同一 operation_id・別 Actor は IDEMPOTENCY_CONFLICT。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    op_id = "fund:op-3"
    _fund(test_db, operation_id=op_id)
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=SYSTEM_ID,  # 別 Actor
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
            operation_id=op_id,
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    assert _view(test_db.conn).available_units == ROOT_BUDGET_UNITS


# ---------------------------------------------------------------------------
# business_key（完了条件3）: 異なる operation_id で同じ業務効果は 1 回だけ
# ---------------------------------------------------------------------------


def test_double_fund_with_different_operation_ids_is_rejected(test_db):
    """二重 fund: 異なる operation_id でも fund:{root_id} は 1 回だけ。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    _fund(test_db, operation_id="fund:first")
    with pytest.raises(OjpError) as exc_info:
        _fund(test_db, operation_id="fund:second")
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    # 効果は 1 回分だけ
    assert _view(test_db.conn).available_units == ROOT_BUDGET_UNITS
    assert _view(test_db.conn).deposit_units == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_double_fund_blocked_by_business_key_even_with_extra_seed(test_db):
    """business_key が二重 fund を防ぐ: Wallet に残高があっても 2 回目は拒否される。

    残高不足（副作用の拒否）ではなく business_key UNIQUE による拒否を区別するため、
    Requester Wallet に 200 を seed しておき、2 回目の fund が残高不足に
    ならない状況でも拒否されることを確認する。
    """
    setup_ledger_demo_world(
        test_db.conn, ROOT_ID, requester_balance_units=ROOT_BUDGET_UNITS * 2
    )
    _fund(test_db, operation_id="fund:first")
    with pytest.raises(OjpError) as exc_info:
        _fund(test_db, operation_id="fund:second")
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    # 残高はあるのに business_key で拒否される（= 効果は 1 回分だけ）
    assert _view(test_db.conn).deposit_units == ROOT_BUDGET_UNITS
    assert _view(test_db.conn).available_units == ROOT_BUDGET_UNITS
    wallet = test_db.conn.execute(
        "SELECT balance_units FROM mock_wallets WHERE participant_id = ?",
        (REQUESTER_ID,),
    ).fetchone()
    assert wallet["balance_units"] == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_double_allocate_with_different_operation_ids_is_rejected(test_db):
    """同じ Child の二重拘束: 異なる operation_id でも allocate:{child_id} は 1 回だけ。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund(test_db)
    _allocate(test_db, operation_id="allocate:first")
    with pytest.raises(OjpError) as exc_info:
        _allocate(test_db, operation_id="allocate:second")
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    # child_work は 10 のまま（二重拘束されない）
    assert _view(test_db.conn).locked_breakdown_units["child_work"] == CHILD_BUDGET_UNITS
    assert _view(test_db.conn).available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_double_parent_refund_with_different_operation_ids_is_rejected(test_db):
    """同じ原資の二重返金予約: refund:{root_id}:terminal は 1 回だけ。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    _fund(test_db)
    service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id="reserve:refund-1",
    )
    with pytest.raises(OjpError) as exc_info:
        service.reserve_parent_refund(
            test_db.conn,
            actor_id=SYSTEM_ID,
            root_id=ROOT_ID,
            amount_units=ROOT_BUDGET_UNITS,
            operation_id="reserve:refund-2",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert _view(test_db.conn).locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


# ---------------------------------------------------------------------------
# Journal 合計 0（完了条件4）: 各操作単位でも累計でも 0
# ---------------------------------------------------------------------------


def test_journal_zero_sum_per_operation_and_cumulative(test_db):
    """MockWallet の入金前残高を含めた全口座間の Journal 合計が 0。

    各操作単位（1 operation の Entry 合計）でも累計でも 0。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID, seed_workers=True)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund(test_db)
    _allocate(test_db)
    service.reserve_child_payout(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        payee_id=AGENT_B_ID,
    )
    service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
    )

    # 各操作単位で 0
    assert ledger.check_journal_zero_sum(test_db.conn) == []
    # 累計でも 0（Wallet の入金前残高を含む）
    assert ledger.check_cumulative_journal_zero_sum(test_db.conn) == 0
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


# ---------------------------------------------------------------------------
# Parent 失敗後の Child 失敗（完了条件6: 計画書 N13 / X08 の会計部分）
# ---------------------------------------------------------------------------


def test_child_failure_after_parent_terminal_refund(test_db):
    """Parent 失敗後に Child 失敗が確定: child_work → available → refund を
    1 transaction で記録し、追加返金が業務キーで 1 回だけになる（N13）。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund(test_db)
    _allocate(test_db)

    # Parent 失敗: available 90 → refund 90（refund:{root_id}:terminal）
    service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
    )
    view = _view(test_db.conn)
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS

    # Child 失敗が確定: child_work 10 → available → refund（1 transaction）
    result = service.return_child_work(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        parent_terminal_refund_reserved=True,
        operation_id="return:child-after-parent-1",
    )
    assert result.data.get("combined") is True

    view = _view(test_db.conn)
    # 追加返金 10 が refund に加わり、累計 100
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS
    assert view.available_units == 0
    assert view.locked_breakdown_units["child_work"] == 0
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)

    # 途中の available を外から再利用させない: この transaction 内で
    # child_work → available → refund の 2 移動が 1 操作として記録される。
    rows = test_db.conn.execute(
        "SELECT e.entry_no, a.bucket, e.delta_units"
        " FROM journal_entries e"
        " JOIN budget_accounts a ON a.id = e.account_id"
        " WHERE e.operation_id = ? ORDER BY e.entry_no",
        ("return:child-after-parent-1",),
    ).fetchall()
    deltas = [(r["bucket"], r["delta_units"]) for r in rows]
    # child_work -10 / available +10 / available -10 / refund +10
    assert deltas == [
        ("child_work", -CHILD_BUDGET_UNITS),
        ("available", CHILD_BUDGET_UNITS),
        ("available", -CHILD_BUDGET_UNITS),
        ("refund", CHILD_BUDGET_UNITS),
    ]


def test_child_failure_after_parent_terminal_persists_in_one_transaction(test_db):
    """Parent 失敗後の Child 失敗が 1 transaction で永続されることを
    新しい接続で確認する（完了条件6・7）。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund(test_db)
    _allocate(test_db)
    service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
    )
    service.return_child_work(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        parent_terminal_refund_reserved=True,
        operation_id="return:persist-1",
    )

    fresh = test_db.fresh_conn()
    try:
        view = _view(fresh)
        assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS
        assert view.available_units == 0
        # 1 つの journal_transaction として永続されている
        row = fresh.execute(
            "SELECT reason FROM journal_transactions WHERE operation_id = ?",
            ("return:persist-1",),
        ).fetchone()
        assert row["reason"] == "child-return-after-parent-terminal"
        ledger.assert_ledger_invariants(fresh, ROOT_ID)
    finally:
        fresh.close()


def test_additional_refund_is_single_by_business_key(test_db):
    """追加返金（refund:{root_id}:child-return:{child_id}）は業務キーで 1 回だけ（X08）。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund(test_db)
    _allocate(test_db)
    service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
    )
    service.return_child_work(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        parent_terminal_refund_reserved=True,
        operation_id="return:first",
    )
    # 異なる operation_id で同じ追加返金を狙っても 1 回だけ
    with pytest.raises(OjpError) as exc_info:
        service.return_child_work(
            test_db.conn,
            actor_id=SYSTEM_ID,
            root_id=ROOT_ID,
            child_id=CHILD_ID,
            amount_units=CHILD_BUDGET_UNITS,
            parent_terminal_refund_reserved=True,
            operation_id="return:second",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    # 累計返金は 100 のまま（追加返金は 1 回だけ）
    assert _view(test_db.conn).locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


# ---------------------------------------------------------------------------
# 同じ Child を別 ID で二重 approve（計画書 第16節: 既存結果を返す）
# ---------------------------------------------------------------------------


def test_double_child_approve_with_different_ids_returns_existing(test_db):
    """同じ Child を別 operation_id で二重 approve しても効果は 1 回だけ。

    S2 では予約と PaymentOperation が同じ transaction で確定し、別 ID の
    再確定は business_key（payout:{child_id}）の照合で既存の結果を返す
    （計画書 第16節「既存 Acceptance と既存 PaymentOperation を返す」）。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID, seed_workers=True)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund(test_db)
    _allocate(test_db)
    first = service.reserve_child_payout(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        payee_id=AGENT_B_ID,
        operation_id="payout:approve-1",
    )
    # PaymentOperation はコマンド ID の派生 ID で確定される
    assert first.data["payment_operation_id"] == "payout:approve-1:payment"
    # 同一 ID 再送は replay
    replay = service.reserve_child_payout(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        payee_id=AGENT_B_ID,
        operation_id="payout:approve-1",
    )
    assert replay.replayed is True
    assert replay.data == first.data
    # 別 ID では business_key が照合され、新しい送金は作られず既存の結果を返す
    second = service.reserve_child_payout(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        payee_id=AGENT_B_ID,
        operation_id="payout:approve-2",
    )
    assert second.replayed is True
    assert second.data["payment_operation_id"] == "payout:approve-1:payment"
    # PaymentOperation は 1 件だけ（二重送金の種を作らない）
    rows = test_db.conn.execute(
        "SELECT COUNT(*) AS c FROM payment_operations"
        " WHERE business_key = ?",
        (f"payout:{CHILD_ID}",),
    ).fetchone()
    assert rows["c"] == 1
    # 効果は 1 回分（child_payout は 10 のまま）
    assert _view(test_db.conn).locked_breakdown_units["child_payout"] == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)
