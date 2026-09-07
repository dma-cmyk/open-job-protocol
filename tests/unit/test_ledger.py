"""台帳の unit テスト: 保存則・非負・Journal 合計 0・Root funding 異常系・no-op。

観察可能な契約（保存則・非負・冪等性・業務キー・拒否理由）を検証する。
"""

from __future__ import annotations

import pytest

from ojp import ledger, service
from ojp.domain import ErrorCode, JobState, OjpError
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    CHILD_BUDGET_UNITS,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    force_job_state,
    insert_child_job,
    insert_root_job,
    setup_ledger_demo_world,
)

ROOT_ID = "job-root-1"
CHILD_ID = "job-child-1"


def _view(handle):
    return ledger.get_root_ledger_view(handle.conn, ROOT_ID)


def _wallet(conn, participant_id):
    row = conn.execute(
        "SELECT balance_units FROM mock_wallets WHERE participant_id = ?",
        (participant_id,),
    ).fetchone()
    return int(row["balance_units"])


def test_fund_then_conservation_holds_with_zero_paid_refunded(test_db):
    """完了条件1: fund 後に D = available + locked + paid + refunded、全口座非負。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    result = service.fund_root(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
    )
    assert result.replayed is False
    view = _view(test_db)
    assert view.deposit_units == ROOT_BUDGET_UNITS
    assert view.available_units == ROOT_BUDGET_UNITS
    assert view.locked_units == 0
    assert view.paid_units == 0
    assert view.refunded_units == 0
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_reserve_and_return_keep_conservation_and_non_negative(test_db):
    """完了条件1: reserve（child_work・child_payout・parent_payout・refund）と
    return の各操作後に保存則・非負が成立する。予約だけでは paid/refunded は
    増えない（Receipt がある時だけ増える）。"""
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

    # Child create 10: available 100 -> 90, child_work 0 -> 10
    service.allocate_child_work(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
    )
    view = _view(test_db)
    assert view.available_units == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    assert view.locked_breakdown_units["child_work"] == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)

    # Child approval: child_work 10 -> child_payout 10
    service.reserve_child_payout(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        payee_id=AGENT_B_ID,
    )
    view = _view(test_db)
    assert view.locked_breakdown_units["child_work"] == 0
    assert view.locked_breakdown_units["child_payout"] == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)

    # Parent approval: available 90 -> parent_payout 90
    # ※ Phase 4 の approve 経路が入るまでの暫定的な組み立てとして、Parent を
    #   DONE にしてから予約する（S3 で reserve_parent_payout は Parent DONE
    #   のときだけ許される。計画書 第8節の表）。
    force_job_state(test_db.conn, ROOT_ID, JobState.DONE)
    service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        payee_id=AGENT_A_ID,
    )
    view = _view(test_db)
    assert view.available_units == 0
    assert view.locked_breakdown_units["parent_payout"] == (
        ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    )
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)
    assert view.deposit_units == ROOT_BUDGET_UNITS
    assert view.paid_units == 0
    assert view.refunded_units == 0


def test_child_failure_returns_to_available(test_db):
    """Child failure: child_work -10 / available +10（return:{child_id}）。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
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
    service.return_child_work(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
    )
    view = _view(test_db)
    assert view.available_units == ROOT_BUDGET_UNITS
    assert view.locked_units == 0
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_parent_failure_reserves_refund(test_db):
    """Parent failure: available -90 / refund +90（refund:{root_id}:terminal）。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
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
    service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
    )
    view = _view(test_db)
    assert view.available_units == 0
    assert view.locked_breakdown_units["refund"] == (
        ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    )
    assert view.locked_breakdown_units["child_work"] == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_negative_balance_move_is_rejected(test_db):
    """全口座非負: 残高を超える移動は CHECK 制約違反をドメインエラーへ変換して拒否。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    service.fund_root(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
    )
    # available 100 を超える parent_payout 予約 101 は拒否される。
    # ※ Phase 4 の approve 経路が入るまでの暫定的な組み立てとして Parent を
    #   DONE にする（S3 で reserve_parent_payout は Parent DONE のときだけ
    #   許される。計画書 第8節の表）。
    force_job_state(test_db.conn, ROOT_ID, JobState.DONE)
    with pytest.raises(OjpError) as exc_info:
        service.reserve_parent_payout(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            amount_units=ROOT_BUDGET_UNITS + 1_000_000,
            payee_id=AGENT_A_ID,
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    # 拒否後も口座は不変
    view = _view(test_db)
    assert view.available_units == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


# ---------------------------------------------------------------------------
# Root funding の異常系（完了条件5）
# ---------------------------------------------------------------------------


def _seed_and_reject(test_db, *, expected, amount, match_code=ErrorCode.INVALID_ARGUMENT):
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,
            expected_amount_units=expected,
            amount_units=amount,
        )
    assert exc_info.value.code == match_code.value
    return exc_info


def test_fund_amount_shortfall_is_rejected(test_db):
    """金額不足の入金は拒否（正確な全額入金のみ）。"""
    _seed_and_reject(
        test_db, expected=ROOT_BUDGET_UNITS, amount=ROOT_BUDGET_UNITS - 1
    )
    view = _view(test_db)
    assert view.deposit_units == 0
    assert view.available_units == 0


def test_fund_amount_excess_is_rejected(test_db):
    """超過入金は拒否（正確な全額入金のみ）。"""
    _seed_and_reject(
        test_db, expected=ROOT_BUDGET_UNITS, amount=ROOT_BUDGET_UNITS + 1
    )
    assert _view(test_db).deposit_units == 0


def test_fund_zero_amount_is_rejected(test_db):
    """ゼロ入金は拒否。"""
    _seed_and_reject(test_db, expected=ROOT_BUDGET_UNITS, amount=0)


def test_fund_without_seed_is_rejected(test_db):
    """seed 経路を経ない Wallet からの引落しは拒否（seed 悪用の防止）。

    fund_root は「既に seed された Wallet」からの引落しのみを行い、
    Wallet が無ければ INVALID_STATE で拒否する。
    """
    from ojp import db as dbmod
    from tests.conftest import insert_participant
    from ojp.domain import ParticipantKind

    with dbmod.transaction(test_db.conn, immediate=True):
        insert_participant(test_db.conn, REQUESTER_ID, ParticipantKind.HUMAN)
        insert_root_job(test_db.conn, ROOT_ID)
        # seed しない
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert _view(test_db).deposit_units == 0


def test_fund_insufficient_wallet_balance_is_rejected(test_db):
    """Wallet 残高不足の引落しは拒否（非負制約のドメイン変換）。"""
    setup_ledger_demo_world(
        test_db.conn, ROOT_ID, requester_balance_units=ROOT_BUDGET_UNITS - 1
    )
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert _view(test_db).deposit_units == 0


def test_fund_does_not_move_between_roots(test_db):
    """Root 間の資金移動の拒否: 別 Root の available は fund の原資に使えない。

    fund_root は Requester の MockWallet からしか引き落とせず、
    別 Root の Escrow available を原資にする入力経路を持たない。
    ここでは、別 Root への fund が先に存在しても、2 つ目の Root の fund は
    「自身の Requester Wallet からの全額引落し」だけを行い、1 つ目の Root の
    available を一切減らさないことを確認する。
    """
    other_root = "job-root-other"
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_root_job(test_db.conn, other_root)
    # Requester Wallet には 100 しか無いので、両方の Root へ 100 ずつ fund すると
    # 2 回目は残高不足で拒否される（別 Root の available は原資に使えない）。
    service.fund_root(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
    )
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=other_root,
            requester_id=REQUESTER_ID,
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    other_view = ledger.get_root_ledger_view(test_db.conn, other_root)
    assert other_view.deposit_units == 0
    # 1 つ目の Root は不変
    assert _view(test_db).available_units == ROOT_BUDGET_UNITS


# ---------------------------------------------------------------------------
# fund_root の正本照合（呼出側の申告を信用しない）
# ---------------------------------------------------------------------------


def test_fund_rejects_child_job_target(test_db):
    """Child を fund 対象にはできない（INVALID_TARGET）。

    fund_root は対象 Job が Root（parent_id IS NULL かつ root_id = id）である
    ことを DB で確認する。Child には available 口座が無く、入金先として
    解決できない。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=AGENT_A_ID,
            root_id=CHILD_ID,
            requester_id=AGENT_A_ID,
            expected_amount_units=CHILD_BUDGET_UNITS,
            amount_units=CHILD_BUDGET_UNITS,
        )
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value
    assert _view(test_db).deposit_units == 0
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_fund_rejects_missing_version(test_db):
    """公開 Version 行が無い Root は INVALID_STATE（jobs.version_id 未設定）。"""
    from ojp import db as dbmod
    from tests.conftest import insert_participant
    from ojp.domain import ParticipantKind

    with dbmod.transaction(test_db.conn, immediate=True):
        insert_participant(test_db.conn, REQUESTER_ID, ParticipantKind.HUMAN)
        # Version 行を作らず jobs 行だけを直接 INSERT する
        test_db.conn.execute(
            "INSERT INTO jobs (id, root_id, parent_id, requester_id, state,"
            " row_version, created_at_us) VALUES (?, ?, NULL, ?, 'DRAFT', 0, 0)",
            (ROOT_ID, ROOT_ID, REQUESTER_ID),
        )
        ledger.seed_mock_wallet_for_demo(
            test_db.conn,
            participant_id=REQUESTER_ID,
            asset="mock-USDC",
            balance_units=ROOT_BUDGET_UNITS,
        )
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert _view(test_db).deposit_units == 0


def test_fund_rejects_wrong_requester_and_amount(test_db):
    """呼出側の申告（requester_id・expected_amount_units）が DB の正本と
    違えば FORBIDDEN。呼出側の値を黙って採用しない。

    - 「別 Actor の Wallet を原資にする」: jobs.requester_id と違う
      requester_id は拒否される（SYSTEM の Wallet から引き落とせない）
    - 「公開予算と違う額を全額入金とみなす」: job_versions.budget_units と
      違う expected_amount_units は拒否される
    いずれも Wallet・台帳・operations は一切動かない。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID, seed_workers=True)
    ledger.seed_mock_wallet_for_demo(
        test_db.conn,
        participant_id=SYSTEM_ID,
        asset="mock-USDC",
        balance_units=ROOT_BUDGET_UNITS,
    )
    # 別 Actor の Wallet を原資にしようとする入力は FORBIDDEN
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=SYSTEM_ID,
            root_id=ROOT_ID,
            requester_id=SYSTEM_ID,  # jobs.requester_id と食い違う
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    # 拒否メッセージに Root Requester の正本の participant id を含まない
    assert REQUESTER_ID not in exc_info.value.message
    # 公開予算と違う額を「全額入金」と申告しても FORBIDDEN
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,
            expected_amount_units=ROOT_BUDGET_UNITS - 1,  # 公開予算と食い違う
            amount_units=ROOT_BUDGET_UNITS - 1,
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    # 拒否メッセージに正本の公開予算額を含まない（権限のない相手へ予算を開示しない）
    assert str(ROOT_BUDGET_UNITS) not in exc_info.value.message
    # いずれも何も動いていない（正本解決に失敗した fund は記録されない）
    assert _view(test_db).deposit_units == 0
    assert _wallet(test_db.conn, REQUESTER_ID) == ROOT_BUDGET_UNITS
    assert _wallet(test_db.conn, SYSTEM_ID) == ROOT_BUDGET_UNITS
    assert (
        test_db.conn.execute(
            "SELECT COUNT(*) AS c FROM operations WHERE kind = 'fund'"
        ).fetchone()["c"]
        == 0
    )
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_fund_rejects_actor_other_than_root_requester(test_db):
    """Root Requester ではない別 Actor は fund を実行できない（FORBIDDEN）。

    仕様（第5節）: Root作成・入金・Root承認は Root Requester。actor_id を
    呼出側が申告するだけで、requester_id に正しい値を渡しても権限の
    代替にはならない。拒否後は Root の available・Requester の Wallet・
    Journal・operations のいずれも変化しない。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID, seed_workers=True)
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=AGENT_A_ID,  # Root Requester ではない別 participant
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,  # 申告は正しい値
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    # 拒否メッセージは主体を特定できない一般表現で、
    # Root Requester の participant id を含まない（権限のない相手へ所有者を開示しない）
    assert REQUESTER_ID not in exc_info.value.message
    # 状態は一切変化しない
    assert _view(test_db).available_units == 0
    assert _wallet(test_db.conn, REQUESTER_ID) == ROOT_BUDGET_UNITS
    assert (
        test_db.conn.execute(
            "SELECT COUNT(*) AS c FROM journal_transactions"
        ).fetchone()["c"]
        == 0
    )
    assert (
        test_db.conn.execute(
            "SELECT COUNT(*) AS c FROM operations"
        ).fetchone()["c"]
        == 0
    )
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_fund_rejects_unauthorized_actor_before_version_state(test_db):
    """公開 Version の無い Root に無権限の Actor が fund すると FORBIDDEN。

    検査順序は Root 性 → Actor 権限 → 申告 requester_id → 公開予算 → asset。
    権限のない Actor に公開状態（Version の有無）を漏らさないため、
    「公開 Version なし × Root Requester ではない Actor」は INVALID_STATE
    ではなく FORBIDDEN で拒否される。拒否後は available・Requester Wallet・
    journal_transactions・operations のいずれも変化しない。
    """
    from ojp import db as dbmod
    from tests.conftest import insert_participant
    from ojp.domain import ParticipantKind

    with dbmod.transaction(test_db.conn, immediate=True):
        insert_participant(test_db.conn, REQUESTER_ID, ParticipantKind.HUMAN)
        insert_participant(test_db.conn, AGENT_A_ID, ParticipantKind.AGENT)
        # Version 行を作らず jobs 行だけを直接 INSERT する（公開状態なし）
        test_db.conn.execute(
            "INSERT INTO jobs (id, root_id, parent_id, requester_id, state,"
            " row_version, created_at_us) VALUES (?, ?, NULL, ?, 'DRAFT', 0, 0)",
            (ROOT_ID, ROOT_ID, REQUESTER_ID),
        )
        ledger.seed_mock_wallet_for_demo(
            test_db.conn,
            participant_id=REQUESTER_ID,
            asset="mock-USDC",
            balance_units=ROOT_BUDGET_UNITS,
        )
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=AGENT_A_ID,  # Root Requester ではない別 participant
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,  # 申告は正しい値
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
        )
    # 公開 Version が無くても、Actor 権限の検査が先なので FORBIDDEN
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    # 状態は一切変化しない
    assert _view(test_db).available_units == 0
    assert _wallet(test_db.conn, REQUESTER_ID) == ROOT_BUDGET_UNITS
    assert (
        test_db.conn.execute(
            "SELECT COUNT(*) AS c FROM journal_transactions"
        ).fetchone()["c"]
        == 0
    )
    assert (
        test_db.conn.execute(
            "SELECT COUNT(*) AS c FROM operations"
        ).fetchone()["c"]
        == 0
    )
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_fund_rejects_non_mock_usdc_asset(test_db):
    """公開 Version の asset が mock-USDC 以外なら INVALID_STATE。"""
    from ojp import db as dbmod
    from tests.conftest import (
        insert_participant,
        insert_root_job as insert_root_with_version,
    )
    from ojp.domain import ParticipantKind

    with dbmod.transaction(test_db.conn, immediate=True):
        insert_participant(test_db.conn, REQUESTER_ID, ParticipantKind.HUMAN)
        insert_root_with_version(
            test_db.conn, ROOT_ID, requester_id=REQUESTER_ID, asset="other-asset"
        )
        ledger.seed_mock_wallet_for_demo(
            test_db.conn,
            participant_id=REQUESTER_ID,
            asset="mock-USDC",
            balance_units=ROOT_BUDGET_UNITS,
        )
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            requester_id=REQUESTER_ID,
            expected_amount_units=ROOT_BUDGET_UNITS,
            amount_units=ROOT_BUDGET_UNITS,
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert _view(test_db).deposit_units == 0


# ---------------------------------------------------------------------------
# no-op（完了条件: 残額 0 なら支払い Operation を作らず正常な no-op）
# ---------------------------------------------------------------------------


def test_zero_amount_reserve_is_normal_no_op(test_db):
    """残額 0 の予約は正常な no-op（エラーにしない）。

    「支払い Operation を作らない」とは、送金処理の PaymentOperation と
    資金移動（Journal）を作らないという意味。再送を同じ no-op として
    返すための Operation 記録自体は残る。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    # ※ Phase 4 の approve 経路が入るまでの暫定的な組み立てとして Parent を
    #   DONE にする（S3 で reserve_parent_payout は Parent DONE のときだけ
    #   許される。計画書 第8節の表）。
    force_job_state(test_db.conn, ROOT_ID, JobState.DONE)
    result = service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=0,
        payee_id=AGENT_A_ID,
    )
    assert result.data.get("no_op") is True
    # no-op では資金移動（journal）は記録されない
    rows = test_db.conn.execute(
        "SELECT COUNT(*) AS c FROM journal_transactions"
    ).fetchone()
    assert rows["c"] == 0
    # 再送は同じ no-op を replay する
    replay = service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=0,
        payee_id=AGENT_A_ID,
        operation_id=result.operation_id,
    )
    assert replay.replayed is True
    assert replay.data.get("no_op") is True
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_zero_amount_return_is_normal_no_op(test_db):
    """残額 0 の返却も正常な no-op（資金移動なし）。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    result = service.return_child_work(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=0,
    )
    assert result.data.get("no_op") is True
    rows = test_db.conn.execute(
        "SELECT COUNT(*) AS c FROM journal_transactions"
    ).fetchone()
    assert rows["c"] == 0
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


def _audit_snapshot(conn):
    """拒否の前後比較用スナップショット（operations・payment_operations・
    Journal・口座残高・Job 状態の一切が不変であることの検証）。"""

    def _rows(sql):
        return tuple(tuple(r) for r in conn.execute(sql).fetchall())

    return (
        _rows("SELECT * FROM operations ORDER BY operation_id"),
        _rows("SELECT * FROM payment_operations ORDER BY operation_id"),
        _rows("SELECT * FROM journal_transactions ORDER BY operation_id"),
        _rows("SELECT * FROM journal_entries ORDER BY operation_id, entry_no"),
        _rows("SELECT * FROM budget_accounts ORDER BY id"),
        _rows("SELECT id, state, row_version FROM jobs ORDER BY id"),
    )


def _fund_root(test_db):
    service.fund_root(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
    )


def _allocate_child(test_db):
    service.allocate_child_work(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
    )


def test_return_child_work_zero_noop_keeps_return_business_key(test_db):
    """return_child_work(amount_units=0) は正常 no-op で return:{child_id} を
    消費しない（Parent 生存中の通常返却経路）。

    - 主 Operation は business_key=NULL で保存され、Journal・残高は不変
    - 同一ゼロ ID の再送は replayed=True で同じ結果
    - その後の別 ID 正額返却が成功し、return:{child_id} は 1 件だけ確定
    - 正額への別 ID 二重試行は INVALID_STATE（二重効果なし）
    - ゼロ→正額への同一 ID 流用は IDEMPOTENCY_CONFLICT
    - 負数は INVALID_ARGUMENT で Operation を残さない
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund_root(test_db)
    _allocate_child(test_db)

    journal_before = _journal_count(test_db.conn)
    view_before = _view(test_db)

    zero = service.return_child_work(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=0,
        operation_id="return:zero-1",
    )
    assert zero.data.get("no_op") is True
    assert _op_business_key(test_db.conn, "return:zero-1") is None
    assert _journal_count(test_db.conn) == journal_before
    assert _view(test_db) == view_before  # 残高不変
    assert _bk_count(test_db.conn, f"return:{CHILD_ID}") == 0

    # 同一ゼロ ID の再送は同じ結果を replay する
    replay = service.return_child_work(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=0,
        operation_id="return:zero-1",
    )
    assert replay.replayed is True
    assert replay.data == zero.data

    # 負数はゼロ特例に含めず INVALID_ARGUMENT（rollback で何も残さない）
    with pytest.raises(OjpError) as exc_info:
        service.return_child_work(
            test_db.conn,
            actor_id=AGENT_A_ID,
            root_id=ROOT_ID,
            child_id=CHILD_ID,
            amount_units=-1,
            operation_id="return:neg-1",
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert not _op_exists(test_db.conn, "return:neg-1")

    # 別 ID の正額返却が成功する（ゼロが業務キーを消費していないことの反例）
    real = service.return_child_work(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        operation_id="return:real-1",
    )
    assert real.data["amount_units"] == CHILD_BUDGET_UNITS
    assert _bk_count(test_db.conn, f"return:{CHILD_ID}") == 1
    view = _view(test_db)
    assert view.available_units == ROOT_BUDGET_UNITS
    assert view.locked_breakdown_units["child_work"] == 0

    # 正額への別 ID 二重試行は INVALID_STATE
    with pytest.raises(OjpError) as exc_info:
        service.return_child_work(
            test_db.conn,
            actor_id=AGENT_A_ID,
            root_id=ROOT_ID,
            child_id=CHILD_ID,
            amount_units=CHILD_BUDGET_UNITS,
            operation_id="return:real-2",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert _view(test_db) == view  # 二重効果なし

    # ゼロで使った ID を正額へ流用すると IDEMPOTENCY_CONFLICT
    with pytest.raises(OjpError) as exc_info:
        service.return_child_work(
            test_db.conn,
            actor_id=AGENT_A_ID,
            root_id=ROOT_ID,
            child_id=CHILD_ID,
            amount_units=CHILD_BUDGET_UNITS,
            operation_id="return:zero-1",
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_reserve_parent_payout_zero_noop_keeps_payout_business_key(test_db):
    """reserve_parent_payout(amount_units=0) は正常 no-op で payout:{root_id} を
    消費しない。

    - 主 Operation は business_key=NULL、派生 Operation と PaymentOperation は無い
    - Journal・残高は不変
    - 同一ゼロ ID の再送は replayed=True
    - その後の別 ID 正額予約が成功し、payout:{root_id} と PaymentOperation は 1 件
    - 正額への別 ID 再確定は既存 PaymentOperation の結果を返す（二重効果なし）
    - 正額効果が既にある状態から別 ID のゼロを呼ぶと正常なゼロ no-op
    - ゼロ→正額への同一 ID 流用は IDEMPOTENCY_CONFLICT
    - 負数は INVALID_ARGUMENT で何も残さない
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    _fund_root(test_db)
    force_job_state(test_db.conn, ROOT_ID, JobState.DONE)

    journal_before = _journal_count(test_db.conn)
    view_before = _view(test_db)

    zero = service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=0,
        payee_id=AGENT_A_ID,
        operation_id="payout:parent-zero",
    )
    assert zero.data.get("no_op") is True
    assert "payment_operation_id" not in zero.data
    assert _op_business_key(test_db.conn, "payout:parent-zero") is None
    # 派生 Operation（<operation_id>:payment）も PaymentOperation も作らない
    assert not _op_exists(test_db.conn, "payout:parent-zero:payment")
    assert _payment_count(test_db.conn) == 0
    assert _journal_count(test_db.conn) == journal_before
    assert _view(test_db) == view_before
    assert _bk_count(test_db.conn, f"payout:{ROOT_ID}") == 0

    # 同一ゼロ ID の再送は同じ結果を replay する
    replay = service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=0,
        payee_id=AGENT_A_ID,
        operation_id="payout:parent-zero",
    )
    assert replay.replayed is True
    assert replay.data == zero.data

    # 負数は INVALID_ARGUMENT で何も残さない
    with pytest.raises(OjpError) as exc_info:
        service.reserve_parent_payout(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            amount_units=-1,
            payee_id=AGENT_A_ID,
            operation_id="payout:parent-neg",
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert not _op_exists(test_db.conn, "payout:parent-neg")

    # 別 ID の正額予約が成功する
    real = service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS,
        payee_id=AGENT_A_ID,
        operation_id="payout:parent-real",
    )
    assert real.data["payment_operation_id"] == "payout:parent-real:payment"
    assert _bk_count(test_db.conn, f"payout:{ROOT_ID}") == 1
    assert _payment_count(test_db.conn, f"payout:{ROOT_ID}") == 1
    view = _view(test_db)
    assert view.available_units == 0
    assert view.locked_breakdown_units["parent_payout"] == ROOT_BUDGET_UNITS

    # 正額への別 ID 再確定は既存 PaymentOperation の結果を返す（二重効果なし）
    second = service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS,
        payee_id=AGENT_A_ID,
        operation_id="payout:parent-second",
    )
    assert second.replayed is True
    assert second.data["payment_operation_id"] == "payout:parent-real:payment"
    assert _payment_count(test_db.conn, f"payout:{ROOT_ID}") == 1

    # 正額効果が既にある状態から別 ID のゼロを呼ぶと、例外でも既存正額結果の
    # 再利用でもなく、正常なゼロ no-op になる
    zero_after = service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=0,
        payee_id=AGENT_A_ID,
        operation_id="payout:parent-zero-after",
    )
    assert zero_after.data.get("no_op") is True
    assert "payment_operation_id" not in zero_after.data
    assert _op_business_key(test_db.conn, "payout:parent-zero-after") is None

    # ゼロで使った ID を正額へ流用すると IDEMPOTENCY_CONFLICT
    with pytest.raises(OjpError) as exc_info:
        service.reserve_parent_payout(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            amount_units=ROOT_BUDGET_UNITS,
            payee_id=AGENT_A_ID,
            operation_id="payout:parent-zero",
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_reserve_parent_refund_zero_noop_keeps_refund_business_key(test_db):
    """reserve_parent_refund(amount_units=0, payee_id=None) は正常 no-op で
    refund:{root_id}:terminal を消費しない。

    - 主 Operation は business_key=NULL、PaymentOperation は無い
    - Journal・残高は不変
    - 同一ゼロ ID の再送は replayed=True
    - その後の別 ID 正額予約が成功し、refund:{root_id}:terminal は 1 件
    - 正額への別 ID 二重予約は INVALID_STATE（payee_id=None の契約）
    - 正額効果が既にある状態から別 ID のゼロを呼ぶと正常なゼロ no-op
    - ゼロ→正額への同一 ID 流用は IDEMPOTENCY_CONFLICT
    - 負数は INVALID_ARGUMENT で何も残さない
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund_root(test_db)

    journal_before = _journal_count(test_db.conn)
    view_before = _view(test_db)

    zero = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=0,
        operation_id="reserve:refund-zero",
    )
    assert zero.data.get("no_op") is True
    assert _op_business_key(test_db.conn, "reserve:refund-zero") is None
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
        operation_id="reserve:refund-zero",
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
            operation_id="reserve:refund-neg",
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert not _op_exists(test_db.conn, "reserve:refund-neg")

    # 別 ID の正額予約が成功する
    real = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        operation_id="reserve:refund-real",
    )
    assert real.data["amount_units"] == ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    assert _bk_count(test_db.conn, f"refund:{ROOT_ID}:terminal") == 1
    view = _view(test_db)
    assert view.available_units == CHILD_BUDGET_UNITS
    assert view.locked_breakdown_units["refund"] == (
        ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS
    )

    # 正額への別 ID 二重予約は INVALID_STATE（payee_id=None の既存契約）
    with pytest.raises(OjpError) as exc_info:
        service.reserve_parent_refund(
            test_db.conn,
            actor_id=SYSTEM_ID,
            root_id=ROOT_ID,
            amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
            operation_id="reserve:refund-second",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert _view(test_db) == view  # 二重効果なし

    # 正額効果が既にある状態から別 ID のゼロを呼ぶと正常なゼロ no-op
    zero_after = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=0,
        operation_id="reserve:refund-zero-after",
    )
    assert zero_after.data.get("no_op") is True
    assert _op_business_key(test_db.conn, "reserve:refund-zero-after") is None

    # ゼロで使った ID を正額へ流用すると IDEMPOTENCY_CONFLICT
    with pytest.raises(OjpError) as exc_info:
        service.reserve_parent_refund(
            test_db.conn,
            actor_id=SYSTEM_ID,
            root_id=ROOT_ID,
            amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
            operation_id="reserve:refund-zero",
        )
    assert exc_info.value.code == ErrorCode.IDEMPOTENCY_CONFLICT.value
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_allocate_child_work_zero_is_invalid_argument_and_leaves_nothing(test_db):
    """allocate_child_work(amount_units=0) はゼロ特例に含めない
    （INVALID_ARGUMENT で transaction 全体が rollback し、Operation・
    business_key・Journal を残さない）。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund_root(test_db)
    with pytest.raises(OjpError) as exc_info:
        service.allocate_child_work(
            test_db.conn,
            actor_id=AGENT_A_ID,
            root_id=ROOT_ID,
            child_id=CHILD_ID,
            amount_units=0,
            operation_id="allocate:zero",
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert not _op_exists(test_db.conn, "allocate:zero")
    assert _bk_count(test_db.conn, f"allocate:{CHILD_ID}") == 0
    assert _journal_count(test_db.conn) == 1  # fund のみ
    assert _view(test_db).available_units == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


# ---------------------------------------------------------------------------
# 正額確定後の負数は INVALID_ARGUMENT（業務キー照会より前の引数検査）
# ---------------------------------------------------------------------------


def test_negative_after_real_return_child_work_is_invalid_argument(test_db):
    """正額の通常返却（return:{child_id}）を確定させた後、別 operation_id で
    負数を渡すと INVALID_ARGUMENT になる（業務キー競合の INVALID_STATE ではなく）。
    拒否は _run_idempotent より前なので Operation 行も増えない。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund_root(test_db)
    _allocate_child(test_db)
    real = service.return_child_work(
        test_db.conn,
        actor_id=AGENT_A_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        operation_id="return:real",
    )
    assert real.replayed is False
    before = _audit_snapshot(test_db.conn)

    with pytest.raises(OjpError) as exc_info:
        service.return_child_work(
            test_db.conn,
            actor_id=AGENT_A_ID,
            root_id=ROOT_ID,
            child_id=CHILD_ID,
            amount_units=-1,
            operation_id="return:neg-after-real",
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert not _op_exists(test_db.conn, "return:neg-after-real")
    assert _audit_snapshot(test_db.conn) == before
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_negative_after_real_child_return_after_parent_terminal_is_invalid_argument(
    test_db,
):
    """Parent 終端後の追加返金（refund:{root_id}:child-return:{child_id}）を
    正額で確定させた後、別 operation_id で負数を渡すと INVALID_ARGUMENT に
    なる（業務キー競合の INVALID_STATE ではなく）。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund_root(test_db)
    _allocate_child(test_db)
    service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
    )
    real = service.return_child_work(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        child_id=CHILD_ID,
        amount_units=CHILD_BUDGET_UNITS,
        parent_terminal_refund_reserved=True,
        operation_id="return:child-real",
    )
    assert real.data.get("combined") is True
    before = _audit_snapshot(test_db.conn)

    with pytest.raises(OjpError) as exc_info:
        service.return_child_work(
            test_db.conn,
            actor_id=SYSTEM_ID,
            root_id=ROOT_ID,
            child_id=CHILD_ID,
            amount_units=-1,
            parent_terminal_refund_reserved=True,
            operation_id="return:child-neg-after-real",
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert not _op_exists(test_db.conn, "return:child-neg-after-real")
    assert _audit_snapshot(test_db.conn) == before
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_negative_after_real_reserve_parent_payout_is_invalid_argument(test_db):
    """正額の Parent payout を確定させた後、別 operation_id で負数を渡すと
    INVALID_ARGUMENT になる（既存の成功結果を返さない）。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID, seed_workers=True)
    _fund_root(test_db)
    force_job_state(test_db.conn, ROOT_ID, JobState.DONE)
    real = service.reserve_parent_payout(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS,
        payee_id=AGENT_A_ID,
        operation_id="payout:parent-real",
    )
    assert real.replayed is False
    before = _audit_snapshot(test_db.conn)

    with pytest.raises(OjpError) as exc_info:
        service.reserve_parent_payout(
            test_db.conn,
            actor_id=REQUESTER_ID,
            root_id=ROOT_ID,
            amount_units=-1,
            payee_id=AGENT_A_ID,
            operation_id="payout:parent-neg-after-real",
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert not _op_exists(test_db.conn, "payout:parent-neg-after-real")
    assert _audit_snapshot(test_db.conn) == before
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_negative_after_real_reserve_parent_refund_is_invalid_argument(test_db):
    """正額の Parent refund（payee_id=None）を確定させた後、別 operation_id で
    負数を渡すと INVALID_ARGUMENT になる（業務キー競合の INVALID_STATE ではなく）。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund_root(test_db)
    _allocate_child(test_db)
    real = service.reserve_parent_refund(
        test_db.conn,
        actor_id=SYSTEM_ID,
        root_id=ROOT_ID,
        amount_units=ROOT_BUDGET_UNITS - CHILD_BUDGET_UNITS,
        operation_id="reserve:refund-real",
    )
    assert real.replayed is False
    before = _audit_snapshot(test_db.conn)

    with pytest.raises(OjpError) as exc_info:
        service.reserve_parent_refund(
            test_db.conn,
            actor_id=SYSTEM_ID,
            root_id=ROOT_ID,
            amount_units=-1,
            operation_id="reserve:refund-neg-after-real",
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert not _op_exists(test_db.conn, "reserve:refund-neg-after-real")
    assert _audit_snapshot(test_db.conn) == before
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


def test_bool_amount_units_is_invalid_argument_for_return(test_db):
    """return_child_work の amount_units に bool を渡すと INVALID_ARGUMENT
    （bool は int として受理しない）。
    """
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
    insert_child_job(test_db.conn, ROOT_ID, CHILD_ID)
    _fund_root(test_db)
    _allocate_child(test_db)
    before = _audit_snapshot(test_db.conn)
    for bad in (True, False):
        with pytest.raises(OjpError) as exc_info:
            service.return_child_work(
                test_db.conn,
                actor_id=AGENT_A_ID,
                root_id=ROOT_ID,
                child_id=CHILD_ID,
                amount_units=bad,
                operation_id=f"return:bad-{bad}",
            )
        assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
        assert _audit_snapshot(test_db.conn) == before
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


# ---------------------------------------------------------------------------
# Child DONE かつ送金障害中の失敗返却禁止（計画書 第9節）
# ---------------------------------------------------------------------------


def test_failure_return_forbidden_when_child_done_payment_pending(test_db):
    """Child が DONE だが送金障害中の場合は失敗返却を禁止する。"""
    setup_ledger_demo_world(test_db.conn, ROOT_ID)
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
    with pytest.raises(OjpError) as exc_info:
        service.return_child_work(
            test_db.conn,
            actor_id=AGENT_A_ID,
            root_id=ROOT_ID,
            child_id=CHILD_ID,
            amount_units=CHILD_BUDGET_UNITS,
            child_done_payment_pending=True,
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    # 拒否後も拘束は維持される
    view = _view(test_db)
    assert view.locked_breakdown_units["child_work"] == CHILD_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, ROOT_ID)


# ---------------------------------------------------------------------------
# canonical JSON（payload_hash の規則固定）
# ---------------------------------------------------------------------------


def test_canonical_json_dumps_key_order_and_unicode():
    """canonical JSON はキー順固定・非 ASCII をエスケープしない。"""
    a = ledger.canonical_json_dumps({"b": 1, "あ": "x", "a": [2, 3]})
    b = ledger.canonical_json_dumps({"a": [2, 3], "あ": "x", "b": 1})
    assert a == b
    assert a == '{"a":[2,3],"b":1,"あ":"x"}'
    assert "\\u" not in a  # 非 ASCII をエスケープしない


def test_canonical_json_dumps_rejects_non_json():
    """dict/list/str/int/float/bool/None 以外は拒否する。"""
    with pytest.raises(OjpError):
        ledger.canonical_json_dumps({"x": object()})
    with pytest.raises(OjpError):
        ledger.canonical_json_dumps({"x": float("nan")})


# ---------------------------------------------------------------------------
# seed の冪等性と区別（デモ準備専用経路の契約）
# ---------------------------------------------------------------------------


def test_seed_is_idempotent_and_rejects_different_balance(test_db):
    """seed は冪等（同額再実行は no-op）で、別額は拒否する。"""
    from ojp import db as dbmod
    from tests.conftest import insert_participant
    from ojp.domain import ParticipantKind

    with dbmod.transaction(test_db.conn, immediate=True):
        insert_participant(test_db.conn, REQUESTER_ID, ParticipantKind.HUMAN)
        ledger.seed_mock_wallet_for_demo(
            test_db.conn,
            participant_id=REQUESTER_ID,
            asset="mock-USDC",
            balance_units=ROOT_BUDGET_UNITS,
        )
        # 同額の再実行は no-op
        ledger.seed_mock_wallet_for_demo(
            test_db.conn,
            participant_id=REQUESTER_ID,
            asset="mock-USDC",
            balance_units=ROOT_BUDGET_UNITS,
        )
        wallet = test_db.conn.execute(
            "SELECT balance_units FROM mock_wallets WHERE participant_id = ?",
            (REQUESTER_ID,),
        ).fetchone()
        assert wallet["balance_units"] == ROOT_BUDGET_UNITS
        # 別額は拒否
        with pytest.raises(OjpError) as exc_info:
            ledger.seed_mock_wallet_for_demo(
                test_db.conn,
                participant_id=REQUESTER_ID,
                asset="mock-USDC",
                balance_units=ROOT_BUDGET_UNITS + 1,
            )
        assert exc_info.value.code == ErrorCode.INVALID_STATE.value
