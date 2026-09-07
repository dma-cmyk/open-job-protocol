"""Phase 3 S2 の unit テスト: create_child（計画書 第8節・第10節・第11節）。

Done when の対応:
- 2（各上限の独立検証: MAX_AMOUNT / MAX_RATIO / MAX_CHILDREN / MAX_DEPTH、
  金額と比率を同時に超える場合は MAX_AMOUNT。X05 前半の件数、X05 後半の深さ）
- 3（task_key 規則 X13: TASK_CONFLICT / TASK_NOT_ALLOWED を POLICY_LIMIT に隠さない）
- 4（X02: 承認済み未送金の Child がある状態での上限判定。child_payout も U に含む）
- 5（X06 の Child 作成部分: input_json / expected_json / verifier_id /
  payee_id は引数として受け付けず、渡すと TypeError になる。Child JobVersion
  は Root 公開 Version のカタログから導出された値と hash を持つ）
- 6（Child 予算と deadline: TASK_BUDGET / ゼロ・負 / now >= deadline /
  Parent deadline 越えの許可）
- 7（Parent Lease の要求: OPEN / 終端 / Lease 失効中は作成不可）
- 8（Child 作成の各 commit 後に assert_ledger_invariants が成立）
- 2.1（作成後の Child は S1 の Claim / heartbeat / abandon / 失効が使える）
"""

from __future__ import annotations

import hashlib

import pytest

from ojp import clock, db, ledger, service
from ojp.domain import (
    ErrorCode,
    JobState,
    OjpError,
    PolicyLimitReason,
    SubcontractPolicy,
)
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    TEST_T0_US,
    insert_demo_participants,
    load_poc_catalog,
)

DEADLINE_US = TEST_T0_US + 86_400_000_000  # t0 + 1日
TEN = 10_000_000
TWENTY = 20_000_000


@pytest.fixture(autouse=True)
def _clear_failpoints():
    """failpoint seam はモジュール属性なので、テスト後は必ず元に戻す。"""
    yield
    ledger.failpoint_before_commit = None
    ledger.failpoint_after_commit = None
    ledger.failpoint_after_receipt = None


@pytest.fixture
def demo_db(test_db):
    """固定 Actor + Wallet seed 済みの test mode DB。

    B への送金（Mock.transfer）が受取人の Wallet を要求するため、Worker の
    Wallet も 0 で seed する（setup_ledger_demo_world(seed_workers=True) と
    同じ構成）。
    """
    with db.transaction(test_db.conn, immediate=True):
        insert_demo_participants(test_db.conn)
        for participant_id, balance in (
            (REQUESTER_ID, ROOT_BUDGET_UNITS),
            (AGENT_A_ID, 0),
            (AGENT_B_ID, 0),
        ):
            ledger.seed_mock_wallet_for_demo(
                test_db.conn,
                participant_id=participant_id,
                asset="mock-USDC",
                balance_units=balance,
            )
    return test_db


def _policy(**overrides) -> SubcontractPolicy:
    """既定（第10節の例に比率だけ 100% へ変えたもの）に差分を載せた policy。"""
    values = {
        "enabled": True,
        "max_amount_units": 30_000_000,
        "max_ratio_bps": 10_000,
        "max_children": 3,
        "max_depth": 1,
    }
    values.update(overrides)
    return SubcontractPolicy(**values)


def _generous_policy(**overrides) -> SubcontractPolicy:
    """予算・件数枠に余裕を持たせた policy（POLICY_LIMIT に隠さない。X13）。"""
    values = {
        "enabled": True,
        "max_amount_units": 100_000_000,
        "max_ratio_bps": 10_000,
        "max_children": 10,
        "max_depth": 1,
    }
    values.update(overrides)
    return SubcontractPolicy(**values)


def _leased_root(demo_db, *, policy=None, suffix="r1", **create_overrides):
    """create_root（第11節の固定カタログ）→ fund_root → A が Claim 済みの
    Root を返す（(root_id, root_version_id, parent_lease_id)）。"""
    _root_def, catalog = load_poc_catalog()
    kwargs = {
        "actor_id": REQUESTER_ID,
        "title": f"root {suffix}",
        "budget": "100.000000",
        "deadline_us": DEADLINE_US,
        "subcontract_policy": policy if policy is not None else _policy(),
        "task_catalog": catalog,
        "operation_id": f"create:{suffix}",
    }
    kwargs.update(create_overrides)
    created = service.create_root(demo_db.conn, **kwargs)
    root_id = created.data["job_id"]
    service.fund_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        root_id=root_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id=f"fund:{suffix}",
    )
    claimed = service.claim(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        expected_version_id=created.data["version_id"],
        operation_id=f"claim:{suffix}",
    )
    return root_id, created.data["version_id"], claimed.data["lease_id"]


def _create_child(
    demo_db,
    parent_job_id,
    lease_id,
    task_key,
    budget_units,
    op_id,
    *,
    actor_id=AGENT_A_ID,
    deadline_us=DEADLINE_US,
):
    """create_child の既定引数をまとめた helper。"""
    return service.create_child(
        demo_db.conn,
        actor_id=actor_id,
        parent_job_id=parent_job_id,
        lease_id=lease_id,
        task_key=task_key,
        budget_units=budget_units,
        deadline_us=deadline_us,
        operation_id=op_id,
    )


def _usage(demo_db, root_id):
    return ledger.get_subcontract_usage(demo_db.conn, root_id)


def _assert_balance(demo_db, root_id, *, u, available, count):
    """commit 済みの拘束・支払い・件数を読み直して照合する（第16節）。"""
    usage = _usage(demo_db, root_id)
    assert usage.in_use_units == u
    assert usage.available_units == available
    assert usage.child_count == count


def _claim_child(demo_db, child_id, version_id, op_id, *, actor_id=AGENT_B_ID):
    return service.claim(
        demo_db.conn,
        actor_id=actor_id,
        job_id=child_id,
        expected_version_id=version_id,
        operation_id=op_id,
    )


# ---------------------------------------------------------------------------
# 基本形: カタログからの導出・1 transaction 確定・台帳不変条件（Done when 8）
# ---------------------------------------------------------------------------


def test_create_child_derives_from_catalog_and_locks_budget(demo_db):
    """Child は作成と同時に OPEN。JobVersion は Root 公開版のカタログから
    導出され、available -X / child_work +X が同じ commit で確定する。"""
    root_id, _version_id, lease_id = _leased_root(demo_db, suffix="basic")
    result = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:basic-c1")
    assert result.replayed is False
    child_id = result.data["child_id"]
    assert result.data["state"] == JobState.OPEN.value
    assert result.data["parent_job_id"] == root_id
    assert result.data["root_id"] == root_id
    assert result.data["in_use_units"] == TEN
    assert result.data["available_units"] == ROOT_BUDGET_UNITS - TEN
    assert result.data["child_count"] == 1

    job = demo_db.conn.execute("SELECT * FROM jobs WHERE id = ?", (child_id,)).fetchone()
    assert job["state"] == JobState.OPEN.value
    assert job["parent_id"] == root_id
    assert job["root_id"] == root_id
    assert job["requester_id"] == AGENT_A_ID  # 作成時の A で固定（第8節）
    assert job["task_key"] == "part-1"
    assert job["creator_lease_id"] == lease_id  # 現在の Parent Lease で固定

    version = demo_db.conn.execute(
        "SELECT * FROM job_versions WHERE id = ?", (result.data["version_id"],)
    ).fetchone()
    assert version["budget_units"] == TEN
    assert version["asset"] == "mock-USDC"
    assert version["input_json"] == ledger.canonical_json_dumps([1, 2, 3])
    assert version["conditions_json"] == ledger.canonical_json_dumps({"sum": 6})
    assert version["deadline_us"] == DEADLINE_US
    # Child からの再委託は必ず拒否されるため、無効化した policy と空カタログ
    child_policy = SubcontractPolicy.model_validate_json(version["subcontract_policy"])
    assert child_policy == SubcontractPolicy(
        enabled=False, max_amount_units=0, max_ratio_bps=0, max_children=0, max_depth=0
    )
    assert version["task_catalog"] == "[]"

    view = ledger.get_root_ledger_view(demo_db.conn, root_id)
    assert view.available_units == ROOT_BUDGET_UNITS - TEN
    assert view.locked_breakdown_units["child_work"] == TEN
    _assert_balance(demo_db, root_id, u=TEN, available=ROOT_BUDGET_UNITS - TEN, count=1)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_create_child_replay_is_idempotent(demo_db):
    """同一 operation_id の再送は replay（Child も拘束も 1 回だけ）。"""
    root_id, _version_id, lease_id = _leased_root(demo_db, suffix="replay")
    first = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:replay-c1")
    replay = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:replay-c1")
    assert replay.replayed is True
    assert replay.data == first.data
    _assert_balance(demo_db, root_id, u=TEN, available=ROOT_BUDGET_UNITS - TEN, count=1)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_create_child_allocation_business_key_blocks_double_allocate(demo_db):
    """business_key allocate:{child_id} で、作成済み Child への直接の二重
    拘束（Phase 2 の allocate 経路）は拒否される（第16節の表）。"""
    root_id, _version_id, lease_id = _leased_root(demo_db, suffix="bk")
    created = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:bk-c1")
    with pytest.raises(OjpError) as exc_info:
        service.allocate_child_work(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            root_id=root_id,
            child_id=created.data["child_id"],
            amount_units=TEN,
            operation_id="allocate:bk-double",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    _assert_balance(demo_db, root_id, u=TEN, available=ROOT_BUDGET_UNITS - TEN, count=1)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 5: 条件のすり替え拒否（X06 の Child 作成部分）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "override_kwarg",
    [
        {"input_json": '[9,9,9]'},
        {"expected_json": '{"sum":999}'},
        {"verifier_id": "forged-verifier"},
        {"payee_id": AGENT_A_ID},
    ],
)
def test_create_child_rejects_condition_override_arguments(demo_db, override_kwarg):
    """成功条件・検証器・入力・受取人のすり替え引数は受け付けない（X06 の
    期待結果は「拒否」。「黙って無視」ではない）: create_child は
    input_json / expected_json / verifier_id / payee_id をシグネチャに
    持たず、渡すと TypeError になる。"""
    root_id, _version_id, lease_id = _leased_root(demo_db, suffix="x06-type")
    with pytest.raises(TypeError):
        service.create_child(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            parent_job_id=root_id,
            lease_id=lease_id,
            task_key="part-1",
            budget_units=TEN,
            deadline_us=DEADLINE_US,
            operation_id="create:x06-type-c1",
            **override_kwarg,
        )
    # すり替えの試行は Job も拘束も残さない
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=0)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_create_child_derives_conditions_from_catalog(demo_db):
    """Child JobVersion は Root 公開 Version のカタログから導出された値と
    hash を持つ（X06。verifier は Root 公開版から継承）。"""
    root_id, _version_id, lease_id = _leased_root(
        demo_db,
        suffix="x06",
        input_json=ledger.canonical_json_dumps([1, 2, 3, 4, 5]),
        verifier_id="sum-v1",
        verifier_hash="root-verifier-hash",
        conditions_json=ledger.canonical_json_dumps({"sum": 15}),
        conditions_hash="root-conditions-hash",
    )
    result = _create_child(
        demo_db,
        root_id,
        lease_id,
        "part-1",
        TEN,
        "create:x06-c1",
    )
    version = demo_db.conn.execute(
        "SELECT * FROM job_versions WHERE id = ?", (result.data["version_id"],)
    ).fetchone()
    # カタログ（part-1: 入力 [1,2,3] / 期待 {"sum":6}）から導出された値と hash
    expected_conditions = ledger.canonical_json_dumps({"sum": 6})
    assert version["input_json"] == ledger.canonical_json_dumps([1, 2, 3])
    assert version["conditions_json"] == expected_conditions
    assert version["conditions_hash"] == hashlib.sha256(
        expected_conditions.encode("utf-8")
    ).hexdigest()
    # verifier は Root 公開 Version から継承（A の指定は採用されない）
    assert version["verifier_id"] == "sum-v1"
    assert version["verifier_hash"] == "root-verifier-hash"
    _assert_balance(demo_db, root_id, u=TEN, available=ROOT_BUDGET_UNITS - TEN, count=1)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 3: task_key 規則（X13。POLICY_LIMIT に隠さない）
# ---------------------------------------------------------------------------


def test_create_child_rejects_unknown_task_key(demo_db):
    """未知の task_key（カタログ外）は TASK_NOT_ALLOWED（X13）。"""
    root_id, _version_id, lease_id = _leased_root(
        demo_db, policy=_generous_policy(), suffix="x13-unknown"
    )
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-unknown", TEN, "create:x13-u")
    assert exc_info.value.code == ErrorCode.TASK_NOT_ALLOWED.value
    assert exc_info.value.details is None  # POLICY_LIMIT ではない
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=0)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_create_child_rejects_live_task_key_conflict(demo_db):
    """生存中（OPEN / LEASED）の task_key の再作成は TASK_CONFLICT（X13）。"""
    root_id, _version_id, lease_id = _leased_root(
        demo_db, policy=_generous_policy(), suffix="x13-live"
    )
    created = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:x13-c1")
    # OPEN のままの再作成
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:x13-c1b")
    assert exc_info.value.code == ErrorCode.TASK_CONFLICT.value
    assert exc_info.value.details is None
    # B が Claim して LEASED でも再作成は拒否
    _claim_child(
        demo_db, created.data["child_id"], created.data["version_id"], "claim:x13-c1"
    )
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:x13-c1c")
    assert exc_info.value.code == ErrorCode.TASK_CONFLICT.value
    _assert_balance(demo_db, root_id, u=TEN, available=ROOT_BUDGET_UNITS - TEN, count=1)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_create_child_rejects_done_task_key_conflict(demo_db):
    """成功済み（DONE）の task_key の再作成は TASK_CONFLICT（X13）。

    ※ DONE は Phase 4 の承認経路（SUBMITTED → approve）が入るまで、jobs.state
    を直接更新する暫定的な組み立てで再現する（S1 の SUBMITTED abandon テスト
    と同じ方式）。
    """
    root_id, _version_id, lease_id = _leased_root(
        demo_db, policy=_generous_policy(), suffix="x13-done"
    )
    created = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:x13-done-c1")
    with db.transaction(demo_db.conn, immediate=True):
        demo_db.conn.execute(
            "UPDATE jobs SET state = ?, row_version = row_version + 1 WHERE id = ?",
            (JobState.DONE.value, created.data["child_id"]),
        )
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:x13-done-c2")
    assert exc_info.value.code == ErrorCode.TASK_CONFLICT.value
    _assert_balance(demo_db, root_id, u=TEN, available=ROOT_BUDGET_UNITS - TEN, count=1)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 2（件数）: X05 前半。失敗・失効したタスクの再発注は可能だが
# 累計 max_children を消費する（枠は戻らない）
# ---------------------------------------------------------------------------


def test_max_children_counts_cumulative_creations(demo_db):
    """X05 前半: max_children=3 で part-1 を 10 で作成 → 失敗を 3 回繰り返し、
    4 件目は POLICY_LIMIT/MAX_CHILDREN。2・3 件目は成功し、各失敗後
    U=0 / available=100（返却済みの失敗 Child 額は U から外れる）。"""
    root_id, _version_id, lease_id = _leased_root(
        demo_db,
        policy=_generous_policy(max_children=3),
        suffix="x05-count",
    )

    # 1 件目: abandon で失敗（FAILED）→ 再発注可能・枠は消費
    c1 = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:x05-c1")
    b1 = _claim_child(demo_db, c1.data["child_id"], c1.data["version_id"], "claim:x05-c1")
    abandoned = service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=c1.data["child_id"],
        lease_id=b1.data["lease_id"],
        operation_id="abandon:x05-c1",
    )
    assert abandoned.data["state"] == JobState.FAILED.value
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=1)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # 2 件目: 失効（EXPIRED）で失敗 → 再発注可能・枠は消費。
    # Parent の Lease だけ heartbeat で延ばし、Child 側の Lease だけを失効させる
    c2 = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:x05-c2")
    assert c2.replayed is False  # 2 件目の作成は成功
    _claim_child(demo_db, c2.data["child_id"], c2.data["version_id"], "claim:x05-c2")
    clock.set_test_now(demo_db.conn, TEST_T0_US + 50_000_000)
    service.heartbeat(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=lease_id,
        generation=1,
        operation_id="heartbeat:x05-parent",
    )  # Parent Lease は t0+110秒 まで有効
    clock.set_test_now(demo_db.conn, TEST_T0_US + 61_000_000)
    results = service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)
    assert [r.data["job_id"] for r in results] == [c2.data["child_id"]]
    expired_job = demo_db.conn.execute(
        "SELECT state FROM jobs WHERE id = ?", (c2.data["child_id"],)
    ).fetchone()
    assert expired_job["state"] == JobState.EXPIRED.value
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=2)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # 3 件目: abandon で失敗。累計 3 件を使い切る
    c3 = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:x05-c3")
    assert c3.replayed is False  # 3 件目の作成は成功
    b3 = _claim_child(demo_db, c3.data["child_id"], c3.data["version_id"], "claim:x05-c3")
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=c3.data["child_id"],
        lease_id=b3.data["lease_id"],
        operation_id="abandon:x05-c3",
    )
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=3)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # 4 件目: 累計作成件数の上限（U=0・金額に余裕があっても MAX_CHILDREN）
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:x05-c4")
    assert exc_info.value.code == ErrorCode.POLICY_LIMIT.value
    assert exc_info.value.details == {"reason": PolicyLimitReason.MAX_CHILDREN.value}
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=3)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 2（深さ）: X05 後半。Child からの発注は task_key 検査より先に
# MAX_DEPTH、資金移動なし
# ---------------------------------------------------------------------------


def test_create_child_from_child_is_max_depth_before_catalog(demo_db):
    """X05 後半: part-1 Child の有効 Worker B が、その Child を親として
    part-2 の孫発注を試行 → task_key 検査より先に POLICY_LIMIT/MAX_DEPTH、
    資金移動なし。未知の task_key でも MAX_DEPTH（カタログ参照より先）。"""
    root_id, _version_id, lease_id = _leased_root(demo_db, suffix="x05-depth")
    created = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:x05d-c1")
    child_id = created.data["child_id"]
    b_claim = _claim_child(demo_db, child_id, created.data["version_id"], "claim:x05d-c1")

    # B は Child の有効 Worker。Child を親にした発注は MAX_DEPTH
    with pytest.raises(OjpError) as exc_info:
        _create_child(
            demo_db,
            child_id,
            b_claim.data["lease_id"],
            "part-2",
            TEN,
            "create:x05d-g1",
            actor_id=AGENT_B_ID,
        )
    assert exc_info.value.code == ErrorCode.POLICY_LIMIT.value
    assert exc_info.value.details == {"reason": PolicyLimitReason.MAX_DEPTH.value}

    # 未知の task_key でも MAX_DEPTH（カタログ参照・重複・予算件数より先）
    with pytest.raises(OjpError) as exc_info:
        _create_child(
            demo_db,
            child_id,
            b_claim.data["lease_id"],
            "part-unknown",
            TEN,
            "create:x05d-g2",
            actor_id=AGENT_B_ID,
        )
    assert exc_info.value.code == ErrorCode.POLICY_LIMIT.value
    assert exc_info.value.details == {"reason": PolicyLimitReason.MAX_DEPTH.value}

    # 資金移動なし: U・available・件数は不変、拒否された試行は Job も
    # operation も残さない
    _assert_balance(demo_db, root_id, u=TEN, available=ROOT_BUDGET_UNITS - TEN, count=1)
    assert (
        demo_db.conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"] == 2
    )
    assert (
        demo_db.conn.execute(
            "SELECT COUNT(*) AS c FROM operations"
            " WHERE operation_id IN ('create:x05d-g1', 'create:x05d-g2')"
        ).fetchone()["c"]
        == 0
    )
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 2: 各上限の独立検証（金額・比率・件数・深さ・同時超過）
# ---------------------------------------------------------------------------


def test_max_amount_limit_independent(demo_db):
    """金額の独立検証: max_amount=30 / ratio=100%。U+20=40>30 は MAX_AMOUNT。"""
    root_id, _version_id, lease_id = _leased_root(demo_db, suffix="lim-amount")
    _create_child(demo_db, root_id, lease_id, "part-1", TWENTY, "create:la-c1")
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-2", TWENTY, "create:la-c2")
    assert exc_info.value.code == ErrorCode.POLICY_LIMIT.value
    assert exc_info.value.details == {"reason": PolicyLimitReason.MAX_AMOUNT.value}
    _assert_balance(demo_db, root_id, u=TWENTY, available=ROOT_BUDGET_UNITS - TWENTY, count=1)
    # 10 なら U=30 <= 30 で作成できる
    ok = _create_child(demo_db, root_id, lease_id, "part-2", TEN, "create:la-c2b")
    assert ok.data["in_use_units"] == 30_000_000
    _assert_balance(demo_db, root_id, u=30_000_000, available=70_000_000, count=2)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_max_ratio_limit_independent(demo_db):
    """比率の独立検証: max_amount=100 / ratio=30%（cap=30）。U+20=40>30 は MAX_RATIO。"""
    root_id, _version_id, lease_id = _leased_root(
        demo_db,
        policy=_policy(max_amount_units=100_000_000, max_ratio_bps=3000),
        suffix="lim-ratio",
    )
    _create_child(demo_db, root_id, lease_id, "part-1", TWENTY, "create:lr-c1")
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-2", TWENTY, "create:lr-c2")
    assert exc_info.value.code == ErrorCode.POLICY_LIMIT.value
    assert exc_info.value.details == {"reason": PolicyLimitReason.MAX_RATIO.value}
    _assert_balance(demo_db, root_id, u=TWENTY, available=ROOT_BUDGET_UNITS - TWENTY, count=1)
    ok = _create_child(demo_db, root_id, lease_id, "part-2", TEN, "create:lr-c2b")
    assert ok.data["in_use_units"] == 30_000_000
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_amount_and_ratio_both_exceeded_returns_max_amount(demo_db):
    """金額と比率を同時に超える場合は MAX_AMOUNT（第11節）。

    max_amount=15 / ratio=10%（cap=10）で U=10 のとき、20 は金額（30>15）
    比率（30>10）の両方を超える（カタログの 1 件上限 20 には収まる）。
    """
    root_id, _version_id, lease_id = _leased_root(
        demo_db,
        policy=_policy(max_amount_units=15_000_000, max_ratio_bps=1000),
        suffix="lim-both",
    )
    _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:lb-c1")
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-2", TWENTY, "create:lb-c2")
    assert exc_info.value.code == ErrorCode.POLICY_LIMIT.value
    assert exc_info.value.details == {"reason": PolicyLimitReason.MAX_AMOUNT.value}
    _assert_balance(demo_db, root_id, u=TEN, available=ROOT_BUDGET_UNITS - TEN, count=1)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


@pytest.mark.parametrize(
    ("policy_overrides", "expected_reason"),
    [
        ({"max_amount_units": 0}, PolicyLimitReason.MAX_AMOUNT.value),
        ({"max_ratio_bps": 0}, PolicyLimitReason.MAX_RATIO.value),
        ({"max_children": 0}, PolicyLimitReason.MAX_CHILDREN.value),
        ({"max_depth": 0}, PolicyLimitReason.MAX_DEPTH.value),
    ],
)
def test_zero_or_disabled_policy_forbids_creation(
    demo_db, policy_overrides, expected_reason
):
    """depth=0 / 上限 0 なら Child 作成不可（第10節）。reason で区別する。"""
    root_id, _version_id, lease_id = _leased_root(
        demo_db,
        policy=_generous_policy(**policy_overrides),
        suffix=f"lim-zero-{expected_reason.lower()}",
    )
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:lz-c1")
    assert exc_info.value.code == ErrorCode.POLICY_LIMIT.value
    assert exc_info.value.details == {"reason": expected_reason}
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=0)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_disabled_policy_forbids_creation_without_reason(demo_db):
    """enabled=false は POLICY_LIMIT で拒否するが、計画書（第10節・第11節）は
    この拒否に個別の details.reason を定めていないため、details は None の
    まま返す（MAX_DEPTH 等へ読み替えない）。"""
    root_id, _version_id, lease_id = _leased_root(
        demo_db,
        policy=_generous_policy(enabled=False),
        suffix="lim-disabled",
    )
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:ld-c1")
    assert exc_info.value.code == ErrorCode.POLICY_LIMIT.value
    assert exc_info.value.details is None
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=0)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 4: X02。承認済み未送金（child_payout）も U に含む
# ---------------------------------------------------------------------------


def test_x02_pending_child_payout_counts_toward_limit(demo_db):
    """X02: part-1 を 20 で作り承認して送金 commit 前で停止（Phase 2 の
    failpoint を使う）。part-2 を 20 で作成試行 → POLICY_LIMIT/MAX_AMOUNT
    （U=20 / available=80 維持）、別 operation_id で同じ part-2 を 10 → 成功
    （U=30 / available=70）。拒否された 20 の試行は Job も task_key も確保
    しない。child_payout も U に含まれることを確認する。

    ※ 承認は Phase 4 の approve が入るまで、その資金効果（child_work →
    child_payout と送金予約の確定）を Phase 2 の reserve_child_payout で
    再現する。
    """
    root_id, _version_id, lease_id = _leased_root(
        demo_db,
        policy=_policy(
            max_amount_units=30_000_000,
            max_ratio_bps=10_000,
            max_children=3,
            max_depth=1,
        ),
        suffix="x02",
    )
    c1 = _create_child(demo_db, root_id, lease_id, "part-1", TWENTY, "create:x02-c1")
    _claim_child(demo_db, c1.data["child_id"], c1.data["version_id"], "claim:x02-c1")
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # 承認（child_work 20 → child_payout 20）と送金予約の確定
    service.reserve_child_payout(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        root_id=root_id,
        child_id=c1.data["child_id"],
        amount_units=TWENTY,
        payee_id=AGENT_B_ID,
        operation_id="payout:x02-c1",
    )
    # 送金 commit 前で停止: failpoint を発火させて transaction ごと rollback
    def boom(name):
        raise RuntimeError(f"injected crash at {name}")

    ledger.failpoint_before_commit = boom
    with pytest.raises(RuntimeError):
        service.process_single_payment(
            demo_db.conn, operation_id="payout:x02-c1:payment"
        )
    payment = ledger.get_payment_operation(demo_db.conn, "payout:x02-c1:payment")
    assert payment is not None
    assert payment.status.value == "PENDING"  # 送金は未確定
    assert ledger.lookup_transfer_receipt(demo_db.conn, "payout:x02-c1:payment") is None

    # child_payout（送金待ち）が U に含まれる: paid=0・child_work=0 でも U=20
    view = ledger.get_root_ledger_view(demo_db.conn, root_id)
    assert view.paid_units == 0
    assert view.locked_breakdown_units["child_work"] == 0
    assert view.locked_breakdown_units["child_payout"] == TWENTY
    _assert_balance(demo_db, root_id, u=TWENTY, available=ROOT_BUDGET_UNITS - TWENTY, count=1)

    # part-2 を 20 で作成試行 → U=20 + 20 = 40 > 30 で MAX_AMOUNT
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-2", TWENTY, "create:x02-c2-20")
    assert exc_info.value.code == ErrorCode.POLICY_LIMIT.value
    assert exc_info.value.details == {"reason": PolicyLimitReason.MAX_AMOUNT.value}

    # 拒否された 20 の試行は Job も task_key も確保しない（U=20 / available=80 維持）
    _assert_balance(demo_db, root_id, u=TWENTY, available=ROOT_BUDGET_UNITS - TWENTY, count=1)
    assert (
        demo_db.conn.execute(
            "SELECT COUNT(*) AS c FROM jobs WHERE parent_id = ?", (root_id,)
        ).fetchone()["c"]
        == 1
    )
    assert (
        demo_db.conn.execute(
            "SELECT COUNT(*) AS c FROM operations WHERE operation_id = 'create:x02-c2-20'"
        ).fetchone()["c"]
        == 0
    )

    # 別 operation_id で同じ part-2 を 10 → 成功（拒否の残滓で重複扱いにならない）
    ok = _create_child(demo_db, root_id, lease_id, "part-2", TEN, "create:x02-c2-10")
    assert ok.data["task_key"] == "part-2"
    _assert_balance(demo_db, root_id, u=30_000_000, available=70_000_000, count=2)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_paid_child_counts_toward_u(demo_db):
    """確定済み Receipt の Child 支払い済み総額も U に含む（第10節）。

    max_amount=25。U=10（支払い済み）+ 20 = 30 > 25 は MAX_AMOUNT。
    U+budget == L の境界ちょうど（10+15=25）は許可される。
    """
    root_id, _version_id, lease_id = _leased_root(
        demo_db, policy=_policy(max_amount_units=25_000_000), suffix="paid-u"
    )
    c1 = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:paid-u-c1")
    _claim_child(demo_db, c1.data["child_id"], c1.data["version_id"], "claim:paid-u-c1")
    # 承認（Phase 4 までは資金効果を Phase 2 のサービスで再現）と送金の確定
    service.reserve_child_payout(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        root_id=root_id,
        child_id=c1.data["child_id"],
        amount_units=TEN,
        payee_id=AGENT_B_ID,
        operation_id="payout:paid-u-c1",
    )
    settled = service.process_single_payment(
        demo_db.conn, operation_id="payout:paid-u-c1:payment"
    )
    assert settled.data["payment_status"] == "SUCCEEDED"
    view = ledger.get_root_ledger_view(demo_db.conn, root_id)
    assert view.paid_units == TEN  # Receipt がある時だけ paid が増える
    _assert_balance(demo_db, root_id, u=TEN, available=ROOT_BUDGET_UNITS - TEN, count=1)

    # 支払い済みを U から外すと U+20=20 <= 25 で通ってしまう。U=10+20=30>25
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-2", TWENTY, "create:paid-u-c2")
    assert exc_info.value.code == ErrorCode.POLICY_LIMIT.value
    assert exc_info.value.details == {"reason": PolicyLimitReason.MAX_AMOUNT.value}
    # U+budget == L の境界ちょうどは許可（U=10+15=25 <= 25）
    ok = _create_child(demo_db, root_id, lease_id, "part-2", 15_000_000, "create:paid-u-c2b")
    assert ok.data["in_use_units"] == 25_000_000
    _assert_balance(demo_db, root_id, u=25_000_000, available=75_000_000, count=2)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 6: Child 予算と deadline
# ---------------------------------------------------------------------------


def test_create_child_budget_cap_and_boundary(demo_db):
    """カタログの 1 件上限（part-1 は 20）超過は POLICY_LIMIT/TASK_BUDGET。
    ちょうど上限は許可される。"""
    root_id, _version_id, lease_id = _leased_root(
        demo_db, policy=_generous_policy(), suffix="cap"
    )
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-1", 21_000_000, "create:cap-ng")
    assert exc_info.value.code == ErrorCode.POLICY_LIMIT.value
    assert exc_info.value.details == {"reason": PolicyLimitReason.TASK_BUDGET.value}
    ok = _create_child(demo_db, root_id, lease_id, "part-1", TWENTY, "create:cap-ok")
    assert ok.data["budget_units"] == TWENTY
    _assert_balance(demo_db, root_id, u=TWENTY, available=ROOT_BUDGET_UNITS - TWENTY, count=1)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


@pytest.mark.parametrize("budget_units", [0, -1])
def test_create_child_rejects_zero_or_negative_budget(demo_db, budget_units):
    """ゼロ・負の予算は拒否（INVALID_ARGUMENT。ゼロChild予算拒否と同じ段階）。"""
    root_id, _version_id, lease_id = _leased_root(
        demo_db, policy=_generous_policy(), suffix=f"zb{budget_units}"
    )
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-1", budget_units, "create:zb-c1")
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=0)


@pytest.mark.parametrize(
    "deadline_us",
    [TEST_T0_US, TEST_T0_US - 1],  # ちょうど / 過去
)
def test_create_child_rejects_deadline_not_in_future(demo_db, deadline_us):
    """now >= deadline の Child 作成は拒否（期限ちょうどは失効側。第7節・第8節）。"""
    root_id, _version_id, lease_id = _leased_root(
        demo_db, policy=_generous_policy(), suffix=f"dd{deadline_us}"
    )
    with pytest.raises(OjpError) as exc_info:
        _create_child(
            demo_db, root_id, lease_id, "part-1", TEN, "create:dd-c1",
            deadline_us=deadline_us,
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=0)


def test_create_child_deadline_may_exceed_parent_deadline(demo_db):
    """Parent の deadline を越える Child deadline は許可される（第8節）。"""
    parent_deadline = TEST_T0_US + 3_600_000_000  # t0 + 1時間
    root_id, _version_id, lease_id = _leased_root(
        demo_db, policy=_generous_policy(), suffix="ddl", deadline_us=parent_deadline
    )
    child_deadline = parent_deadline + 86_400_000_000  # Parent deadline + 1日
    ok = _create_child(
        demo_db, root_id, lease_id, "part-1", TEN, "create:ddl-c1",
        deadline_us=child_deadline,
    )
    assert ok.data["deadline_us"] == child_deadline
    _assert_balance(demo_db, root_id, u=TEN, available=ROOT_BUDGET_UNITS - TEN, count=1)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 7: Parent Lease の要求
# ---------------------------------------------------------------------------


def test_create_child_requires_existing_lease_on_open_parent(demo_db):
    """Parent が OPEN（未 Claim）は Child を作れない（有効な Lease が無い）。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        title="root open-parent",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=_generous_policy(),
        task_catalog=catalog,
        operation_id="create:open-parent",
    )
    root_id = created.data["job_id"]
    service.fund_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        root_id=root_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id="fund:open-parent",
    )
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, "lease:nonexistent", "part-1", TEN, "create:op-c1")
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=0)


def test_create_child_rejects_non_worker_actor(demo_db):
    """Parent Lease の Worker 本人でなければ FORBIDDEN。"""
    root_id, _version_id, lease_id = _leased_root(demo_db, suffix="nw")
    with pytest.raises(OjpError) as exc_info:
        _create_child(
            demo_db, root_id, lease_id, "part-1", TEN, "create:nw-c1",
            actor_id=AGENT_B_ID,
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=0)


def test_create_child_rejects_terminal_parent(demo_db):
    """Parent が終端（abandon で FAILED）は Child を作れない（閉じた Lease）。"""
    root_id, _version_id, lease_id = _leased_root(demo_db, suffix="term")
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=lease_id,
        operation_id="abandon:term",
    )
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:term-c1")
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value
    _assert_balance(demo_db, root_id, u=0, available=0, count=0)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


def test_create_child_rejects_expired_parent_lease_without_tick(demo_db):
    """Parent の Lease 失効中は Child を作れない（tick 未実行でも期限を検査。
    第7節）。"""
    root_id, _version_id, lease_id = _leased_root(demo_db, suffix="exp")
    clock.set_test_now(demo_db.conn, TEST_T0_US + 61_000_000)  # Lease 期限超過
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:exp-c1")
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=0)


def test_create_child_rejects_unknown_parent_job(demo_db):
    """存在しない親 Job は INVALID_TARGET。"""
    _leased_root(demo_db, suffix="np")
    with pytest.raises(OjpError) as exc_info:
        _create_child(demo_db, "job:nonexistent", "lease:x", "part-1", TEN, "create:np-c1")
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value


def test_create_child_rejects_unfunded_root(demo_db):
    """入金前の Root（DRAFT）からは Child を作れない（有効な Lease が無い）。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        title="root draft",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=_generous_policy(),
        task_catalog=catalog,
        operation_id="create:draft-root",
    )
    with pytest.raises(OjpError) as exc_info:
        _create_child(
            demo_db, created.data["job_id"], "lease:none", "part-1", TEN, "create:dr-c1"
        )
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value


# ---------------------------------------------------------------------------
# 2.1: 作成後の Child は S1 の Claim / heartbeat / abandon がそのまま使える
# ---------------------------------------------------------------------------


def test_created_child_supports_claim_heartbeat_and_abandon(demo_db):
    """作成後の Child は OPEN で公開済みのため、S1 の Claim / heartbeat /
    abandon がそのまま使える（Child の Requester A は Claim できない）。"""
    root_id, _version_id, lease_id = _leased_root(demo_db, suffix="s1flow")
    created = _create_child(demo_db, root_id, lease_id, "part-1", TEN, "create:s1-c1")
    child_id = created.data["child_id"]
    child_version_id = created.data["version_id"]

    # Child の Requester（A）は Claim できない（第7節: Requester本人でないWorker）
    with pytest.raises(OjpError) as exc_info:
        _claim_child(demo_db, child_id, child_version_id, "claim:s1-a", actor_id=AGENT_A_ID)
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value

    # B が Claim できる
    claimed = _claim_child(demo_db, child_id, child_version_id, "claim:s1-b")
    assert claimed.data["state"] == JobState.LEASED.value
    assert claimed.data["expires_at"] == TEST_T0_US + 60_000_000

    # heartbeat もそのまま使える
    clock.set_test_now(demo_db.conn, TEST_T0_US + 20_000_000)
    beat = service.heartbeat(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=claimed.data["lease_id"],
        generation=1,
        operation_id="heartbeat:s1-b",
    )
    assert beat.data["expires_at"] == TEST_T0_US + 20_000_000 + 60_000_000

    # abandon もそのまま使える（child_work → available へ戻る）
    abandoned = service.abandon(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=claimed.data["lease_id"],
        operation_id="abandon:s1-b",
    )
    assert abandoned.data["state"] == JobState.FAILED.value
    _assert_balance(demo_db, root_id, u=0, available=ROOT_BUDGET_UNITS, count=1)
    ledger.assert_ledger_invariants(demo_db.conn, root_id)
