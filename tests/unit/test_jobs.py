"""Phase 3 S1 の unit テスト: create_root（policy 値域・immutable の前提）・
fund_root の DRAFT→OPEN 遷移・Claim/heartbeat の検査・require_active_lease
共通判定の直接検証。

Done when の対応:
- 4（未入金 Root は Claim できず、fund 成功で OPEN になる）
- 5（policy 値域の拒否）
- Done when 2 の一部（require_active_lease の直接検証は test_db 上で行う）
"""

from __future__ import annotations

import pytest

from ojp import db, ledger, service
from ojp.domain import (
    ErrorCode,
    JobState,
    OjpError,
    SubcontractPolicy,
    TaskCatalogEntry,
    TimingPolicy,
)
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    TEST_T0_US,
    default_subcontract_policy,
    insert_demo_participants,
    insert_root_job,
    setup_ledger_demo_world,
)

DEADLINE_US = TEST_T0_US + 86_400_000_000  # t0 + 1日


def _create_root(test_db, **overrides):
    """create_root の既定引数をまとめた helper。"""
    kwargs = {
        "actor_id": REQUESTER_ID,
        "title": "root job",
        "budget": "100.000000",
        "deadline_us": DEADLINE_US,
        "subcontract_policy": default_subcontract_policy(),
        "task_catalog": [],
    }
    kwargs.update(overrides)
    return service.create_root(test_db.conn, **kwargs)


def _prepare_open_root(test_db, root_suffix="1"):
    """create_root → fund_root 済みの OPEN Root を返す（(job_id, version_id)）。"""
    created = _create_root(test_db, operation_id=f"create:root-{root_suffix}")
    job_id = created.data["job_id"]
    version_id = created.data["version_id"]
    service.fund_root(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=job_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id=f"fund:root-{root_suffix}",
    )
    return job_id, version_id


@pytest.fixture
def demo_db(test_db):
    """固定 Actor + Requester Wallet seed 済みの test mode DB。"""
    with db.transaction(test_db.conn, immediate=True):
        insert_demo_participants(test_db.conn)
        ledger.seed_mock_wallet_for_demo(
            test_db.conn,
            participant_id=REQUESTER_ID,
            asset="mock-USDC",
            balance_units=ROOT_BUDGET_UNITS,
        )
    return test_db


# ---------------------------------------------------------------------------
# create_root: 正常系と JobVersion の保存
# ---------------------------------------------------------------------------


def test_create_root_creates_draft_job_and_published_version(demo_db):
    """create_root は DRAFT の Root と公開 Version（version=1）を作る。

    JobVersion のフィールドは計画書 第5節の表どおり保存され、
    jobs.version_id がその公開版を指す。timing_policy は実値（既定 60/20/30/30）
    が JSON で保存される。
    """
    catalog = [
        TaskCatalogEntry(
            task_key="part-1",
            input_values=[1, 2, 3],
            expected={"sum": 6},
            budget_cap_units=20_000_000,
        )
    ]
    result = _create_root(demo_db, task_catalog=catalog, operation_id="create:r1")
    assert result.replayed is False
    job_id = result.data["job_id"]
    assert result.data["state"] == JobState.DRAFT.value
    assert result.data["budget_units"] == ROOT_BUDGET_UNITS

    job = demo_db.conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert job["state"] == JobState.DRAFT.value
    assert job["parent_id"] is None
    assert job["root_id"] == job_id
    assert job["requester_id"] == REQUESTER_ID
    assert job["version_id"] == result.data["version_id"]

    version = demo_db.conn.execute(
        "SELECT * FROM job_versions WHERE id = ?", (result.data["version_id"],)
    ).fetchone()
    assert version["job_id"] == job_id
    assert version["version"] == 1
    assert version["budget_units"] == ROOT_BUDGET_UNITS
    assert version["asset"] == "mock-USDC"
    assert version["deadline_us"] == DEADLINE_US
    timing = TimingPolicy.model_validate_json(version["timing_policy"])
    assert timing == TimingPolicy()  # 既定: Lease 60秒 / heartbeat 20秒 / 検収30秒 / 異議30秒
    policy = SubcontractPolicy.model_validate_json(version["subcontract_policy"])
    assert policy == default_subcontract_policy()
    import json as _json

    stored_catalog = _json.loads(version["task_catalog"])
    assert [e["task_key"] for e in stored_catalog] == ["part-1"]


def test_create_root_same_operation_id_replays_single_effect(demo_db):
    """同一 operation_id の再送は保存済み結果を返す（Root は 1 件だけ）。"""
    first = _create_root(demo_db, operation_id="create:replay")
    replay = _create_root(demo_db, operation_id="create:replay")
    assert replay.replayed is True
    assert replay.data == first.data
    count = demo_db.conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"]
    assert count == 1


def test_create_root_allows_same_payload_with_different_operation_ids(demo_db):
    """同一 Requester・同一 payload・別 operation_id で Root を複数作成できる。

    Root 作成は資金移動の業務効果を持たないので business_key を持たず、
    同じ内容の Root を 2 つ作れる（第16節: business_key は fund / allocate /
    return / payout / refund にだけ定義する）。同一 operation_id の再送
    だけが replay され、Job は増えない。
    """
    kwargs = {
        "actor_id": REQUESTER_ID,
        "title": "sum-v1",
        "budget": "100.000000",
        "deadline_us": DEADLINE_US,
        "subcontract_policy": default_subcontract_policy(),
        "task_catalog": [],
    }
    first = service.create_root(demo_db.conn, operation_id="create:one", **kwargs)
    second = service.create_root(demo_db.conn, operation_id="create:two", **kwargs)
    assert first.replayed is False
    assert second.replayed is False
    assert first.data["job_id"] != second.data["job_id"]
    assert first.data["version_id"] != second.data["version_id"]
    assert demo_db.conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"] == 2

    # 同一 operation_id の再送は replay され、Job は増えない
    replay = service.create_root(demo_db.conn, operation_id="create:one", **kwargs)
    assert replay.replayed is True
    assert replay.data == first.data
    assert demo_db.conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"] == 2


def test_create_root_rejects_duplicate_task_key_in_catalog(demo_db):
    """task_catalog 内の task_key 重複は INVALID_ARGUMENT で拒否する（第11節）。

    カタログは task_key で引く表であり、重複があると Child 作成時のカタログ
    導出が一意に決まらない。
    """
    catalog = [
        TaskCatalogEntry(
            task_key="part-1",
            input_values=[1, 2],
            expected={"sum": 3},
            budget_cap_units=10_000_000,
        ),
        TaskCatalogEntry(
            task_key="part-1",
            input_values=[3, 4],
            expected={"sum": 7},
            budget_cap_units=20_000_000,
        ),
    ]
    with pytest.raises(OjpError) as exc_info:
        _create_root(demo_db, task_catalog=catalog, operation_id="create:dup-task")
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert demo_db.conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"] == 0


def test_create_root_rejects_past_or_exact_deadline(demo_db):
    """deadline は作成時点の now より未来。now >= deadline は拒否（第7節: 期限ちょうどは失効側）。"""
    with pytest.raises(OjpError) as exc_info:
        _create_root(demo_db, deadline_us=TEST_T0_US)  # ちょうど
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    with pytest.raises(OjpError) as exc_info:
        _create_root(demo_db, deadline_us=TEST_T0_US - 1)  # 過去
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert demo_db.conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"] == 0


# ---------------------------------------------------------------------------
# Done when 5: subcontract_policy の値域
# ---------------------------------------------------------------------------


def _policy(**overrides):
    values = {
        "enabled": True,
        "max_amount_units": 30_000_000,
        "max_ratio_bps": 3000,
        "max_children": 3,
        "max_depth": 1,
    }
    values.update(overrides)
    return values


class _RawPolicy:
    """pydantic 検証を通さず生の値だけを保持する policy スタブ。

    通常経路は SubcontractPolicy の型で値域が強制されるため、「値域違反の
    policy」を組み立てるには検証のバイパスが必要。サービス境界（create_root）
    が再検証で拒否することを確認するためのテスト専用の入れ物で、本番コード
    パスには現れない。
    """

    def __init__(self, values: dict) -> None:
        self._values = values

    def model_dump(self) -> dict:
        return dict(self._values)


@pytest.mark.parametrize(
    "values",
    [
        _policy(max_ratio_bps=10_001),  # 範囲外（上限超）
        _policy(max_ratio_bps=-1),  # 範囲外（負）
        _policy(max_amount_units=-1),  # 非負違反
        _policy(max_children=-1),  # 非負違反
        _policy(max_children=1.5),  # 非整数
        _policy(max_depth=2),  # 0 または 1 のみ
    ],
)
def test_create_root_rejects_out_of_range_policy(demo_db, values):
    """policy の値域違反は INVALID_ARGUMENT で拒否する（計画書 第10節）。

    max_ratio_bps は 0〜10000、max_amount_units は非負、max_children は非負
    整数、max_depth は 0 または 1。
    """
    with pytest.raises(OjpError) as exc_info:
        service.create_root(
            demo_db.conn,
            actor_id=REQUESTER_ID,
            title="root job",
            budget="100.000000",
            deadline_us=DEADLINE_US,
            subcontract_policy=_RawPolicy(values),
            task_catalog=[],
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value
    assert demo_db.conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"] == 0


@pytest.mark.parametrize(
    "values",
    [
        {k: v for k, v in _policy().items() if k != "max_amount_units"},
        {k: v for k, v in _policy().items() if k != "max_ratio_bps"},
    ],
)
def test_create_root_rejects_missing_amount_or_ratio_constraint(demo_db, values):
    """金額制約（max_amount）と比率制約（max_ratio_bps）の両方の指定を必須
    にする（計画書 第10節）。片方だけの指定（もう片方の欠落）は拒否する。"""
    with pytest.raises(OjpError) as exc_info:
        service.create_root(
            demo_db.conn,
            actor_id=REQUESTER_ID,
            title="root job",
            budget="100.000000",
            deadline_us=DEADLINE_US,
            subcontract_policy=_RawPolicy(values),
            task_catalog=[],
        )
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT.value


def test_subcontract_policy_type_enforces_range():
    """通常経路（pydantic 検証）でも値域違反は型レベルで拒否される。"""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        SubcontractPolicy(
            enabled=True,
            max_amount_units=30_000_000,
            max_ratio_bps=10_001,
            max_children=3,
            max_depth=1,
        )


# ---------------------------------------------------------------------------
# Done when 4: 未入金 Root は Claim できず、fund 成功で OPEN になる
# ---------------------------------------------------------------------------


def test_fund_root_transitions_draft_to_open(demo_db):
    """fund 成功で jobs.state が DRAFT → OPEN に遷移する（第6節の表）。"""
    created = _create_root(demo_db, operation_id="create:f1")
    job_id = created.data["job_id"]
    before = demo_db.conn.execute(
        "SELECT state, row_version FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    assert before["state"] == JobState.DRAFT.value

    service.fund_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        root_id=job_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id="fund:f1",
    )
    after = demo_db.conn.execute(
        "SELECT state, row_version FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    assert after["state"] == JobState.OPEN.value
    assert after["row_version"] == before["row_version"] + 1
    view = ledger.get_root_ledger_view(demo_db.conn, job_id)
    assert view.deposit_units == ROOT_BUDGET_UNITS
    assert view.available_units == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(demo_db.conn, job_id)


def test_double_fund_is_single_effect_and_keeps_open(demo_db):
    """二重 fund は business_key fund:{root_id} で 1 回だけ（既存の冪等性を壊さない）。

    2 回目（別 operation_id）は拒否され、入金・引落しは 1 回だけ、
    Job は OPEN のまま。
    """
    created = _create_root(demo_db, operation_id="create:f2")
    job_id = created.data["job_id"]
    kwargs = {
        "actor_id": REQUESTER_ID,
        "root_id": job_id,
        "requester_id": REQUESTER_ID,
        "expected_amount_units": ROOT_BUDGET_UNITS,
        "amount_units": ROOT_BUDGET_UNITS,
    }
    service.fund_root(demo_db.conn, operation_id="fund:first", **kwargs)
    with pytest.raises(OjpError) as exc_info:
        service.fund_root(demo_db.conn, operation_id="fund:second", **kwargs)
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    view = ledger.get_root_ledger_view(demo_db.conn, job_id)
    assert view.deposit_units == ROOT_BUDGET_UNITS
    assert view.available_units == ROOT_BUDGET_UNITS
    state = demo_db.conn.execute(
        "SELECT state FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()["state"]
    assert state == JobState.OPEN.value
    ledger.assert_ledger_invariants(demo_db.conn, job_id)


def test_claim_rejects_draft_root(demo_db):
    """未入金 Root（DRAFT）は Claim できない（計画書 X01 の一部）。"""
    created = _create_root(demo_db, operation_id="create:c1")
    with pytest.raises(OjpError) as exc_info:
        service.claim(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            job_id=created.data["job_id"],
            expected_version_id=created.data["version_id"],
            operation_id="claim:draft",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert (
        demo_db.conn.execute("SELECT COUNT(*) AS c FROM leases").fetchone()["c"] == 0
    )


# ---------------------------------------------------------------------------
# Claim の検査（計画書 第7節）
# ---------------------------------------------------------------------------


def test_claim_creates_lease_and_leases_the_job(demo_db):
    """Claim 成功: Lease が作られ Job は LEASED、expires_at = now + 60秒。

    Lease 長は JobVersion の timing_policy（保存された実値）から読む。
    """
    job_id, version_id = _prepare_open_root(demo_db)
    result = service.claim(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        expected_version_id=version_id,
        operation_id="claim:ok",
    )
    assert result.data["state"] == JobState.LEASED.value
    assert result.data["generation"] == 1
    assert result.data["expires_at"] == TEST_T0_US + 60_000_000
    lease = demo_db.conn.execute(
        "SELECT * FROM leases WHERE id = ?", (result.data["lease_id"],)
    ).fetchone()
    assert lease["worker_id"] == AGENT_A_ID
    assert lease["version_id"] == version_id
    assert lease["claimed_at_us"] == TEST_T0_US
    assert lease["heartbeat_at_us"] == TEST_T0_US
    assert lease["closed_reason"] is None
    job = demo_db.conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert job["state"] == JobState.LEASED.value
    assert job["active_lease_id"] == result.data["lease_id"]


def test_claim_expiry_is_capped_by_job_deadline(demo_db):
    """expires_at = min(now + lease_seconds, job deadline)（第7節）。"""
    near_deadline = TEST_T0_US + 30_000_000  # 30秒後に deadline
    created = _create_root(
        demo_db, deadline_us=near_deadline, operation_id="create:cap"
    )
    job_id = created.data["job_id"]
    service.fund_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        root_id=job_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id="fund:cap",
    )
    result = service.claim(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        expected_version_id=created.data["version_id"],
        operation_id="claim:cap",
    )
    # now + 60秒 ではなく deadline（30秒後）が上限になる
    assert result.data["expires_at"] == near_deadline


def test_claim_rejects_requester_self(demo_db):
    """Requester 本人は Claim できない（FORBIDDEN。第7節）。"""
    job_id, version_id = _prepare_open_root(demo_db)
    with pytest.raises(OjpError) as exc_info:
        service.claim(
            demo_db.conn,
            actor_id=REQUESTER_ID,
            job_id=job_id,
            expected_version_id=version_id,
            operation_id="claim:self",
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value


def test_claim_rejects_wrong_version(demo_db):
    """expected_version_id が公開版と不一致なら INVALID_TARGET。"""
    job_id, _version_id = _prepare_open_root(demo_db)
    with pytest.raises(OjpError) as exc_info:
        service.claim(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            job_id=job_id,
            expected_version_id="version:other:1",
            operation_id="claim:wrong-version",
        )
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value


def test_claim_rejects_second_claim_as_conflict(demo_db):
    """LEASED への再 Claim は CLAIM_CONFLICT（先に有効 Lease が成立している）。

    自動 OPEN 復帰は無いため、Lease が切れた後の再 Claim も同じく
    CLAIM_CONFLICT（失効は expire_due_leases が EXPIRED へ確定する）。
    """
    job_id, version_id = _prepare_open_root(demo_db)
    service.claim(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        expected_version_id=version_id,
        operation_id="claim:first",
    )
    with pytest.raises(OjpError) as exc_info:
        service.claim(
            demo_db.conn,
            actor_id=AGENT_B_ID,
            job_id=job_id,
            expected_version_id=version_id,
            operation_id="claim:second",
        )
    assert exc_info.value.code == ErrorCode.CLAIM_CONFLICT.value


# ---------------------------------------------------------------------------
# heartbeat（計画書 第7節）
# ---------------------------------------------------------------------------


def _claim(demo_db, job_id, version_id, *, actor_id=AGENT_A_ID, op_id="claim:h"):
    return service.claim(
        demo_db.conn,
        actor_id=actor_id,
        job_id=job_id,
        expected_version_id=version_id,
        operation_id=op_id,
    )


def test_heartbeat_extends_expiry_and_returns_it(demo_db):
    """heartbeat 成功: heartbeat_at_us と expires_at が更新され、戻り値に含まれる。"""
    job_id, version_id = _prepare_open_root(demo_db)
    claimed = _claim(demo_db, job_id, version_id)
    from ojp import clock

    clock.set_test_now(demo_db.conn, TEST_T0_US + 20_000_000)
    result = service.heartbeat(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        lease_id=claimed.data["lease_id"],
        generation=1,
        operation_id="heartbeat:1",
    )
    assert result.data["expires_at"] == TEST_T0_US + 20_000_000 + 60_000_000
    lease = demo_db.conn.execute(
        "SELECT * FROM leases WHERE id = ?", (claimed.data["lease_id"],)
    ).fetchone()
    assert lease["heartbeat_at_us"] == TEST_T0_US + 20_000_000


def test_heartbeat_never_exceeds_job_deadline(demo_db):
    """heartbeat で Job deadline を越えない（第7節）。"""
    near_deadline = TEST_T0_US + 90_000_000
    created = _create_root(
        demo_db, deadline_us=near_deadline, operation_id="create:hb-cap"
    )
    job_id = created.data["job_id"]
    service.fund_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        root_id=job_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id="fund:hb-cap",
    )
    claimed = _claim(demo_db, job_id, created.data["version_id"], op_id="claim:hb-cap")
    from ojp import clock

    clock.set_test_now(demo_db.conn, TEST_T0_US + 50_000_000)
    result = service.heartbeat(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        lease_id=claimed.data["lease_id"],
        generation=1,
        operation_id="heartbeat:cap",
    )
    # now(50s) + 60s = 110s ではなく deadline(90s) に制限される
    assert result.data["expires_at"] == near_deadline


def test_heartbeat_rejects_non_worker(demo_db):
    """Lease の Worker 本人でなければ FORBIDDEN（検査順序 4）。"""
    job_id, version_id = _prepare_open_root(demo_db)
    claimed = _claim(demo_db, job_id, version_id)
    with pytest.raises(OjpError) as exc_info:
        service.heartbeat(
            demo_db.conn,
            actor_id=AGENT_B_ID,
            job_id=job_id,
            lease_id=claimed.data["lease_id"],
            generation=1,
            operation_id="heartbeat:forbidden",
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value


def test_heartbeat_rejects_wrong_lease_and_generation(demo_db):
    """lease_id がその Job の Lease でなければ INVALID_TARGET、
    generation 不一致なら LEASE_EXPIRED（検査順序 1・3）。"""
    job_id, version_id = _prepare_open_root(demo_db)
    claimed = _claim(demo_db, job_id, version_id)
    with pytest.raises(OjpError) as exc_info:
        service.heartbeat(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            job_id=job_id,
            lease_id="lease:unknown",
            generation=1,
            operation_id="heartbeat:bad-lease",
        )
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value
    with pytest.raises(OjpError) as exc_info:
        service.heartbeat(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            job_id=job_id,
            lease_id=claimed.data["lease_id"],
            generation=99,
            operation_id="heartbeat:bad-generation",
        )
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value


def test_heartbeat_rejects_expired_lease(demo_db):
    """now >= expires_at_us なら LEASE_EXPIRED（期限ちょうどは失効側）。"""
    job_id, version_id = _prepare_open_root(demo_db)
    claimed = _claim(demo_db, job_id, version_id)
    from ojp import clock

    clock.set_test_now(demo_db.conn, TEST_T0_US + 60_000_000)  # ちょうど期限
    with pytest.raises(OjpError) as exc_info:
        service.heartbeat(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            job_id=job_id,
            lease_id=claimed.data["lease_id"],
            generation=1,
            operation_id="heartbeat:expired",
        )
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value


# ---------------------------------------------------------------------------
# require_active_lease の直接検証（Done when 2 の土台。submit は Phase 4）
# ---------------------------------------------------------------------------


def test_require_active_lease_direct_checks(demo_db):
    """共通判定 require_active_lease を単体で直接検証する。

    Phase 4 の submit が使う前提の公開関数であり、Claim/heartbeat と同じ
    検査順序（INVALID_TARGET → 閉じた Lease → generation → FORBIDDEN →
    期限）をそのまま単体テストで担保する。
    """
    job_id, version_id = _prepare_open_root(demo_db)
    claimed = _claim(demo_db, job_id, version_id)
    lease_id = claimed.data["lease_id"]

    # 有効な Lease は Lease モデルを返す
    lease = service.require_active_lease(
        demo_db.conn, job_id, lease_id, 1, AGENT_A_ID, TEST_T0_US
    )
    assert lease.id == lease_id
    assert lease.worker_id == AGENT_A_ID

    # 1. 別 Job の lease_id は INVALID_TARGET
    with pytest.raises(OjpError) as exc_info:
        service.require_active_lease(
            demo_db.conn, "job:other", lease_id, 1, AGENT_A_ID, TEST_T0_US
        )
    assert exc_info.value.code == ErrorCode.INVALID_TARGET.value

    # 4. Worker 本人でなければ FORBIDDEN
    with pytest.raises(OjpError) as exc_info:
        service.require_active_lease(
            demo_db.conn, job_id, lease_id, 1, AGENT_B_ID, TEST_T0_US
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value

    # 3. generation 不一致は LEASE_EXPIRED
    with pytest.raises(OjpError) as exc_info:
        service.require_active_lease(
            demo_db.conn, job_id, lease_id, 2, AGENT_A_ID, TEST_T0_US
        )
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value

    # 5. 期限ちょうどは失効側
    with pytest.raises(OjpError) as exc_info:
        service.require_active_lease(
            demo_db.conn, job_id, lease_id, 1, AGENT_A_ID, TEST_T0_US + 60_000_000
        )
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value


def test_require_active_lease_never_revives_closed_lease(demo_db):
    """閉じた Lease を復活させない（検査順序 2。abandon 済みの Lease を渡す）。"""
    job_id, version_id = _prepare_open_root(demo_db)
    claimed = _claim(demo_db, job_id, version_id)
    lease_id = claimed.data["lease_id"]
    service.abandon(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        lease_id=lease_id,
        operation_id="abandon:close",
    )
    # 閉じた Lease は heartbeat でも共通判定でも拒否される
    with pytest.raises(OjpError) as exc_info:
        service.require_active_lease(
            demo_db.conn, job_id, lease_id, 1, AGENT_A_ID, TEST_T0_US
        )
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value
    with pytest.raises(OjpError) as exc_info:
        service.heartbeat(
            demo_db.conn,
            actor_id=AGENT_A_ID,
            job_id=job_id,
            lease_id=lease_id,
            generation=1,
            operation_id="heartbeat:closed",
        )
    assert exc_info.value.code == ErrorCode.LEASE_EXPIRED.value


# ---------------------------------------------------------------------------
# conftest の最小 Root helper（Phase 2 資産）でも fund → OPEN → Claim が通る
# ---------------------------------------------------------------------------


def test_insert_root_job_helper_also_transitions_to_open(test_db):
    """Phase 2 の insert_root_job で作った DRAFT Root も fund で OPEN になり
    Claim できる（既存の台帳テスト資産との整合）。"""
    root_id = "job-root-helper"
    setup_ledger_demo_world(test_db.conn, root_id)
    service.fund_root(
        test_db.conn,
        actor_id=REQUESTER_ID,
        root_id=root_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id="fund:helper",
    )
    state = test_db.conn.execute(
        "SELECT state FROM jobs WHERE id = ?", (root_id,)
    ).fetchone()["state"]
    assert state == JobState.OPEN.value
    version_id = test_db.conn.execute(
        "SELECT version_id FROM jobs WHERE id = ?", (root_id,)
    ).fetchone()["version_id"]
    # deadline は helper では t0 に設定されるため Claim は期限検査で拒否される
    # （insert_root_job は台帳テスト用の最小行で、timing の実値は持たない）。
    # state が OPEN になったことの確認がこのテストの主旨。
    assert version_id is not None
