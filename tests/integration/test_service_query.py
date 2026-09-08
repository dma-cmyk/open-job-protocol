"""Integration tests for service query APIs (list_jobs, get_job, get_ledger).

Contract verification according to PoC implementation plan:
1. list_jobs limit validation (default=20, 1..100 strict int, boundary & type checks)
2. list_jobs keyset pagination & deterministic ordering with duplicate created_at_us
3. list_jobs invalid cursor (INVALID_ARGUMENT), state filtering, parent_id filtering
4. get_job basic view keys, budget matching ledger view, regex on amounts and timestamps
5. get_job artifact permission matrix (system, worker, requester, root requester vs unrelated)
6. get_job parent-independent artifact permission (parent lease expired/abandoned, parent FAILED/EXPIRED)
7. get_ledger lifecycle integration, totals matching ledger view, conservation.ok is True, payees sum
8. get_ledger permissions (root requester / system allowed, others FORBIDDEN, child/nonexistent INVALID_TARGET)
9. Nonexistent actor_id rejection (FORBIDDEN across all 3 query functions)
10. Read-only preservation of DB state (operations, journal_entries, budget_accounts, jobs counts & row_version sum)
"""

from __future__ import annotations

import json
import re
import sqlite3

import pytest

from ojp import clock, db, domain, ledger, service, verification
from ojp.domain import ErrorCode, JobState, OjpError, ParticipantKind
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    CHILD_BUDGET_UNITS,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    TEST_T0_US,
    default_subcontract_policy,
    insert_demo_participants,
    insert_participant,
    load_poc_catalog,
)

UNRELATED_ID = "pt-unrelated"
AMOUNT_RE = re.compile(r"^[0-9]+\.[0-9]{6}$")
TIMESTAMP_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$"
)


def _setup_world(handle, *, seed_balance=ROOT_BUDGET_UNITS * 50):
    """固定 Actor 4 件 + 無関係 Actor 1 件 + Requester Wallet seed を投入する。"""
    with db.transaction(handle.conn, immediate=True):
        insert_demo_participants(handle.conn)
        insert_participant(handle.conn, UNRELATED_ID, ParticipantKind.AGENT)
        ledger.seed_mock_wallet_for_demo(
            handle.conn,
            participant_id=REQUESTER_ID,
            asset="mock-USDC",
            balance_units=seed_balance,
        )
        for worker_id in (AGENT_A_ID, AGENT_B_ID):
            ledger.seed_mock_wallet_for_demo(
                handle.conn,
                participant_id=worker_id,
                asset="mock-USDC",
                balance_units=0,
            )


def _create_open_root(conn, suffix, *, budget="100.000000", budget_units=ROOT_BUDGET_UNITS):
    root_def, task_catalog = load_poc_catalog()
    created = service.create_root(
        conn,
        actor_id=REQUESTER_ID,
        title=f"Root {suffix}",
        budget=budget,
        deadline_us=TEST_T0_US + 86_400_000_000,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=task_catalog,
        input_json=ledger.canonical_json_dumps(root_def.input_values),
        verifier_id=verification.VERIFIER_ID,
        verifier_hash=verification.verifier_hash(),
        conditions_json=ledger.canonical_json_dumps(root_def.expected),
        operation_id=f"create:root:{suffix}",
    )
    root_id = created.data["job_id"]
    version_id = created.data["version_id"]
    service.fund_root(
        conn,
        actor_id=REQUESTER_ID,
        root_id=root_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=budget_units,
        amount_units=budget_units,
        operation_id=f"fund:root:{suffix}",
    )
    return root_id, version_id


def _create_and_submit_child(conn, root_id, root_version_id, suffix):
    claimed_root = service.claim(
        conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        expected_version_id=root_version_id,
        operation_id=f"claim:root:{suffix}",
    )
    root_lease_id = claimed_root.data["lease_id"]

    created_child = service.create_child(
        conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease_id,
        task_key="part-1",
        budget_units=CHILD_BUDGET_UNITS,
        deadline_us=TEST_T0_US + 86_400_000_000,
        operation_id=f"create:child:{suffix}",
    )
    child_id = created_child.data["child_id"]
    child_v1 = created_child.data["version_id"]

    claimed_child = service.claim(
        conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=child_v1,
        operation_id=f"claim:child:{suffix}",
    )
    child_lease_id = claimed_child.data["lease_id"]

    artifact_json = json.dumps({"sum": 6})
    submitted = service.submit(
        conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease_id,
        version_id=child_v1,
        artifact_json=artifact_json,
        operation_id=f"submit:child:{suffix}",
    )
    sub_id = submitted.data["submission_id"]
    return {
        "root_id": root_id,
        "root_lease_id": root_lease_id,
        "child_id": child_id,
        "child_v1": child_v1,
        "child_lease_id": child_lease_id,
        "submission_id": sub_id,
    }


def _db_fingerprint(conn: sqlite3.Connection):
    return (
        conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM journal_entries").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM budget_accounts").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0],
        conn.execute("SELECT COALESCE(SUM(row_version), 0) FROM jobs").fetchone()[0],
    )


# ---------------------------------------------------------------------------
# Test 1: list_jobs limit validation
# ---------------------------------------------------------------------------


def test_list_jobs_limit_validation(test_db):
    """1. list_jobs: 既定 limit が 20 であること／limit=0・limit=101・limit=True・limit="5"

    が INVALID_ARGUMENT／limit=100 は通る
    """
    _setup_world(test_db)

    # 25 件作成
    for i in range(25):
        _create_open_root(test_db.conn, f"lim-{i:02d}")

    # 既定 limit は 20
    res_default = service.list_jobs(test_db.conn, actor_id=REQUESTER_ID)
    assert len(res_default["jobs"]) == 20
    assert res_default["next_cursor"] is not None

    # limit=0 は INVALID_ARGUMENT
    with pytest.raises(OjpError) as exc_info:
        service.list_jobs(test_db.conn, actor_id=REQUESTER_ID, limit=0)
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT

    # limit=101 は INVALID_ARGUMENT
    with pytest.raises(OjpError) as exc_info:
        service.list_jobs(test_db.conn, actor_id=REQUESTER_ID, limit=101)
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT

    # limit=True は INVALID_ARGUMENT (bool は int のサブクラスだが拒否)
    with pytest.raises(OjpError) as exc_info:
        service.list_jobs(test_db.conn, actor_id=REQUESTER_ID, limit=True)
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT

    # limit="5" は INVALID_ARGUMENT
    with pytest.raises(OjpError) as exc_info:
        service.list_jobs(test_db.conn, actor_id=REQUESTER_ID, limit="5")
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT

    # limit=100 は通る (25件すべて取得、next_cursor は None)
    res_100 = service.list_jobs(test_db.conn, actor_id=REQUESTER_ID, limit=100)
    assert len(res_100["jobs"]) == 25
    assert res_100["next_cursor"] is None


# ---------------------------------------------------------------------------
# Test 2: list_jobs keyset pagination & deterministic order
# ---------------------------------------------------------------------------


def test_list_jobs_keyset_pagination_and_deterministic_order(test_db):
    """2. list_jobs: 3件以上を作り limit=1 で next_cursor を辿ると重複も欠落もなく

    全件を1回ずつ取得でき、最終ページの next_cursor が None になること。
    created_at_us が同値の Job が2件以上ある状態を含めて順序が決定的であることを確認する。
    """
    _setup_world(test_db)

    # 固定時刻モードのまま 4 件作成 -> すべて同じ created_at_us (TEST_T0_US)
    created_ids = []
    for i in range(4):
        rid, _ = _create_open_root(test_db.conn, f"page-{i}")
        created_ids.append(rid)

    # DB 内の全件を確認（created_at_us が全て同一であることを検証）
    rows = test_db.conn.execute(
        "SELECT id, created_at_us FROM jobs ORDER BY created_at_us ASC, id ASC"
    ).fetchall()
    assert len(rows) == 4
    assert len({r["created_at_us"] for r in rows}) == 1  # 全て同一タイムスタンプ
    expected_order = [r["id"] for r in rows]

    # limit=1 で next_cursor を辿る
    cursor = None
    collected_ids = []
    for _ in range(10):  # 無限ループ防止ガード
        page = service.list_jobs(test_db.conn, actor_id=REQUESTER_ID, limit=1, cursor=cursor)
        assert len(page["jobs"]) == 1
        collected_ids.append(page["jobs"][0]["job_id"])
        cursor = page["next_cursor"]
        if cursor is None:
            break

    # 全件取得・重複なし・欠落なし・最終 next_cursor は None
    assert cursor is None
    assert len(collected_ids) == 4
    assert len(set(collected_ids)) == 4
    # 同一 created_at_us のもとで id 順の決定的な全順序になっていること
    assert collected_ids == expected_order


# ---------------------------------------------------------------------------
# Test 3: list_jobs filtering and invalid cursor
# ---------------------------------------------------------------------------


def test_list_jobs_filtering_and_invalid_cursor(test_db):
    """3. list_jobs: 不正 cursor（"abc"、"x:y"）が INVALID_ARGUMENT／

    state 絞り込みが効く／parent_id 絞り込みが Child だけを返す／
    存在しない parent_id が INVALID_TARGET
    """
    _setup_world(test_db)

    # 1件作成: DRAFT のまま
    root_def, task_catalog = load_poc_catalog()
    draft_created = service.create_root(
        test_db.conn,
        actor_id=REQUESTER_ID,
        title="Draft Root",
        budget="100.000000",
        deadline_us=TEST_T0_US + 86_400_000_000,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=task_catalog,
        operation_id="create:draft",
    )
    draft_id = draft_created.data["job_id"]

    # 1件作成: OPEN にして Child も作成
    root_id, root_v = _create_open_root(test_db.conn, "parent-test")
    child_info = _create_and_submit_child(test_db.conn, root_id, root_v, "c1")
    child_id = child_info["child_id"]

    # 不正 cursor: "abc" (コロンなし)
    with pytest.raises(OjpError) as exc_info:
        service.list_jobs(test_db.conn, actor_id=REQUESTER_ID, cursor="abc")
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT

    # 不正 cursor: "x:y" (timestamp が整数でない)
    with pytest.raises(OjpError) as exc_info:
        service.list_jobs(test_db.conn, actor_id=REQUESTER_ID, cursor="x:y")
    assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT

    # state 絞り込み: DRAFT
    draft_jobs = service.list_jobs(
        test_db.conn, actor_id=REQUESTER_ID, state=JobState.DRAFT.value
    )
    assert len(draft_jobs["jobs"]) == 1
    assert draft_jobs["jobs"][0]["job_id"] == draft_id
    assert draft_jobs["jobs"][0]["state"] == JobState.DRAFT.value

    # state 絞り込み: SUBMITTED (Child が SUBMITTED)
    submitted_jobs = service.list_jobs(
        test_db.conn, actor_id=REQUESTER_ID, state=JobState.SUBMITTED.value
    )
    assert len(submitted_jobs["jobs"]) == 1
    assert submitted_jobs["jobs"][0]["job_id"] == child_id

    # parent_id 絞り込み: root_id を指定して Child だけ取得
    child_jobs = service.list_jobs(test_db.conn, actor_id=REQUESTER_ID, parent_id=root_id)
    assert len(child_jobs["jobs"]) == 1
    assert child_jobs["jobs"][0]["job_id"] == child_id
    assert child_jobs["jobs"][0]["parent_id"] == root_id
    assert child_jobs["jobs"][0]["task_key"] == "part-1"

    # 存在しない parent_id は INVALID_TARGET
    with pytest.raises(OjpError) as exc_info:
        service.list_jobs(test_db.conn, actor_id=REQUESTER_ID, parent_id="nonexistent-parent")
    assert exc_info.value.code == ErrorCode.INVALID_TARGET


# ---------------------------------------------------------------------------
# Test 4: get_job basic view & formatting
# ---------------------------------------------------------------------------


def test_get_job_basic_view_and_formatting(test_db):
    r"""4. get_job: Root と Child それぞれで基本ビューの必須キーが揃い、

    budget の各値が ledger.get_root_ledger_view の同じ値の6桁小数表現と一致すること。
    金額キーがすべて ^[0-9]+\.[0-9]{6}$ に一致し、時刻キーがすべて Z 終端の ISO-8601 マイクロ秒であること
    """
    _setup_world(test_db)
    root_id, root_v = _create_open_root(test_db.conn, "view-test")
    child_info = _create_and_submit_child(test_db.conn, root_id, root_v, "c-view")
    child_id = child_info["child_id"]

    root_view = service.get_job(test_db.conn, actor_id=REQUESTER_ID, job_id=root_id)
    child_view = service.get_job(test_db.conn, actor_id=REQUESTER_ID, job_id=child_id)

    required_top_keys = {
        "job",
        "version",
        "lease",
        "budget",
        "children",
        "deadlines",
        "verdict",
        "payment",
        "submission",
        "now",
    }

    for view, is_root in [(root_view, True), (child_view, False)]:
        assert set(view.keys()) == required_top_keys
        # job keys
        assert {"job_id", "root_id", "parent_id", "requester_id", "state", "task_key", "created_at"}.issubset(
            view["job"].keys()
        )
        assert TIMESTAMP_RE.match(view["job"]["created_at"])
        assert TIMESTAMP_RE.match(view["now"])

        # version keys
        v = view["version"]
        assert v is not None
        assert AMOUNT_RE.match(v["budget"])
        assert TIMESTAMP_RE.match(v["deadline"])

        # budget keys matching ledger.get_root_ledger_view
        b = view["budget"]
        expected_ledger = ledger.get_root_ledger_view(test_db.conn, root_id)
        assert b["deposit"] == domain.format_amount_units(expected_ledger.deposit_units)
        assert b["escrow"] == domain.format_amount_units(expected_ledger.escrow_units)
        assert b["available"] == domain.format_amount_units(expected_ledger.available_units)
        assert b["locked"] == domain.format_amount_units(expected_ledger.locked_units)
        assert b["paid"] == domain.format_amount_units(expected_ledger.paid_units)
        assert b["refunded"] == domain.format_amount_units(expected_ledger.refunded_units)

        for amt_key in ["deposit", "escrow", "available", "locked", "paid", "refunded"]:
            assert AMOUNT_RE.match(b[amt_key])
        for b_name, b_amt in b["locked_breakdown"].items():
            assert AMOUNT_RE.match(b_amt)

        # deadlines
        for dl_key, dl_val in view["deadlines"].items():
            if dl_val is not None:
                assert TIMESTAMP_RE.match(dl_val)

        if is_root:
            assert view["job"]["parent_id"] is None
            assert len(view["children"]) == 1
            c = view["children"][0]
            assert c["child_id"] == child_id
            assert AMOUNT_RE.match(c["budget"])
            assert TIMESTAMP_RE.match(c["deadline"])
        else:
            assert view["job"]["parent_id"] == root_id
            assert view["children"] == []  # Root 以外は空 list


# ---------------------------------------------------------------------------
# Test 5: get_job artifact permission matrix
# ---------------------------------------------------------------------------


def test_get_job_artifact_permission_matrix(test_db):
    """5. get_job の成果物読取権マトリクス（Child に submit 済みの状態を作る）:

    system=可／提出した Worker B=可／Child の Requester A=可／Root Requester=可／
    無関係な Actor=不可（artifact_json と verification_evidence のキーが存在しない、
    かつ artifact_readable is False、かつ artifact_hash は返る）
    """
    _setup_world(test_db)
    root_id, root_v = _create_open_root(test_db.conn, "perm-test")
    child_info = _create_and_submit_child(test_db.conn, root_id, root_v, "c-perm")
    child_id = child_info["child_id"]

    # 1. system=可
    v_sys = service.get_job(test_db.conn, actor_id=SYSTEM_ID, job_id=child_id)
    sub_sys = v_sys["submission"]
    assert json.loads(sub_sys["artifact_json"]) == {"sum": 6}

    # 2. 提出した Worker B=可
    v_b = service.get_job(test_db.conn, actor_id=AGENT_B_ID, job_id=child_id)
    sub_b = v_b["submission"]
    assert sub_b["artifact_readable"] is True
    assert "artifact_json" in sub_b
    assert "verification_evidence" in sub_b

    # 3. Child の Requester A=可
    v_a = service.get_job(test_db.conn, actor_id=AGENT_A_ID, job_id=child_id)
    sub_a = v_a["submission"]
    assert sub_a["artifact_readable"] is True
    assert "artifact_json" in sub_a
    assert "verification_evidence" in sub_a

    # 4. Root Requester=可
    v_req = service.get_job(test_db.conn, actor_id=REQUESTER_ID, job_id=child_id)
    sub_req = v_req["submission"]
    assert sub_req["artifact_readable"] is True
    assert "artifact_json" in sub_req
    assert "verification_evidence" in sub_req

    # 5. 無関係な Actor=不可
    v_unrelated = service.get_job(test_db.conn, actor_id=UNRELATED_ID, job_id=child_id)
    sub_unrelated = v_unrelated["submission"]
    assert sub_unrelated["artifact_readable"] is False
    assert "artifact_json" not in sub_unrelated
    assert "verification_evidence" not in sub_unrelated
    assert "artifact_hash" in sub_unrelated
    assert sub_unrelated["verification_result"] == "PASS"


# ---------------------------------------------------------------------------
# Test 6: Parent-independent artifact readability
# ---------------------------------------------------------------------------


def test_get_job_parent_independent_artifact_permission(test_db):
    """6. Parent 非依存の読取権（最重要）: Child に有効 Submission がある状態で、

    (a) Parent の Lease を失効・放棄させた後、
    (b) さらに Parent Job を終端状態（FAILED または EXPIRED）にした後、
    いずれの時点でも Root Requester が Child の artifact_json を取得できること。
    同じ状態で無関係 Actor は取得できないこと
    """
    _setup_world(test_db)

    # --- ケース 1: Parent Lease 放棄 -> Parent FAILED ---
    root1_id, root1_v = _create_open_root(test_db.conn, "p-indep-1")
    c1_info = _create_and_submit_child(test_db.conn, root1_id, root1_v, "c-indep-1")
    c1_id = c1_info["child_id"]
    r1_lease = c1_info["root_lease_id"]

    # Parent Worker A が Root Lease を abandon -> Root Lease は closed_reason='abandoned', Root は FAILED
    service.abandon(
        test_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root1_id,
        lease_id=r1_lease,
        operation_id="abandon:root1",
    )
    # Root の状態が FAILED であることを確認
    parent_job_row = test_db.conn.execute("SELECT state FROM jobs WHERE id = ?", (root1_id,)).fetchone()
    assert parent_job_row["state"] == JobState.FAILED.value

    # この状態で Root Requester は Child の artifact_json を取得できる
    view_req1 = service.get_job(test_db.conn, actor_id=REQUESTER_ID, job_id=c1_id)
    assert view_req1["submission"]["artifact_readable"] is True
    assert "artifact_json" in view_req1["submission"]
    assert json.loads(view_req1["submission"]["artifact_json"]) == {"sum": 6}

    # 無関係 Actor は不可
    view_unrel1 = service.get_job(test_db.conn, actor_id=UNRELATED_ID, job_id=c1_id)
    assert view_unrel1["submission"]["artifact_readable"] is False
    assert "artifact_json" not in view_unrel1["submission"]

    # --- ケース 2: Parent Lease 失効 -> expire_due_leases で Parent EXPIRED ---
    root2_id, root2_v = _create_open_root(test_db.conn, "p-indep-2")
    c2_info = _create_and_submit_child(test_db.conn, root2_id, root2_v, "c-indep-2")
    c2_id = c2_info["child_id"]
    # 時刻を前進させて Parent Lease を期限切れ（+70秒）にする
    clock.set_test_now(test_db.conn, TEST_T0_US + 70_000_000)

    # expire_due_leases を実行して Parent Lease を閉じ、Parent Job を EXPIRED にする
    service.expire_due_leases(test_db.conn, actor_id=SYSTEM_ID)
    parent_row = test_db.conn.execute(
        "SELECT state FROM jobs WHERE id = ?", (root2_id,)
    ).fetchone()
    assert parent_row is not None
    assert parent_row["state"] == JobState.EXPIRED.value

    # Parent が EXPIRED になった後でも、Root Requester は Child の成果物を取得できる
    view_req2 = service.get_job(test_db.conn, actor_id=REQUESTER_ID, job_id=c2_id)
    assert view_req2["submission"]["artifact_readable"] is True
    assert "artifact_json" in view_req2["submission"]
    assert json.loads(view_req2["submission"]["artifact_json"]) == {"sum": 6}
    # 無関係 Actor は不可
    view_unrel2 = service.get_job(test_db.conn, actor_id=UNRELATED_ID, job_id=c2_id)
    assert view_unrel2["submission"]["artifact_readable"] is False
    assert "artifact_json" not in view_unrel2["submission"]


# ---------------------------------------------------------------------------
# Test 7: get_ledger full lifecycle & conservation
# ---------------------------------------------------------------------------


def test_get_ledger_full_lifecycle_and_conservation(test_db):
    """7. get_ledger: 入金→Child 作成→submit→approve→送金処理まで進めた世界で

    totals が ledger.get_root_ledger_view と一致し、conservation.ok が true、
    payees の合計が totals.paid + totals.refunded と一致すること
    """
    _setup_world(test_db)
    root_id, root_v = _create_open_root(test_db.conn, "ledger-flow")
    child_info = _create_and_submit_child(test_db.conn, root_id, root_v, "c-ledger")
    child_id = child_info["child_id"]
    sub_id = child_info["submission_id"]

    # Child を承認 (approve)
    service.approve(
        test_db.conn,
        actor_id=AGENT_A_ID,
        job_id=child_id,
        submission_id=sub_id,
        operation_id="approve:child",
    )

    # 送金処理 (process_payments)
    service.process_payments(test_db.conn)

    # get_ledger 呼び出し
    ledger_data = service.get_ledger(test_db.conn, actor_id=REQUESTER_ID, root_id=root_id)

    # totals が ledger.get_root_ledger_view と一致
    expected_view = ledger.get_root_ledger_view(test_db.conn, root_id)
    assert ledger_data["totals"]["deposit"] == domain.format_amount_units(expected_view.deposit_units)
    assert ledger_data["totals"]["escrow"] == domain.format_amount_units(expected_view.escrow_units)
    assert ledger_data["totals"]["available"] == domain.format_amount_units(expected_view.available_units)
    assert ledger_data["totals"]["locked"] == domain.format_amount_units(expected_view.locked_units)
    assert ledger_data["totals"]["paid"] == domain.format_amount_units(expected_view.paid_units)
    assert ledger_data["totals"]["refunded"] == domain.format_amount_units(expected_view.refunded_units)
    # conservation.ok is True
    assert ledger_data["conservation"]["ok"] is True
    assert ledger_data["conservation"]["accounts_non_negative"] == []
    assert ledger_data["conservation"]["journal_zero_sum"] == []

    # payees の合計が totals.paid + totals.refunded と一致
    payees_sum_units = sum(domain.parse_amount_units(p["amount"]) for p in ledger_data["payees"])
    expected_sum_units = domain.parse_amount_units(
        ledger_data["totals"]["paid"], allow_zero=True
    ) + domain.parse_amount_units(
        ledger_data["totals"]["refunded"], allow_zero=True
    )
    assert payees_sum_units == expected_sum_units
    assert payees_sum_units == CHILD_BUDGET_UNITS  # 10.000000 mock-USDC が Agent B へ送金済み

    # operations に payout が含まれる
    assert any(op["kind"] == "payout" and op["status"] == "SUCCEEDED" for op in ledger_data["operations"])


# ---------------------------------------------------------------------------
# Test 8: get_ledger permissions and target validation
# ---------------------------------------------------------------------------


def test_get_ledger_permissions_and_target_validation(test_db):
    """8. get_ledger: Root Requester=可／system=可／Agent B（無関係）=FORBIDDEN／

    Child の id を渡すと INVALID_TARGET／存在しない id で INVALID_TARGET
    """
    _setup_world(test_db)
    root_id, root_v = _create_open_root(test_db.conn, "ledger-perm")
    child_info = _create_and_submit_child(test_db.conn, root_id, root_v, "c-ledger-perm")
    child_id = child_info["child_id"]

    # Root Requester=可
    res_req = service.get_ledger(test_db.conn, actor_id=REQUESTER_ID, root_id=root_id)
    assert res_req["root_id"] == root_id

    # system=可
    res_sys = service.get_ledger(test_db.conn, actor_id=SYSTEM_ID, root_id=root_id)
    assert res_sys["root_id"] == root_id

    # Agent B（Worker / 会計ビュー無権限）=FORBIDDEN
    with pytest.raises(OjpError) as exc_info:
        service.get_ledger(test_db.conn, actor_id=AGENT_B_ID, root_id=root_id)
    assert exc_info.value.code == ErrorCode.FORBIDDEN

    # Child の id を渡すと INVALID_TARGET
    with pytest.raises(OjpError) as exc_info:
        service.get_ledger(test_db.conn, actor_id=REQUESTER_ID, root_id=child_id)
    assert exc_info.value.code == ErrorCode.INVALID_TARGET

    # 存在しない id は INVALID_TARGET
    with pytest.raises(OjpError) as exc_info:
        service.get_ledger(test_db.conn, actor_id=REQUESTER_ID, root_id="nonexistent-root")
    assert exc_info.value.code == ErrorCode.INVALID_TARGET


# ---------------------------------------------------------------------------
# Test 9: All 3 functions reject nonexistent actor_id
# ---------------------------------------------------------------------------


def test_all_queries_reject_nonexistent_actor(test_db):
    """9. 3関数すべてで存在しない actor_id が FORBIDDEN になること"""
    _setup_world(test_db)
    root_id, _ = _create_open_root(test_db.conn, "actor-check")

    ghost_actor = "pt-ghost-actor"

    # list_jobs
    with pytest.raises(OjpError) as exc_info:
        service.list_jobs(test_db.conn, actor_id=ghost_actor)
    assert exc_info.value.code == ErrorCode.FORBIDDEN

    # get_job
    with pytest.raises(OjpError) as exc_info:
        service.get_job(test_db.conn, actor_id=ghost_actor, job_id=root_id)
    assert exc_info.value.code == ErrorCode.FORBIDDEN

    # get_ledger
    with pytest.raises(OjpError) as exc_info:
        service.get_ledger(test_db.conn, actor_id=ghost_actor, root_id=root_id)
    assert exc_info.value.code == ErrorCode.FORBIDDEN


# ---------------------------------------------------------------------------
# Test 10: All 3 functions are read-only and do not mutate DB
# ---------------------------------------------------------------------------


def test_all_queries_are_read_only_and_preserve_db_state(test_db):
    """10. 3関数が呼び出し前後で DB を変更しないこと

    （operations / journal_entries / budget_accounts / jobs の件数と
    jobs.row_version の合計が不変であることを確認する）
    """
    _setup_world(test_db)
    root_id, root_v = _create_open_root(test_db.conn, "ro-check")
    child_info = _create_and_submit_child(test_db.conn, root_id, root_v, "c-ro")
    child_id = child_info["child_id"]

    before = _db_fingerprint(test_db.conn)

    # 3関数をそれぞれ複数回実行
    service.list_jobs(test_db.conn, actor_id=REQUESTER_ID)
    service.list_jobs(test_db.conn, actor_id=AGENT_A_ID, parent_id=root_id)
    service.get_job(test_db.conn, actor_id=REQUESTER_ID, job_id=root_id)
    service.get_job(test_db.conn, actor_id=AGENT_B_ID, job_id=child_id)
    service.get_ledger(test_db.conn, actor_id=REQUESTER_ID, root_id=root_id)
    service.get_ledger(test_db.conn, actor_id=SYSTEM_ID, root_id=root_id)

    after = _db_fingerprint(test_db.conn)
    assert before == after
