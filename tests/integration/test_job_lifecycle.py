"""Phase 3 S1 の integration テスト: 同時 Claim（別プロセス・barrier）・
提出前失効・abandon・JobVersion immutable・失効/abandon 後の台帳不変条件。

Done when の対応:
- 1（同時 Claim: 独立プロセス・独立接続を barrier で同時開始）
- 2（敗者の後続操作: heartbeat 拒否 + require_active_lease の直接拒否）
- 3（提出前失効: EXPIRED・自動復帰なし・期限ちょうどは失効側）
- 6（JobVersion immutable: UPDATE / DELETE が DB レベルで拒否）
- 7（abandon: 第14節の 3 ケース + 成功時の FAILED・資金の後始末）
- 8（失効・abandon の各 commit 後に assert_ledger_invariants が成立）
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import textwrap

import pytest

from ojp import clock, db, ledger, service
from ojp.domain import ClockMode, ErrorCode, JobState, OjpError, SubcontractPolicy
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    TEST_T0_US,
    default_subcontract_policy,
    insert_demo_participants,
    insert_participant,
    setup_ledger_demo_world,
)

DEADLINE_US = TEST_T0_US + 86_400_000_000  # t0 + 1日


def _prepare_db(handle, *, seed_balance=ROOT_BUDGET_UNITS):
    """固定 Actor + Requester Wallet seed を test mode DB に投入する。"""
    with db.transaction(handle.conn, immediate=True):
        insert_demo_participants(handle.conn)
        ledger.seed_mock_wallet_for_demo(
            handle.conn,
            participant_id=REQUESTER_ID,
            asset="mock-USDC",
            balance_units=seed_balance,
        )


def _create_open_root(handle, suffix, *, budget="100.000000", budget_units=ROOT_BUDGET_UNITS, deadline_us=DEADLINE_US):
    """create_root → fund_root 済みの OPEN Root を返す（(job_id, version_id)）。"""
    created = service.create_root(
        handle.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}",
        budget=budget,
        deadline_us=deadline_us,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=[],
        operation_id=f"create:{suffix}",
    )
    job_id = created.data["job_id"]
    service.fund_root(
        handle.conn,
        actor_id=REQUESTER_ID,
        root_id=job_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=budget_units,
        amount_units=budget_units,
        operation_id=f"fund:{suffix}",
    )
    return job_id, created.data["version_id"]


def _insert_child_with_work(conn, root_id, child_id, *, budget_units, state=JobState.OPEN.value):
    """Child Job 行と child_work 拘束を作る（S2 の create_child が来るまでの土台）。

    child_work の拘束は ledger.allocate_child_work_in_tx（S2 の Child 作成が
    使うのと同じ台帳プリミティブ）で行い、台帳の保存則を実際に満たす形にする。
    """
    with db.transaction(conn, immediate=True):
        version_id = f"version:{child_id}:1"
        conn.execute(
            "INSERT INTO jobs (id, root_id, parent_id, requester_id, state,"
            " row_version, created_at_us, task_key)"
            " VALUES (?, ?, ?, ?, ?, 0, ?, 'part-1')",
            (child_id, root_id, root_id, AGENT_A_ID, state, TEST_T0_US),
        )
        conn.execute(
            "INSERT INTO job_versions (id, job_id, version, title, budget_units,"
            " asset, subcontract_policy, task_catalog, timing_policy, deadline_us)"
            " VALUES (?, ?, 1, ?, ?, 'mock-USDC', '{}', '[]', '{}', ?)",
            (version_id, child_id, f"child {child_id}", budget_units, DEADLINE_US),
        )
        conn.execute(
            "UPDATE jobs SET version_id = ? WHERE id = ?", (version_id, child_id)
        )
        ledger.allocate_child_work_in_tx(
            conn,
            root_id=root_id,
            child_id=child_id,
            amount_units=budget_units,
            operation_id=f"allocate:{child_id}",
            now_us=TEST_T0_US,
        )


def _view(conn, root_id):
    return ledger.get_root_ledger_view(conn, root_id)


# ---------------------------------------------------------------------------
# Done when 6: JobVersion immutable（migration 004 のトリガー）
# ---------------------------------------------------------------------------


def test_job_versions_update_is_rejected_by_trigger(test_db):
    """公開済み job_versions 行の UPDATE は DB レベルで拒否される（第5節 公開後 immutable）。"""
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "imm-u")
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        test_db.conn.execute(
            "UPDATE job_versions SET title = 'changed' WHERE id = ?", (version_id,)
        )
    # 拒否されたので内容は変わらない
    row = test_db.conn.execute(
        "SELECT title FROM job_versions WHERE id = ?", (version_id,)
    ).fetchone()
    assert row["title"] != "changed"


def test_job_versions_delete_is_rejected_by_trigger(test_db):
    """公開済み job_versions 行の DELETE は DB レベルで拒否される。"""
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "imm-d")
    with pytest.raises(sqlite3.IntegrityError, match="cannot be deleted"):
        test_db.conn.execute("DELETE FROM job_versions WHERE id = ?", (version_id,))
    count = test_db.conn.execute(
        "SELECT COUNT(*) AS c FROM job_versions WHERE id = ?", (version_id,)
    ).fetchone()["c"]
    assert count == 1


def test_existing_migrations_still_apply_after_004(tmp_path):
    """004 を含む全 migration が新規 DB へ順に適用できる（既存ファイルは不変）。"""
    path = tmp_path / "mig.sqlite3"
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=TEST_T0_US)
    names = [
        row["name"]
        for row in conn.execute("SELECT name FROM schema_migrations ORDER BY name")
    ]
    assert names == [
        "001_initial.sql",
        "002_ledger_uniqueness.sql",
        "003_receipt_source_account.sql",
        "004_job_versions_immutable.sql",
    ]


# ---------------------------------------------------------------------------
# Done when 1: 同時 Claim（独立プロセス・独立接続を barrier で同時開始）
# ---------------------------------------------------------------------------

_CLAIM_SCRIPT = textwrap.dedent(
    """
    import sys
    from ojp import clock, service
    from ojp.domain import ClockMode, OjpError

    path, job_id, version_id, actor_id, op_id, barrier_path, now_us = sys.argv[1:8]
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))
    # 共有ファイルへの追記で barrier: 両プロセスが書き込むまで待つ。
    # 共有メモリ Lock には頼らず、開始の同期だけをファイルで行う
    # （Claim 自体の直列化は DB の BEGIN IMMEDIATE と UNIQUE 制約が担う）。
    import os, time
    with open(barrier_path, "a") as f:
        f.write(actor_id + "\\n")
    deadline = time.monotonic() + 10
    while True:
        with open(barrier_path) as f:
            if len(f.read().split()) >= 2:
                break
        if time.monotonic() > deadline:
            print("barrier timeout", flush=True)
            sys.exit(9)
        time.sleep(0.005)
    try:
        result = service.claim(
            conn,
            actor_id=actor_id,
            job_id=job_id,
            expected_version_id=version_id,
            operation_id=op_id,
        )
    except OjpError as exc:
        print(f"error={exc.code}", flush=True)
        sys.exit(0)
    print(f"ok lease={result.data['lease_id']}", flush=True)
    conn.close()
    """
)


def test_concurrent_claim_only_one_lease_wins(test_db, tmp_path):
    """同時 Claim: 独立プロセス・独立接続を barrier で同時開始し、1 件だけ
    Lease が作られ、敗者は CLAIM_CONFLICT を受け取る（共有メモリ Lock に
    頼らない。計画書 第16節・N06）。"""
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "race")
    barrier = tmp_path / "claim-barrier.txt"
    barrier.write_text("")
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                _CLAIM_SCRIPT,
                str(test_db.path),
                job_id,
                version_id,
                actor,
                f"claim:{actor}",
                str(barrier),
                str(TEST_T0_US),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for actor in (AGENT_A_ID, AGENT_B_ID)
    ]
    outputs = []
    for proc in procs:
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, f"stdout: {out}\nstderr: {err}"
        outputs.append(out.strip())
    winners = [o for o in outputs if o.startswith("ok ")]
    losers = [o for o in outputs if "error=CLAIM_CONFLICT" in o]
    assert len(winners) == 1, f"outputs: {outputs}"
    assert len(losers) == 1, f"outputs: {outputs}"

    fresh = test_db.fresh_conn()
    try:
        leases = fresh.execute(
            "SELECT * FROM leases WHERE job_id = ?", (job_id,)
        ).fetchall()
        assert len(leases) == 1  # 1 件だけ Lease が作られる
        job = fresh.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        assert job["state"] == JobState.LEASED.value
        assert job["active_lease_id"] == leases[0]["id"]
    finally:
        fresh.close()


def test_concurrent_claim_loser_followup_is_rejected(test_db, tmp_path):
    """Done when 2: 同時 Claim の敗者の後続操作は拒否される。

    敗者の heartbeat が拒否され、require_active_lease（Phase 4 の submit が
    使う共通判定）が敗者の lease_id を拒否することを直接確認する。
    （submit 自体は Phase 4 の範囲なので実装しない。）
    """
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "race2")
    barrier = tmp_path / "claim-barrier2.txt"
    barrier.write_text("")
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                _CLAIM_SCRIPT,
                str(test_db.path),
                job_id,
                version_id,
                actor,
                f"claim:{actor}",
                str(barrier),
                str(TEST_T0_US),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for actor in (AGENT_A_ID, AGENT_B_ID)
    ]
    results = {}
    for proc, actor in zip(procs, (AGENT_A_ID, AGENT_B_ID)):
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, f"stdout: {out}\nstderr: {err}"
        results[actor] = out.strip()
    loser = AGENT_A_ID if "CLAIM_CONFLICT" in results[AGENT_A_ID] else AGENT_B_ID
    winner = AGENT_B_ID if loser == AGENT_A_ID else AGENT_A_ID
    winner_lease_id = results[winner].split("lease=", 1)[1]

    fresh = test_db.fresh_conn()
    try:
        # 敗者には Lease 行が一切無い（敗者の lease_id は存在しない）
        loser_leases = fresh.execute(
            "SELECT COUNT(*) AS c FROM leases WHERE job_id = ? AND worker_id = ?",
            (job_id, loser),
        ).fetchone()["c"]
        assert loser_leases == 0

        # 敗者が「自分が取れたつもりの lease_id」で heartbeat しても拒否される
        # （敗者の lease_id は DB に存在しない → INVALID_TARGET）
        loser_lease_id = f"lease:claim:{loser}"
        with pytest.raises(OjpError) as exc_info:
            service.heartbeat(
                fresh,
                actor_id=loser,
                job_id=job_id,
                lease_id=loser_lease_id,
                generation=1,
                operation_id="heartbeat:loser",
            )
        assert exc_info.value.code == ErrorCode.INVALID_TARGET.value

        # require_active_lease（共通判定）も敗者の lease_id を拒否する
        with pytest.raises(OjpError) as exc_info:
            service.require_active_lease(
                fresh, job_id, loser_lease_id, 1, loser, TEST_T0_US
            )
        assert exc_info.value.code == ErrorCode.INVALID_TARGET.value

        # 勝者の Lease を敗者が使おうとしても Worker 不一致で FORBIDDEN
        with pytest.raises(OjpError) as exc_info:
            service.require_active_lease(
                fresh, job_id, winner_lease_id, 1, loser, TEST_T0_US
            )
        assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    finally:
        fresh.close()


# ---------------------------------------------------------------------------
# Done when 3: 提出前失効（EXPIRED・自動復帰なし・期限ちょうどは失効側）
# ---------------------------------------------------------------------------


def test_lease_expiry_marks_job_expired_and_reserves_root_refund(test_db):
    """Lease 失効（提出前）で Job は EXPIRED、Root の未拘束額は返金予約へ。

    自動 OPEN 復帰・Worker 交代・新 Lease 作成は起きない。
    失効 commit 後に assert_ledger_invariants が成立する（Done when 8）。
    """
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "exp1")
    claimed = service.claim(
        test_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        expected_version_id=version_id,
        operation_id="claim:exp1",
    )
    clock.set_test_now(test_db.conn, TEST_T0_US + 61_000_000)  # Lease 期限超過
    results = service.expire_due_leases(test_db.conn, actor_id=SYSTEM_ID)
    assert len(results) == 1
    assert results[0].data["state"] == JobState.EXPIRED.value
    assert results[0].operation_id == f"expiry:{claimed.data['lease_id']}"

    fresh = test_db.fresh_conn()
    try:
        job = fresh.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        assert job["state"] == JobState.EXPIRED.value
        assert job["active_lease_id"] is None
        lease = fresh.execute(
            "SELECT * FROM leases WHERE id = ?", (claimed.data["lease_id"],)
        ).fetchone()
        assert lease["closed_reason"] == "expired"
        # Root の未拘束額（available 全額）が返金予約へ移る（locked には触らない）
        view = _view(fresh, job_id)
        assert view.available_units == 0
        assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS
        # 自動 OPEN 復帰なし・新 Lease なし・Worker 交代なし
        assert fresh.execute("SELECT COUNT(*) AS c FROM leases").fetchone()["c"] == 1
        ledger.assert_ledger_invariants(fresh, job_id)
    finally:
        fresh.close()

    # 失効後の再実行は冪等（同じ期限を二重処理しない）
    again = service.expire_due_leases(test_db.conn, actor_id=SYSTEM_ID)
    assert again == []
    ledger.assert_ledger_invariants(test_db.conn, job_id)


def test_expiry_at_exact_lease_expiry_is_expired_side(test_db):
    """期限ちょうど（now == expires_at）は失効側を優先する（第7節）。"""
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "exp-exact")
    claimed = service.claim(
        test_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        expected_version_id=version_id,
        operation_id="claim:exp-exact",
    )
    expires_at = claimed.data["expires_at"]
    clock.set_test_now(test_db.conn, expires_at)  # ちょうど期限
    results = service.expire_due_leases(test_db.conn, actor_id=SYSTEM_ID)
    assert len(results) == 1
    assert results[0].data["state"] == JobState.EXPIRED.value
    job = test_db.conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert job["state"] == JobState.EXPIRED.value
    ledger.assert_ledger_invariants(test_db.conn, job_id)


def test_open_root_deadline_arrival_expires_without_lease(test_db):
    """対象 2: OPEN のまま deadline 到来（Lease が無い）→ EXPIRED。

    Lease を伴わない Job 失効は expiry:job:{job_id} の派生 ID を使う。
    """
    _prepare_db(test_db)
    job_id, _version_id = _create_open_root(test_db, "exp-open")
    clock.set_test_now(test_db.conn, DEADLINE_US)  # deadline ちょうど（失効側）
    results = service.expire_due_leases(test_db.conn, actor_id=SYSTEM_ID)
    assert len(results) == 1
    assert results[0].operation_id == f"expiry:job:{job_id}"
    assert results[0].data["state"] == JobState.EXPIRED.value
    job = test_db.conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert job["state"] == JobState.EXPIRED.value
    view = _view(test_db.conn, job_id)
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, job_id)


def test_leased_root_job_deadline_arrival_closes_lease_and_expires(test_db):
    """対象 3: LEASED で job deadline 到来 → Lease を閉じ Job を EXPIRED。"""
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "exp-deadline")
    claimed = service.claim(
        test_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        expected_version_id=version_id,
        operation_id="claim:exp-deadline",
    )
    # Lease の expires_at は deadline に制限されているため、deadline ちょうどで
    # 「対象 1（Lease 期限）」としても検出される。どちらの経路でも Job は
    # EXPIRED、Lease は expired で閉じる。
    clock.set_test_now(test_db.conn, DEADLINE_US)
    results = service.expire_due_leases(test_db.conn, actor_id=SYSTEM_ID)
    assert len(results) == 1
    assert results[0].data["state"] == JobState.EXPIRED.value
    lease = test_db.conn.execute(
        "SELECT closed_reason FROM leases WHERE id = ?", (claimed.data["lease_id"],)
    ).fetchone()
    assert lease["closed_reason"] == "expired"
    job = test_db.conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert job["state"] == JobState.EXPIRED.value
    ledger.assert_ledger_invariants(test_db.conn, job_id)


def test_no_automatic_reopen_or_worker_switch_after_expiry(test_db):
    """失効後も OPEN 復帰・Worker 交代・新 Lease 作成は起きない。

    失効した Job を別 Worker が Claim しても終端状態なので拒否される。
    """
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "exp-noreopen")
    service.claim(
        test_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        expected_version_id=version_id,
        operation_id="claim:exp-noreopen",
    )
    clock.set_test_now(test_db.conn, TEST_T0_US + 61_000_000)
    service.expire_due_leases(test_db.conn, actor_id=SYSTEM_ID)
    with pytest.raises(OjpError) as exc_info:
        service.claim(
            test_db.conn,
            actor_id=AGENT_B_ID,
            job_id=job_id,
            expected_version_id=version_id,
            operation_id="claim:after-expiry",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    assert test_db.conn.execute("SELECT COUNT(*) AS c FROM leases").fetchone()["c"] == 1
    ledger.assert_ledger_invariants(test_db.conn, job_id)


def test_expiry_of_available_zero_root_creates_no_refund_reservation(test_db):
    """available が 0 の Root 失効は返金予約を作らない（正常な no-op。第16節）。"""
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "exp-zero")
    # available を 0 にする（Child 拘束の代わりに parent_payout 予約で全額 locked へ。
    # S1 の範囲では locked に触れないため、返金予約は作られないはず）。
    # ※ service.reserve_parent_payout は S3 で Parent DONE のときだけ許される
    #   判定（第8節の表）が入ったため、この fixture は Phase 2 の台帳
    #   プリミティブ ledger.parent_approval_in_tx を直接呼んで組み立てる。
    with db.transaction(test_db.conn, immediate=True):
        ledger.parent_approval_in_tx(
            test_db.conn,
            root_id=job_id,
            amount_units=ROOT_BUDGET_UNITS,
            operation_id="payout:reserve-all",
            now_us=TEST_T0_US,
        )
    assert _view(test_db.conn, job_id).available_units == 0
    clock.set_test_now(test_db.conn, DEADLINE_US)
    results = service.expire_due_leases(test_db.conn, actor_id=SYSTEM_ID)
    assert len(results) == 1
    assert results[0].data["state"] == JobState.EXPIRED.value
    assert results[0].data["funds"].get("no_op") is True
    view = _view(test_db.conn, job_id)
    assert view.locked_breakdown_units["refund"] == 0  # 返金予約は作らない
    assert view.locked_breakdown_units["parent_payout"] == ROOT_BUDGET_UNITS  # locked は触らない
    ledger.assert_ledger_invariants(test_db.conn, job_id)


def test_child_expiry_returns_child_work_to_available(test_db):
    """Child の失効は child_work → available へ戻す（return:{child_id} 相当）。

    S3 の保護規則全体ではなく、「未拘束額だけを移す」範囲の後始末。
    """
    _prepare_db(test_db)
    root_id, _root_version = _create_open_root(test_db, "exp-child-root")
    child_id = "job:child-exp"
    _insert_child_with_work(
        test_db.conn, root_id, child_id, budget_units=10_000_000
    )
    # Child を OPEN のまま deadline 到来させる
    clock.set_test_now(test_db.conn, DEADLINE_US)
    results = service.expire_due_leases(test_db.conn, actor_id=SYSTEM_ID)
    by_job = {r.data["job_id"]: r for r in results}
    assert by_job[child_id].data["state"] == JobState.EXPIRED.value
    assert by_job[child_id].data["funds"]["amount_units"] == 10_000_000
    view = _view(test_db.conn, root_id)
    # child_work は available へ戻り、Root の未拘束 available は返金予約へ
    assert view.locked_breakdown_units["child_work"] == 0
    assert view.available_units == 0
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 7: abandon（計画書 第14節の 3 ケース + 成功時の後始末）
# ---------------------------------------------------------------------------


def _claim_as(demo_conn, job_id, version_id, actor_id, op_id):
    return service.claim(
        demo_conn,
        actor_id=actor_id,
        job_id=job_id,
        expected_version_id=version_id,
        operation_id=op_id,
    )


def test_abandon_by_worker_fails_job_and_returns_funds(test_db):
    """abandon 成功: Lease は abandoned で閉じ、Job は FAILED、資金は後始末。

    abandon commit 後に assert_ledger_invariants が成立する（Done when 8）。
    """
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "ab-ok")
    claimed = _claim_as(test_db.conn, job_id, version_id, AGENT_A_ID, "claim:ab-ok")
    result = service.abandon(
        test_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        lease_id=claimed.data["lease_id"],
        operation_id="abandon:ab-ok",
    )
    assert result.data["state"] == JobState.FAILED.value
    job = test_db.conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert job["state"] == JobState.FAILED.value
    assert job["active_lease_id"] is None
    lease = test_db.conn.execute(
        "SELECT closed_reason FROM leases WHERE id = ?", (claimed.data["lease_id"],)
    ).fetchone()
    assert lease["closed_reason"] == "abandoned"
    view = _view(test_db.conn, job_id)
    assert view.available_units == 0
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS
    ledger.assert_ledger_invariants(test_db.conn, job_id)


def test_abandon_child_by_requester_a_is_forbidden(test_db):
    """第14節ケース 1: Child Requester A が Child を abandon → FORBIDDEN。

    A は Child の Requester（jobs.requester_id）だが Lease の Worker ではない。
    Lease に記録された Worker（B）との Actor 一致検査が先に来るため FORBIDDEN。
    """
    _prepare_db(test_db)
    root_id, _root_version = _create_open_root(test_db, "ab-child-root")
    child_id = "job:child-ab1"
    _insert_child_with_work(test_db.conn, root_id, child_id, budget_units=10_000_000)
    # B が Child を Claim する
    child_version_id = test_db.conn.execute(
        "SELECT version_id FROM jobs WHERE id = ?", (child_id,)
    ).fetchone()["version_id"]
    claimed = _claim_as(test_db.conn, child_id, child_version_id, AGENT_B_ID, "claim:b")
    with pytest.raises(OjpError) as exc_info:
        service.abandon(
            test_db.conn,
            actor_id=AGENT_A_ID,  # Child Requester A
            job_id=child_id,
            lease_id=claimed.data["lease_id"],
            operation_id="abandon:a-child",
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    # Child は失敗へ変更されない
    job = test_db.conn.execute("SELECT state FROM jobs WHERE id = ?", (child_id,)).fetchone()
    assert job["state"] == JobState.LEASED.value
    view = _view(test_db.conn, root_id)
    assert view.locked_breakdown_units["child_work"] == 10_000_000
    ledger.assert_ledger_invariants(test_db.conn, root_id)


def test_abandon_submitted_child_by_worker_b_is_invalid_state(test_db):
    """第14節ケース 2: 有効提出後（SUBMITTED）の Worker B の abandon → INVALID_STATE。

    ※「提出済み」状態は Phase 4 の submit が無いため、テスト内で submissions
    行と jobs.state='SUBMITTED' を直接用意する暫定的な組み立てである。
    Phase 4 で submit が入るまでの間、この fixture で検査順序（Job が
    LEASED であること）を担保する。
    """
    _prepare_db(test_db)
    root_id, _root_version = _create_open_root(test_db, "ab-child-root2")
    child_id = "job:child-ab2"
    _insert_child_with_work(test_db.conn, root_id, child_id, budget_units=10_000_000)
    child_version_id = test_db.conn.execute(
        "SELECT version_id FROM jobs WHERE id = ?", (child_id,)
    ).fetchone()["version_id"]
    claimed = _claim_as(test_db.conn, child_id, child_version_id, AGENT_B_ID, "claim:b2")
    lease_id = claimed.data["lease_id"]

    # --- Phase 4 の submit が入るまでの暫定的な組み立て（ここから） ---
    # submissions 行と jobs.state='SUBMITTED' を直接用意して「有効提出後」を再現する。
    # submit 実装後はこの直接 INSERT を submit 経路へ置き換える。
    with db.transaction(test_db.conn, immediate=True):
        test_db.conn.execute(
            "INSERT INTO submissions (id, job_id, lease_id, version_id, artifact_json,"
            " artifact_hash, verification_result, verification_evidence,"
            " submitted_at_us, valid_at_us, review_due_at_us)"
            " VALUES (?, ?, ?, ?, '{}', 'hash', 'PASS', '{}', ?, ?, ?)",
            (
                f"submission:{child_id}",
                child_id,
                lease_id,
                child_version_id,
                TEST_T0_US,
                TEST_T0_US,
                TEST_T0_US + 30_000_000,
            ),
        )
        test_db.conn.execute(
            "UPDATE leases SET closed_reason = 'submitted' WHERE id = ?", (lease_id,)
        )
        test_db.conn.execute(
            "UPDATE jobs SET state = 'SUBMITTED', active_lease_id = NULL,"
            " row_version = row_version + 1 WHERE id = ?",
            (child_id,),
        )
    # --- 暫定的な組み立て（ここまで） ---

    with pytest.raises(OjpError) as exc_info:
        service.abandon(
            test_db.conn,
            actor_id=AGENT_B_ID,  # 提出済み Child の Worker B
            job_id=child_id,
            lease_id=lease_id,
            operation_id="abandon:b-submitted",
        )
    assert exc_info.value.code == ErrorCode.INVALID_STATE.value
    # Child は失敗へ変更されない（提出後の保護）
    job = test_db.conn.execute("SELECT state FROM jobs WHERE id = ?", (child_id,)).fetchone()
    assert job["state"] == JobState.SUBMITTED.value
    ledger.assert_ledger_invariants(test_db.conn, root_id)


def test_abandon_by_requester_is_forbidden(test_db):
    """第14節ケース 3: Requester の一方的 fail には使えない（FORBIDDEN）。

    Requester は Lease の Worker ではないため、Worker 一致検査で FORBIDDEN。
    """
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "ab-req")
    claimed = _claim_as(test_db.conn, job_id, version_id, AGENT_A_ID, "claim:ab-req")
    with pytest.raises(OjpError) as exc_info:
        service.abandon(
            test_db.conn,
            actor_id=REQUESTER_ID,
            job_id=job_id,
            lease_id=claimed.data["lease_id"],
            operation_id="abandon:requester",
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN.value
    job = test_db.conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert job["state"] == JobState.LEASED.value
    ledger.assert_ledger_invariants(test_db.conn, job_id)


def test_abandon_child_returns_child_work_to_available(test_db):
    """Child の abandon 成功: child_work → available へ戻す（return:{child_id} 相当）。"""
    _prepare_db(test_db)
    root_id, _root_version = _create_open_root(test_db, "ab-child-root3")
    child_id = "job:child-ab3"
    _insert_child_with_work(test_db.conn, root_id, child_id, budget_units=10_000_000)
    child_version_id = test_db.conn.execute(
        "SELECT version_id FROM jobs WHERE id = ?", (child_id,)
    ).fetchone()["version_id"]
    claimed = _claim_as(test_db.conn, child_id, child_version_id, AGENT_B_ID, "claim:b3")
    result = service.abandon(
        test_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=claimed.data["lease_id"],
        operation_id="abandon:b3",
    )
    assert result.data["state"] == JobState.FAILED.value
    assert result.data["funds"]["amount_units"] == 10_000_000
    view = _view(test_db.conn, root_id)
    assert view.locked_breakdown_units["child_work"] == 0
    assert view.available_units == ROOT_BUDGET_UNITS  # Root はまだ OPEN のまま
    ledger.assert_ledger_invariants(test_db.conn, root_id)


# ---------------------------------------------------------------------------
# Done when 8 の補強: abandon / 失効の各 commit 後の台帳不変条件（再送冪等）
# ---------------------------------------------------------------------------


def test_abandon_replay_is_idempotent_and_invariants_hold(test_db):
    """同一 operation_id の abandon 再送は 1 回分の効果だけ（後始末も 1 回）。"""
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "ab-replay")
    claimed = _claim_as(test_db.conn, job_id, version_id, AGENT_A_ID, "claim:ab-replay")
    first = service.abandon(
        test_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        lease_id=claimed.data["lease_id"],
        operation_id="abandon:replay",
    )
    replay = service.abandon(
        test_db.conn,
        actor_id=AGENT_A_ID,
        job_id=job_id,
        lease_id=claimed.data["lease_id"],
        operation_id="abandon:replay",
    )
    assert replay.replayed is True
    assert replay.data == first.data
    view = _view(test_db.conn, job_id)
    assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS  # 二重予約なし
    ledger.assert_ledger_invariants(test_db.conn, job_id)
