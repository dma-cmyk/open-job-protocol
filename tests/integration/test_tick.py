"""Phase 4 S2 の integration テスト: scheduler tick（計画書 第12節・第14節・
第15節・N08・N09・N11・X10 相当）。

実ファイル SQLite（test mode の共有 Clock）で検証する:

- 期限後の自動承認 → 支払い処理までが 1 tick で進む
- watch(max_iterations=1) と tick_once が同じ効果（--once と --watch が
  同じ処理を呼ぶ）
- 独立プロセス（python -m ojp.scheduler --root <path> --once --json）から
  実行して同じ結果になり、stdout が単一 JSON
- 全プロセス停止後も期限は DB に残り、再起動時に過期限分を処理する（X10）
- N08: 有効提出後、lease_expires_at < now < review_due_at で tick を 1 回
  実行しても SUBMITTED 維持・Acceptance・PaymentOperation 0 件・残高不変。
  その後 review_due_at まで進めると自動承認（N11）
- N09: Child の Worker B が提出した後に Parent（Root）を失効させても、
  Child の期限承認で B への支払い予約と送金が成立する

create_child は timing を Root 公開版から継承するため、Root と Child は
同じ timing である前提でテストを組む（job_versions を直接 UPDATE しない）。
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from ojp import clock, db, ledger, scheduler, service, verification
from ojp.domain import (
    JobState,
    PaymentStatus,
    TimingPolicy,
)
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    TEST_T0_US,
    default_subcontract_policy,
    insert_demo_participants,
    load_poc_catalog,
)

DEADLINE_US = TEST_T0_US + 86_400_000_000  # t0 + 1日
TEN = 10_000_000
REVIEW_WINDOW_US = 30_000_000  # TimingPolicy 既定の検収待ち30秒

# Lease 30秒・検収 120秒: 有効提出（t0+1秒）で review_due_at = t0+121秒と
# なり、旧 Lease 期限（t0+30秒）だけを先に越えられる（N08）
N08_TIMING = TimingPolicy(
    lease_seconds=30,
    heartbeat_seconds=20,
    review_window_seconds=120,
    dispute_window_seconds=30,
)


@pytest.fixture
def demo_db(test_db):
    """固定 Actor + Wallet seed 済みの test mode DB（Worker も 0 で seed）。

    scheduler.main が data/ojp.sqlite3 を解決するため、DB パスは
    プロジェクトルート風の配置（tmp_path 配下）を前提にする（subprocess
    テストでは DB ファイルをコピーして使う）。
    Requester は 200（Root 2 件分）持たせておき、複数 Root を作る
    テスト（tick_once と watch の等価性比較）に対応する。
    """
    with db.transaction(test_db.conn, immediate=True):
        insert_demo_participants(test_db.conn)
        for participant_id, balance in (
            (REQUESTER_ID, ROOT_BUDGET_UNITS * 2),
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


def _leased_root(demo_db, *, suffix="r1", timing=None):
    """create_root → fund_root → A が Claim 済みの Root を返す
    （(root_id, version_id, lease_id)）。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        demo_db.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=catalog,
        timing_policy=timing,
        input_json=ledger.canonical_json_dumps(_root_def.input_values),
        verifier_id=verification.VERIFIER_ID,
        verifier_hash=verification.verifier_hash(),
        conditions_json=ledger.canonical_json_dumps(_root_def.expected),
        operation_id=f"create:{suffix}",
    )
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


def _submitted_child(demo_db, *, suffix="c1", timing=None):
    """create_root → fund → A Claim → create_child → B Claim → B submit
    （t0+1秒）済みの Child を返す
    （(root_id, child_id, submission_id)）。"""
    root_id, _root_v, root_lease = _leased_root(
        demo_db, suffix=f"{suffix}-r", timing=timing
    )
    created = service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease,
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id=f"create:{suffix}",
    )
    child_id = created.data["child_id"]
    claimed = service.claim(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=created.data["version_id"],
        operation_id=f"claim:{suffix}-b",
    )
    # 有効提出は t0+1秒で行う（review_due_at と旧 Lease 期限を分離するため）
    clock.set_test_now(demo_db.conn, TEST_T0_US + 1_000_000)
    submitted = service.submit(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=claimed.data["lease_id"],
        version_id=created.data["version_id"],
        artifact_json='{"sum": 6}',
        operation_id=f"submit:{suffix}",
    )
    return root_id, child_id, submitted.data["submission_id"]


def _job(conn, job_id):
    return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()


def _wallets(conn):
    return {
        row["participant_id"]: int(row["balance_units"])
        for row in conn.execute("SELECT * FROM mock_wallets")
    }


def _payment(conn, business_key):
    return conn.execute(
        "SELECT * FROM payment_operations WHERE business_key = ?",
        (business_key,),
    ).fetchone()


def _run_scheduler_once(root_dir: Path) -> subprocess.CompletedProcess[str]:
    """独立プロセスで python -m ojp.scheduler --root <path> --once --json。"""
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "ojp.scheduler",
            "--root",
            str(root_dir),
            "--once",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )


# ---------------------------------------------------------------------------
# 1 tick で自動承認 → 支払い処理まで進む
# ---------------------------------------------------------------------------


def test_tick_advances_approval_and_payment(demo_db):
    """期限後の自動承認 → 支払い処理までが 1 tick で進む。"""
    root_id, child_id, submission_id = _submitted_child(demo_db)
    review_due_at = demo_db.conn.execute(
        "SELECT review_due_at_us FROM submissions WHERE id = ?",
        (submission_id,),
    ).fetchone()["review_due_at_us"]
    clock.set_test_now(demo_db.conn, review_due_at)

    result = scheduler.tick_once(demo_db.conn, actor_id=SYSTEM_ID)
    assert result["counts"]["approved_submissions"] == 1
    assert result["counts"]["processed_payments"] == 1

    job = _job(demo_db.conn, child_id)
    assert job["state"] == JobState.DONE.value
    acceptance = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM acceptances WHERE job_id = ?", (child_id,)
    ).fetchone()["c"]
    assert acceptance == 1
    # 送金まで完了（B の Wallet に 10）
    assert _wallets(demo_db.conn)[AGENT_B_ID] == TEN
    payment = _payment(demo_db.conn, f"payout:{child_id}")
    assert payment["status"] == PaymentStatus.SUCCEEDED.value
    ledger.assert_ledger_invariants(demo_db.conn, root_id)


# ---------------------------------------------------------------------------
# watch(max_iterations=1) と tick_once の等価性
# ---------------------------------------------------------------------------


def test_watch_single_iteration_equals_tick_once(demo_db, tmp_path):
    """watch(max_iterations=1) と tick_once が同じ効果になる
    （--once と --watch が同じ処理を呼ぶ）。

    同じ状態の DB コピー 2 枚を作り、片方へ tick_once、もう片方へ
    watch(max_iterations=1) を実行して結果を比較する。
    """
    _root, child_id, submission_id = _submitted_child(demo_db, suffix="weq")
    review_due_at = demo_db.conn.execute(
        "SELECT review_due_at_us FROM submissions WHERE id = ?",
        (submission_id,),
    ).fetchone()["review_due_at_us"]

    # 時刻を進める前に DB を 2 枚コピーする（どちらも提出済み・期限前）。
    # close して WAL をチェックポイントさせてからコピーする
    demo_db.conn.close()
    copies = []
    for name in ("once", "watch"):
        root_dir = tmp_path / f"proj-{name}"
        (root_dir / "data").mkdir(parents=True)
        db_path = root_dir / "data" / "ojp.sqlite3"
        db_path.write_bytes(Path(demo_db.path).read_bytes())
        copies.append((root_dir, db_path))

    # コピー A: tick_once（--once と同じ処理）
    root_a, db_a = copies[0]
    conn_a = db.connect(db_a)
    try:
        clock.set_test_now(conn_a, review_due_at)
        result_once = scheduler.tick_once(conn_a, actor_id=SYSTEM_ID)
        state_once = {
            "job_state": _job(conn_a, child_id)["state"],
            "acceptances": conn_a.execute(
                "SELECT COUNT(*) AS c FROM acceptances WHERE job_id = ?",
                (child_id,),
            ).fetchone()["c"],
            "wallets": _wallets(conn_a),
        }
        assert result_once["counts"]["approved_submissions"] == 1
    finally:
        conn_a.close()

    # コピー B: watch(max_iterations=1)（--watch と同じ処理）
    root_b, db_b = copies[1]
    conn_b = db.connect(db_b)
    try:
        clock.set_test_now(conn_b, review_due_at)
        scheduler.watch(
            conn_b,
            actor_id=SYSTEM_ID,
            max_iterations=1,
            interval_seconds=0.01,
        )
        state_watch = {
            "job_state": _job(conn_b, child_id)["state"],
            "acceptances": conn_b.execute(
                "SELECT COUNT(*) AS c FROM acceptances WHERE job_id = ?",
                (child_id,),
            ).fetchone()["c"],
            "wallets": _wallets(conn_b),
        }
    finally:
        conn_b.close()
    del root_a, root_b
    assert state_watch == state_once


def test_watch_stops_with_max_iterations_and_stop(demo_db):
    """watch は max_iterations と stop で有限回に止まる。"""
    _root, _child, _s = _submitted_child(demo_db, suffix="finite")
    # max_iterations=2 で 2 回止まる（sleep しないよう interval=0）
    calls = []
    original = scheduler.tick_once

    def _counting(conn, **kwargs):
        calls.append(1)
        return original(conn, **kwargs)

    scheduler.tick_once = _counting
    try:
        scheduler.watch(
            demo_db.conn,
            actor_id=SYSTEM_ID,
            max_iterations=2,
            interval_seconds=0,
        )
    finally:
        scheduler.tick_once = original
    assert len(calls) == 2

    calls.clear()
    scheduler.tick_once = _counting
    try:
        scheduler.watch(
            demo_db.conn,
            actor_id=SYSTEM_ID,
            interval_seconds=0,
            max_iterations=5,
            stop=lambda: len(calls) >= 3,
        )
    finally:
        scheduler.tick_once = original
    assert len(calls) == 3


# ---------------------------------------------------------------------------
# 独立プロセス（--once --json）
# ---------------------------------------------------------------------------


def test_subprocess_tick_once_json(demo_db, tmp_path):
    """独立プロセスから python -m ojp.scheduler --root <path> --once --json を
    実行して自動承認が進み、stdout が単一 JSON・終了コード 0。"""
    _root, _child, submission_id = _submitted_child(demo_db, suffix="sp")
    # DB をプロジェクトルート風の配置へ複製する
    root_dir = tmp_path / "proj"
    (root_dir / "data").mkdir(parents=True)
    db_path = root_dir / "data" / "ojp.sqlite3"
    demo_db.conn.close()
    db_path.write_bytes(Path(demo_db.path).read_bytes())

    conn = db.connect(db_path)
    try:
        submission_id = conn.execute("SELECT id FROM submissions").fetchone()["id"]
        review_due_at = conn.execute(
            "SELECT review_due_at_us FROM submissions WHERE id = ?",
            (submission_id,),
        ).fetchone()["review_due_at_us"]
        # Clock を期限後へ進める（時刻更新 transaction を Job 処理と混在させない）
        clock.set_test_now(conn, review_due_at)
    finally:
        conn.close()

    proc = _run_scheduler_once(root_dir)
    assert proc.returncode == 0, proc.stderr
    # stdout は単一 JSON（余計なログが混ざらない）
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    result = json.loads(lines[0])
    assert result["counts"]["approved_submissions"] == 1
    assert result["counts"]["processed_payments"] == 1
    assert result["error"] is None

    conn = db.connect(db_path)
    try:
        child_id = conn.execute(
            "SELECT job_id FROM submissions WHERE id = ?", (submission_id,)
        ).fetchone()["job_id"]
        job = _job(conn, child_id)
        assert job["state"] == JobState.DONE.value
        assert _wallets(conn)[AGENT_B_ID] == TEN
        root_id = job["root_id"]
        ledger.assert_ledger_invariants(conn, root_id)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 全プロセス停止・再起動後の過期限処理（X10）
# ---------------------------------------------------------------------------


def test_overdue_processed_after_restart(demo_db, tmp_path):
    """全プロセス停止後も期限は DB に残り、再起動時に過期限分を処理する
    （接続を閉じ、新しい接続で Clock を期限後へ進め、独立プロセスの tick が
    処理する）。"""
    _root, _child, _sub = _submitted_child(demo_db, suffix="restart")
    root_dir = tmp_path / "proj"
    (root_dir / "data").mkdir(parents=True)
    db_path = root_dir / "data" / "ojp.sqlite3"
    demo_db.conn.close()
    db_path.write_bytes(Path(demo_db.path).read_bytes())

    # 接続を閉じた状態で（全プロセス停止を模倣）期限だけ過ぎている
    conn = db.connect(db_path)
    try:
        submission_id = conn.execute("SELECT id FROM submissions").fetchone()["id"]
        review_due_at = conn.execute(
            "SELECT review_due_at_us FROM submissions WHERE id = ?",
            (submission_id,),
        ).fetchone()["review_due_at_us"]
        # 新しい接続で Clock を期限後へ進める
        clock.set_test_now(conn, review_due_at + 5_000_000)
    finally:
        conn.close()

    # 独立プロセスの tick が過期限分を処理する
    proc = _run_scheduler_once(root_dir)
    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.strip())
    assert result["counts"]["approved_submissions"] == 1
    assert result["counts"]["processed_payments"] == 1

    conn = db.connect(db_path)
    try:
        child_id = conn.execute(
            "SELECT job_id FROM submissions WHERE id = ?", (submission_id,)
        ).fetchone()["job_id"]
        assert _job(conn, child_id)["state"] == JobState.DONE.value
        assert _wallets(conn)[AGENT_B_ID] == TEN
        root_id = _job(conn, child_id)["root_id"]
        ledger.assert_ledger_invariants(conn, root_id)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# N08: 有効提出後・検収期限前の旧 Lease 期限通過
# ---------------------------------------------------------------------------


def test_tick_keeps_submitted_between_lease_expiry_and_review_due(demo_db):
    """N08: lease_expires_at < now < review_due_at で tick を 1 回実行 →
    Child は SUBMITTED 維持・Acceptance・PaymentOperation（Child 分）0 件・
    全口座残高・Wallet・PaymentOperation・Acceptance が提出直後の
    スナップショットから 1 行単位で不変。窓の中では資金を動かす操作を
    一切行わない。

    注意: create_child は timing を Root 公開版から継承するため、Child だけ
    別の Lease 長 / review_window を持つ構成は作れない（計画書のN08専用
    fixture では Root Lease 300秒・Child Lease 60秒・review 120秒）。
    そのためこのテストでは Root も同じ timing（Lease 30秒・review 120秒）
    を持つ前提で組み、窓の中で Root が失効して返金送金が動くと残高不変の
    検証にならないため、窓に入る前に A が Root の heartbeat を 1 回だけ
    行って Root Lease を延ばす（heartbeat は資金を動かさない操作）。
    この Root/Child の timing 分離ができない点は計画書との差分として
    報告する。
    """
    root_id, child_id, submission_id = _submitted_child(
        demo_db, suffix="n08", timing=N08_TIMING
    )
    review_due_at = demo_db.conn.execute(
        "SELECT review_due_at_us FROM submissions WHERE id = ?",
        (submission_id,),
    ).fetchone()["review_due_at_us"]
    # t0+1秒提出・Lease 30秒・review 120秒 → review_due_at = t0+121秒
    assert review_due_at == TEST_T0_US + 1_000_000 + 120_000_000
    lease = demo_db.conn.execute(
        "SELECT * FROM leases WHERE job_id = ?", (child_id,)
    ).fetchone()
    assert lease["expires_at_us"] == TEST_T0_US + 30_000_000

    # 旧 Lease 期限（t0+30秒）より後・review_due_at より前へ進める。
    # 窓の中で Root が失効して返金送金が動くと残高不変の検証にならない
    # ため、窓に入る前に A が Root の heartbeat を 1 回だけ行って Root
    # Lease を延ばす（heartbeat は資金を動かさない操作。送金待ちを残さ
    # ない構成）。
    wallets_after_submit = _wallets(demo_db.conn)
    buckets_after_submit = {
        (r["owner_job_id"], r["bucket"], r["source_key"]): int(r["amount_units"])
        for r in demo_db.conn.execute(
            "SELECT owner_job_id, bucket, source_key, amount_units"
            " FROM budget_accounts WHERE root_id = ?",
            (root_id,),
        )
    }
    payments_after_submit = demo_db.conn.execute(
        "SELECT operation_id, business_key, amount_units, payee_id, status"
        " FROM payment_operations WHERE root_id = ? ORDER BY operation_id",
        (root_id,),
    ).fetchall()
    assert payments_after_submit == []  # 送金待ちを残さない
    acceptances_after_submit = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM acceptances WHERE job_id = ?",
        (child_id,),
    ).fetchone()["c"]
    events_after_submit = demo_db.conn.execute(
        "SELECT COUNT(*) AS c FROM events WHERE job_id = ?", (child_id,)
    ).fetchone()["c"]

    # Root Lease を延ばす（t0+29秒。heartbeat は資金を動かさない）。
    # これで窓の中の tick が Root を失効させず、返金予約・返金送金も
    # 起こらない
    clock.set_test_now(demo_db.conn, TEST_T0_US + 29_000_000)
    root_lease = demo_db.conn.execute(
        "SELECT * FROM leases WHERE job_id = ? AND closed_reason IS NULL",
        (root_id,),
    ).fetchone()
    assert root_lease is not None
    service.heartbeat(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=root_lease["id"],
        generation=int(root_lease["generation"]),
        operation_id="heartbeat:n08-root",
    )

    # 窓（t0+45秒）: lease_expires_at < now < review_due_at
    now = TEST_T0_US + 45_000_000
    assert lease["expires_at_us"] < now < review_due_at
    clock.set_test_now(demo_db.conn, now)

    result = scheduler.tick_once(demo_db.conn, actor_id=SYSTEM_ID)
    assert result["counts"]["approved_submissions"] == 0

    conn = demo_db.fresh_conn()
    try:
        # Child は SUBMITTED を維持（OPEN / EXPIRED へ戻らない）
        job = _job(conn, child_id)
        assert job["state"] == JobState.SUBMITTED.value
        # Child の Acceptance・支払い予約は 0 件
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM acceptances WHERE job_id = ?",
                (child_id,),
            ).fetchone()["c"]
            == 0
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM payment_operations WHERE job_id = ?",
                (child_id,),
            ).fetchone()["c"]
            == 0
        )
        # Lease は監査値のまま・'submitted' で閉じたまま
        fresh_lease = conn.execute(
            "SELECT * FROM leases WHERE job_id = ?", (child_id,)
        ).fetchone()
        assert fresh_lease["expires_at_us"] == TEST_T0_US + 30_000_000
        assert fresh_lease["closed_reason"] == "submitted"
        # Child への新しい Lease や expiry Event は作られない
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM leases WHERE job_id = ?", (child_id,)
            ).fetchone()["c"]
            == 1
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM events WHERE job_id = ?",
                (child_id,),
            ).fetchone()["c"]
            == events_after_submit
        )
        # 全口座残高（budget_accounts）は提出直後から 1 行単位で不変
        buckets_after_tick = {
            (r["owner_job_id"], r["bucket"], r["source_key"]): int(r["amount_units"])
            for r in conn.execute(
                "SELECT owner_job_id, bucket, source_key, amount_units"
                " FROM budget_accounts WHERE root_id = ?",
                (root_id,),
            )
        }
        assert buckets_after_tick == buckets_after_submit
        # Wallet は全口座不変（返金送金も起こっていない）
        fresh_wallets = _wallets(conn)
        assert fresh_wallets == wallets_after_submit
        # PaymentOperation も 1 行単位で不変
        payments_after_tick = conn.execute(
            "SELECT operation_id, business_key, amount_units, payee_id, status"
            " FROM payment_operations WHERE root_id = ? ORDER BY operation_id",
            (root_id,),
        ).fetchall()
        assert [tuple(r) for r in payments_after_tick] == [
            tuple(r) for r in payments_after_submit
        ]
        # Acceptance の行数も不変
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM acceptances WHERE job_id = ?",
                (child_id,),
            ).fetchone()["c"]
            == acceptances_after_submit
        )
        ledger.assert_ledger_invariants(conn, root_id)
    finally:
        conn.close()

    # N11: review_due_at まで進めると通常の自動承認が機能する
    clock.set_test_now(demo_db.conn, review_due_at)
    result = scheduler.tick_once(demo_db.conn, actor_id=SYSTEM_ID)
    assert result["counts"]["approved_submissions"] == 1
    assert _job(demo_db.conn, child_id)["state"] == JobState.DONE.value
    assert _wallets(demo_db.conn)[AGENT_B_ID] == TEN
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

# ---------------------------------------------------------------------------
# N09: Parent 失効後も Child の期限処理が独立に継続
# ---------------------------------------------------------------------------


def test_child_approval_survives_parent_expiry(demo_db):
    """N09: A が Root を Claim して Child を作成した後、**Parent（Root）を
    失効させる**。その後に B が Child を Claim して提出しても、固定検証と
    期限承認・送金が成立する（Parent 終端を Child へ伝播しない。完了条件4
    「Parent 失効後も Child の Lease・提出・期限処理が独立に継続する」）。

    順序（A プロセス停止の再現）: Root Claim → Child 作成 → Parent 失効
    （t0+61秒で Lease 期限切れ）→ B が Child Claim（t0+62秒）→ B submit
    （t0+63秒）→ review_due_at（t0+63+120秒）まで進めて tick → B へ支払い。
    Root Requester が Child の成果物を（保存済み artifact として）取得
    できることも確認する。
    """
    root_id, child_version_id, root_lease = _leased_root(
        demo_db, suffix="n09-r", timing=N08_TIMING
    )
    created_child = service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease,
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id="create:n09",
    )
    child_id = created_child.data["child_id"]

    # Parent（Root）を失効させる（Child はまだ OPEN なので対象外。
    # Parent 終端を Child へ伝播しない）
    clock.set_test_now(demo_db.conn, TEST_T0_US + 61_000_000)
    service.expire_due_leases(demo_db.conn, actor_id=SYSTEM_ID)
    assert _job(demo_db.conn, root_id)["state"] == JobState.EXPIRED.value
    assert _job(demo_db.conn, child_id)["state"] == JobState.OPEN.value

    # Parent 失効後に B が Child を Claim して提出できる（t0+62〜63秒）
    clock.set_test_now(demo_db.conn, TEST_T0_US + 62_000_000)
    claimed_child = service.claim(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=created_child.data["version_id"],
        operation_id="claim:n09-b",
    )
    clock.set_test_now(demo_db.conn, TEST_T0_US + 63_000_000)
    submitted = service.submit(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=claimed_child.data["lease_id"],
        version_id=created_child.data["version_id"],
        artifact_json='{"sum": 6}',
        operation_id="submit:n09",
    )
    submission_id = submitted.data["submission_id"]
    review_due_at = demo_db.conn.execute(
        "SELECT review_due_at_us FROM submissions WHERE id = ?",
        (submission_id,),
    ).fetchone()["review_due_at_us"]
    assert review_due_at == TEST_T0_US + 63_000_000 + 120_000_000
    assert _job(demo_db.conn, child_id)["state"] == JobState.SUBMITTED.value
    del child_version_id

    # Child の review_due_at 到達で tick: 自動承認 → 送金まで成立する
    clock.set_test_now(demo_db.conn, review_due_at)
    result = scheduler.tick_once(demo_db.conn, actor_id=SYSTEM_ID)
    assert result["counts"]["approved_submissions"] == 1
    assert result["counts"]["processed_payments"] >= 1
    assert _job(demo_db.conn, child_id)["state"] == JobState.DONE.value
    assert _wallets(demo_db.conn)[AGENT_B_ID] == TEN
    payment = _payment(demo_db.conn, f"payout:{child_id}")
    assert payment["status"] == PaymentStatus.SUCCEEDED.value
    # Parent 終端後は Root の未拘束 available も返金予約へ
    refund = _payment(demo_db.conn, f"refund:{root_id}:terminal")
    assert refund is not None
    assert refund["status"] == PaymentStatus.SUCCEEDED.value
    ledger.assert_ledger_invariants(demo_db.conn, root_id)

    # Root Requester は Child の保存成果物を取得できる（Phase 5 の取得
    # service は未実装のため、保存済み artifact と hash の照合で確認）
    row = demo_db.conn.execute(
        "SELECT artifact_json, artifact_hash, verification_result"
        " FROM submissions WHERE id = ?",
        (submission_id,),
    ).fetchone()
    import hashlib as _hashlib

    assert row["verification_result"] == "PASS"
    assert (
        _hashlib.sha256(str(row["artifact_json"]).encode("utf-8")).hexdigest()
        == row["artifact_hash"]
    )
    assert json.loads(str(row["artifact_json"])) == {"sum": 6}
