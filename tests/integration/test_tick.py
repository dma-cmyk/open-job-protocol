"""Phase 4 S2 の integration テスト: scheduler tick（計画書 第12節・第14節・
第15節・N08・N09・N11・X10 相当）。

実ファイル SQLite（test mode の共有 Clock）で検証する:

- 期限後の自動承認 → 支払い処理までが 1 tick で進む
- watch(max_iterations=1) と tick_once が同じ効果（--once と --watch が
  同じ処理を呼ぶ）
- 独立プロセス（python -m ojp.scheduler --root <path> --once --json）から
  実行して同じ結果になり、stdout が単一 JSON
- 全プロセス停止後も期限は DB に残り、再起動時に過期限分を処理する（X10）
- N08: 専用 timing fixture（Root Lease 300秒・Child Lease 60秒・
  review_window 120秒。カタログ entry の timing_policy で Child にだけ
  別 timing を事前許可する）で有効提出後、
  `child_expires_at < now < child_review_due_at < root_lease_expires_at`
  を確認してから tick を 1 回実行しても SUBMITTED 維持・Acceptance・
  PaymentOperation 0 件・残高不変。その後 review_due_at まで進めると
  自動承認（N11）
- N09: Child の Worker B が提出した後に Parent（Root）を失効させても、
  Child の期限承認で B への支払い予約と送金が成立する

カタログ entry に timing_policy を持たない Root では、従来どおり Child が
Root 公開版の timing を継承する（後方互換。job_versions を直接 UPDATE
しない）。
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from ojp import clock, db, ledger, scheduler, service, verification
from ojp.domain import (
    ClockMode,
    ErrorCode,
    JobState,
    PaymentStatus,
    TaskCatalogEntry,
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

# N09 用: Lease 30秒・検収 120秒（N08 とは別 fixture。Root / Child で同じ
# timing を継承する従来構成で、Parent 失効後の Child 継続を検証する）
N09_TIMING = TimingPolicy(
    lease_seconds=30,
    heartbeat_seconds=20,
    review_window_seconds=120,
    dispute_window_seconds=30,
)

# N08 専用 timing fixture（計画書 第18節「N08の専用時刻fixture」）:
# Root JobVersion は Lease 300秒（Root deadline は t0+300秒 より後）、
# カタログの part-1 entry は Lease 60秒・review_window 120秒を Child の
# timing として事前許可する。これにより A の Root Lease は t0+300秒、
# B の Child Lease は t0+60秒、t0+1秒の有効提出で
# review_due_at = t0+121秒となり、
# child_expires_at < tick < child_review_due_at < root_lease_expires_at
# の窓を作れる
N08_ROOT_TIMING = TimingPolicy(
    lease_seconds=300,
    heartbeat_seconds=20,
    review_window_seconds=30,
    dispute_window_seconds=30,
)
N08_CHILD_TIMING = TimingPolicy(
    lease_seconds=60,
    heartbeat_seconds=20,
    review_window_seconds=120,
    dispute_window_seconds=30,
)


def _n08_catalog() -> tuple:
    """N08 専用カタログ: part-1 entry にだけ Child 用 timing を事前許可する。"""
    _root_def, base = load_poc_catalog()
    catalog = [
        TaskCatalogEntry(
            task_key=entry.task_key,
            input_values=entry.input_values,
            expected=entry.expected,
            budget_cap_units=entry.budget_cap_units,
            timing_policy=(
                N08_CHILD_TIMING if entry.task_key == "part-1" else None
            ),
        )
        for entry in base
    ]
    return _root_def, catalog


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


def _leased_root(demo_db, *, suffix="r1", timing=None, catalog=None):
    """create_root → fund_root → A が Claim 済みの Root を返す
    （(root_id, version_id, lease_id)）。catalog を渡すと既定の PoC
    カタログの代わりにそれを使う（N08 専用カタログ等）。"""
    if catalog is None:
        _root_def, catalog = load_poc_catalog()
    else:
        _root_def, _ = load_poc_catalog()
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


def _run_scheduler_once(
    root_dir: Path, *, clock_mode: str | None = "test"
) -> subprocess.CompletedProcess[str]:
    """独立プロセスで python -m ojp.scheduler --root <path> --once --json。

    プロセス待機の timeout は OS の単調時計に依存する通常の subprocess
    timeout を使い、Job 期限を実時間 sleep で待たない（計画書 第7節）。

    clock_mode は起動時に期待する DB の Clock mode（--clock-mode に渡す）。
    このファイルの DB は test mode で作られているため既定は "test"
    （第7節「test DB の利用には各プロセスで明示的な test mode 起動が必要」）。
    None を渡すと --clock-mode を省略する（scheduler 側の既定 realtime の
    動作を検証するため）。
    """
    cmd = [
        sys.executable,
        "-m",
        "ojp.scheduler",
        "--root",
        str(root_dir),
        "--once",
        "--json",
    ]
    if clock_mode is not None:
        cmd += ["--clock-mode", clock_mode]
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
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


def test_tick_keeps_submitted_between_lease_expiry_and_review_due(
    demo_db, tmp_path
):
    """N08（計画書 第18節「N08の専用時刻fixture」）: Root Lease 300秒・
    カタログの part-1 entry に Child 用 timing（Lease 60秒・
    review_window 120秒）を事前許可し、共有 Clock を t0 に固定したまま
    A が Root を Claim・Child を作成、B も Child を Claim する。B は
    t0+1秒で有効提出（review_due_at = t0+121秒）。harness は Clock を
    t0+61秒へ進め、
    `child_expires_at < now < child_review_due_at < root_lease_expires_at`
    を確認してから独立プロセスの tick
    （python -m ojp.scheduler --root <path> --once --json）を 1 回実行する
    （計画書 第7節「期限処理の E2E は時刻更新 commit 後に独立プロセスの
    ojp tick --once 完了を待つ」）。

    tick 後も Child は SUBMITTED 維持・Acceptance・PaymentOperation
    （Child 分）0 件・全口座残高・Wallet・PaymentOperation・Acceptance が
    提出直後のスナップショットから 1 行単位で不変。Lease の
    expires_at_us は監査値として t0+60秒のまま、closed_reason は
    'submitted' のまま、expiry による Event や新 Lease は作られない。
    窓の中では資金を動かす操作を一切行わない。
    """
    _root_def, n08_catalog = _n08_catalog()
    root_id, root_version_id, root_lease_id = _leased_root(
        demo_db, suffix="n08-r", timing=N08_ROOT_TIMING, catalog=n08_catalog
    )
    # A の Root Lease は t0+300秒（Root の timing は Lease 300秒）
    root_lease = demo_db.conn.execute(
        "SELECT * FROM leases WHERE id = ?", (root_lease_id,)
    ).fetchone()
    root_lease_expires_at = int(root_lease["expires_at_us"])
    assert root_lease_expires_at == TEST_T0_US + 300_000_000

    # 共有 Clock は t0 のまま A が Child を作成し、B が Claim する
    created = service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease_id,
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id="create:n08",
    )
    child_id = created.data["child_id"]
    # Child の JobVersion にはカタログの timing（Lease 60秒・review 120秒）
    # が保存される（Root 公開版の timing 継承ではない）
    child_version = demo_db.conn.execute(
        "SELECT timing_policy FROM job_versions WHERE id = ?",
        (created.data["version_id"],),
    ).fetchone()
    assert TimingPolicy.model_validate_json(
        child_version["timing_policy"]
    ) == N08_CHILD_TIMING
    claimed = service.claim(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=created.data["version_id"],
        operation_id="claim:n08-b",
    )
    del root_version_id

    # B は共有 Clock の t0+1秒で有効提出する
    clock.set_test_now(demo_db.conn, TEST_T0_US + 1_000_000)
    submitted = service.submit(
        demo_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=claimed.data["lease_id"],
        version_id=created.data["version_id"],
        artifact_json='{"sum": 6}',
        operation_id="submit:n08",
    )
    submission_id = submitted.data["submission_id"]
    review_due_at = demo_db.conn.execute(
        "SELECT review_due_at_us FROM submissions WHERE id = ?",
        (submission_id,),
    ).fetchone()["review_due_at_us"]
    # t0+1秒提出・review 120秒 → review_due_at = t0+121秒
    assert review_due_at == TEST_T0_US + 121_000_000
    lease = demo_db.conn.execute(
        "SELECT * FROM leases WHERE job_id = ?", (child_id,)
    ).fetchone()
    child_expires_at = int(lease["expires_at_us"])
    # B の Child Lease は t0+60秒（カタログの Child timing は Lease 60秒）
    assert child_expires_at == TEST_T0_US + 60_000_000

    # 提出直後のスナップショット（送金待ちを残さない構成）
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

    # harness は共有 Clock を t0+61秒へ進め、計画書の時刻関係
    # （child_expires_at < now < child_review_due_at < root_lease_expires_at）
    # を確認する
    now = TEST_T0_US + 61_000_000
    assert child_expires_at < now < review_due_at < root_lease_expires_at

    # DB をプロジェクトルート風の配置へ複製し、時刻更新 transaction を
    # commit してから（時刻更新 transaction に Job 処理を混在させない）、
    # 期限処理は独立プロセスの tick で行う（計画書 第7節「期限処理の
    # E2E は時刻更新 commit 後に独立プロセスの ojp tick --once 完了を
    # 待つ」）
    root_dir = tmp_path / "proj"
    (root_dir / "data").mkdir(parents=True)
    db_path = root_dir / "data" / "ojp.sqlite3"
    demo_db.conn.close()
    db_path.write_bytes(Path(demo_db.path).read_bytes())

    conn = db.connect(db_path)
    try:
        clock.set_test_now(conn, now)
    finally:
        conn.close()

    # 独立プロセスの tick を 1 回実行し、完了を待って終了コードと stdout
    # の JSON を確認する
    proc = _run_scheduler_once(root_dir)
    assert proc.returncode == 0, proc.stderr
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    tick_result = json.loads(lines[0])
    assert tick_result["counts"]["approved_submissions"] == 0
    assert tick_result["error"] is None

    # tick 後の状態は新しい接続で読み直して確認する
    conn = db.connect(db_path)
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
        # Lease は監査値のまま（t0+60秒）・'submitted' で閉じたまま
        fresh_lease = conn.execute(
            "SELECT * FROM leases WHERE job_id = ?", (child_id,)
        ).fetchone()
        assert fresh_lease["expires_at_us"] == child_expires_at
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

    # N11: review_due_at まで進めると通常の自動承認が機能する。こちらも
    # 時刻更新 commit 後に独立プロセスの tick で行う
    conn = db.connect(db_path)
    try:
        clock.set_test_now(conn, review_due_at)
    finally:
        conn.close()

    proc = _run_scheduler_once(root_dir)
    assert proc.returncode == 0, proc.stderr
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    tick_result = json.loads(lines[0])
    assert tick_result["counts"]["approved_submissions"] == 1
    assert tick_result["error"] is None

    conn = db.connect(db_path)
    try:
        assert _job(conn, child_id)["state"] == JobState.DONE.value
        assert _wallets(conn)[AGENT_B_ID] == TEN
        ledger.assert_ledger_invariants(conn, root_id)
    finally:
        conn.close()


def test_child_without_catalog_timing_inherits_root_timing(demo_db):
    """後方互換: カタログ entry に timing_policy を持たない従来の Root では、
    Child は Root 公開版の timing を継承する（Child の JobVersion の
    timing_policy が Root と一致する）。"""
    custom_root_timing = TimingPolicy(
        lease_seconds=90,
        heartbeat_seconds=20,
        review_window_seconds=45,
        dispute_window_seconds=30,
    )
    root_id, _root_v, root_lease_id = _leased_root(
        demo_db, suffix="inherit-r", timing=custom_root_timing
    )
    created = service.create_child(
        demo_db.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease_id,
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id="create:inherit",
    )
    child_version = demo_db.conn.execute(
        "SELECT timing_policy FROM job_versions WHERE id = ?",
        (created.data["version_id"],),
    ).fetchone()
    assert TimingPolicy.model_validate_json(
        child_version["timing_policy"]
    ) == custom_root_timing
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
        demo_db, suffix="n09-r", timing=N09_TIMING
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


# ---------------------------------------------------------------------------
# Clock mode の起動照合（計画書 第7節「起動設定と DB の mode が違えば起動を
# 拒否する」）
#
# 成功経路（test mode の DB に --clock-mode test で起動して正常に処理される）
# は既存の test_subprocess_tick_once_json / test_overdue_processed_after_restart
# / test_tick_keeps_submitted_between_lease_expiry_and_review_due が
# _run_scheduler_once（--clock-mode test 付き）で検証している。
# ---------------------------------------------------------------------------


def _copy_db_to_project_root(src_path: Path, root_dir: Path) -> Path:
    """DB ファイルを scheduler が解決する <root>/data/ojp.sqlite3 へ複製する。

    呼出前に接続を閉じて WAL を checkpoint させること（既存テストと同じ手順）。
    """
    (root_dir / "data").mkdir(parents=True)
    db_path = root_dir / "data" / "ojp.sqlite3"
    db_path.write_bytes(Path(src_path).read_bytes())
    return db_path


def _state_snapshot(conn, root_id, child_id):
    """起動拒否の前後比較用スナップショット（Job 状態・Acceptance・
    PaymentOperation・Receipt・口座残高・Wallet）。"""
    return {
        "job_state": _job(conn, child_id)["state"],
        "acceptances": conn.execute(
            "SELECT COUNT(*) AS c FROM acceptances WHERE job_id = ?",
            (child_id,),
        ).fetchone()["c"],
        "payments": conn.execute(
            "SELECT COUNT(*) AS c FROM payment_operations WHERE root_id = ?",
            (root_id,),
        ).fetchone()["c"],
        "receipts": conn.execute(
            "SELECT COUNT(*) AS c FROM transfer_receipts"
        ).fetchone()["c"],
        "buckets": {
            (r["owner_job_id"], r["bucket"], r["source_key"]): int(
                r["amount_units"]
            )
            for r in conn.execute(
                "SELECT owner_job_id, bucket, source_key, amount_units"
                " FROM budget_accounts WHERE root_id = ?",
                (root_id,),
            )
        },
        "wallets": _wallets(conn),
    }


def test_tick_rejects_realtime_clock_mode_on_test_db(demo_db, tmp_path):
    """計画書 第7節: test mode の DB に対して --clock-mode realtime で起動
    すると、起動設定と DB の mode が違うため MODE_MISMATCH（終了コード 2）
    で起動を拒否し、Lifecycle を 1 つも実行しない。

    検収期限を越えた提出済み Child（一致する mode で起動すれば自動承認が
    走る状態。成功経路は test_subprocess_tick_once_json が検証）を用意し、
    拒否後に Job 状態・Acceptance・PaymentOperation・Receipt・口座残高・
    Wallet が起動前から不変であることを新しい接続で確認する。
    """
    root_id, child_id, submission_id = _submitted_child(demo_db, suffix="mm-rt")
    review_due_at = demo_db.conn.execute(
        "SELECT review_due_at_us FROM submissions WHERE id = ?",
        (submission_id,),
    ).fetchone()["review_due_at_us"]
    clock.set_test_now(demo_db.conn, review_due_at)
    before = _state_snapshot(demo_db.conn, root_id, child_id)
    # 前提: 期限を越えた提出済みで、一致 mode なら処理される状態
    assert before["job_state"] == JobState.SUBMITTED.value
    assert before["acceptances"] == 0
    assert before["payments"] == 0

    root_dir = tmp_path / "proj"
    demo_db.conn.close()
    db_path = _copy_db_to_project_root(demo_db.path, root_dir)

    proc = _run_scheduler_once(root_dir, clock_mode="realtime")
    assert proc.returncode == 2, proc.stderr
    # stdout は単一 JSON（余計なログが混ざらない）
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    result = json.loads(lines[0])
    assert result["error"]["code"] == ErrorCode.MODE_MISMATCH.value

    # 期限処理は 1 件も実行されていない（起動前から不変）
    conn = db.connect(db_path)
    try:
        after = _state_snapshot(conn, root_id, child_id)
        assert after == before
        assert after["job_state"] == JobState.SUBMITTED.value
        ledger.assert_ledger_invariants(conn, root_id)
    finally:
        conn.close()


def test_tick_rejects_test_clock_mode_on_realtime_db(tmp_path):
    """計画書 第7節: realtime mode の DB に対して --clock-mode test で起動
    すると、同じく MODE_MISMATCH（終了コード 2）で起動を拒否する。"""
    rt_src = tmp_path / "ojp-realtime.sqlite3"
    conn = clock.initialize_database(rt_src, ClockMode.REALTIME)
    conn.close()
    root_dir = tmp_path / "proj"
    db_path = _copy_db_to_project_root(rt_src, root_dir)

    proc = _run_scheduler_once(root_dir, clock_mode="test")
    assert proc.returncode == 2, proc.stderr
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    result = json.loads(lines[0])
    assert result["error"]["code"] == ErrorCode.MODE_MISMATCH.value

    # 何も処理・記録されていない（空の DB のまま）
    conn = db.connect(db_path)
    try:
        for table in (
            "jobs",
            "acceptances",
            "payment_operations",
            "transfer_receipts",
        ):
            assert (
                conn.execute(
                    f"SELECT COUNT(*) AS c FROM {table}"
                ).fetchone()["c"]
                == 0
            )
    finally:
        conn.close()


def test_tick_defaults_to_realtime_and_rejects_test_db(demo_db, tmp_path):
    """計画書 第7節: --clock-mode を省略した場合は realtime として扱われ、
    test mode の DB では MODE_MISMATCH（終了コード 2）で起動を拒否する
    （明示しない限り test DB は使えない）。"""
    root_id, child_id, submission_id = _submitted_child(demo_db, suffix="mm-om")
    review_due_at = demo_db.conn.execute(
        "SELECT review_due_at_us FROM submissions WHERE id = ?",
        (submission_id,),
    ).fetchone()["review_due_at_us"]
    clock.set_test_now(demo_db.conn, review_due_at)
    before = _state_snapshot(demo_db.conn, root_id, child_id)

    root_dir = tmp_path / "proj"
    demo_db.conn.close()
    db_path = _copy_db_to_project_root(demo_db.path, root_dir)

    proc = _run_scheduler_once(root_dir, clock_mode=None)
    assert proc.returncode == 2, proc.stderr
    result = json.loads(proc.stdout.strip())
    assert result["error"]["code"] == ErrorCode.MODE_MISMATCH.value

    conn = db.connect(db_path)
    try:
        assert _state_snapshot(conn, root_id, child_id) == before
    finally:
        conn.close()
