"""Clock の初期化・mode 不変性・同時初期化の Integration テスト（レビュー指摘 2・3）。

- 入力検証は DB に触れる前に完了し、初期 mode・時刻は単一 transaction で確定する
- 失敗（指定漏れ・不正値・migration 適用後の途中停止）後も正しい設定で再試行できる
- 既存 DB の mode は SQL トリガーで本当に変更不可（realtime → test も不可）
- 複数プロセスが同じ新規 DB へ同時に初期化を試みても競合しない
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from ojp import clock, db
from ojp.domain import ClockMode, ErrorCode, OjpError
from tests.conftest import TEST_T0_US, DbHandle


class TestInitializeValidation:
    def test_test_mode_requires_test_now(self, tmp_path: Path) -> None:
        path = tmp_path / "v.sqlite3"
        with pytest.raises(OjpError) as excinfo:
            clock.initialize_database(path, ClockMode.TEST)
        assert excinfo.value.code == ErrorCode.CLOCK_ERROR
        # 入力検証は DB に触れる前に完了するため、失敗の痕跡が残らない
        assert not path.exists()

    def test_negative_test_now_rejected(self, tmp_path: Path) -> None:
        path = tmp_path / "v.sqlite3"
        with pytest.raises(OjpError) as excinfo:
            clock.initialize_database(path, ClockMode.TEST, test_now_us=-1)
        assert excinfo.value.code == ErrorCode.INVALID_ARGUMENT
        assert not path.exists()

    @pytest.mark.parametrize("bad", [True, 1.0, "1000"])
    def test_non_int_test_now_rejected(self, tmp_path: Path, bad: object) -> None:
        path = tmp_path / "v.sqlite3"
        with pytest.raises(OjpError) as excinfo:
            clock.initialize_database(path, ClockMode.TEST, test_now_us=bad)  # type: ignore[arg-type]
        assert excinfo.value.code == ErrorCode.INVALID_ARGUMENT
        assert not path.exists()

    def test_test_now_rejected_for_realtime_mode(self, tmp_path: Path) -> None:
        path = tmp_path / "v.sqlite3"
        with pytest.raises(OjpError) as excinfo:
            clock.initialize_database(path, ClockMode.REALTIME, test_now_us=TEST_T0_US)
        assert excinfo.value.code == ErrorCode.INVALID_ARGUMENT
        assert not path.exists()


class TestInitializationAtomicity:
    def test_retry_after_missing_test_now_succeeds(self, tmp_path: Path) -> None:
        """test_now 指定漏れで失敗した後、正しい設定で再試行すれば test DB が作れる。"""
        path = tmp_path / "v.sqlite3"
        with pytest.raises(OjpError):
            clock.initialize_database(path, ClockMode.TEST)
        conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=TEST_T0_US)
        try:
            assert clock.read_mode(conn) == ClockMode.TEST
            assert clock.now_for_read_snapshot(conn) == TEST_T0_US
        finally:
            conn.close()

    def test_migration_and_clock_row_commit_together(self, tmp_path: Path, monkeypatch) -> None:
        """新規 DB の初期化では 001 の適用・Clock 行作成・schema_migrations への
        適用記録が同じ transaction で commit する（レビュー指摘A）。

        db.connect を差し替えて SQL の trace を取り、INSERT INTO runtime_clock
        が 001 の CREATE TABLE や INSERT INTO schema_migrations と同一の
        BEGIN..COMMIT 内にあることを検証する。旧実装（migration を commit した
        後に別 transaction で Clock 行を作る版）では、Clock 行の INSERT だけが
        单独の transaction に現れるため失敗する。
        """
        path = tmp_path / "atomic.sqlite3"
        log: list[str] = []

        def traced_connect(db_path: object) -> sqlite3.Connection:
            conn = sqlite3.connect(
                str(db_path), isolation_level=None, timeout=db.BUSY_TIMEOUT_MS / 1000
            )
            db._apply_pragmas(conn)
            conn.row_factory = sqlite3.Row
            conn.set_trace_callback(lambda stmt: log.append(" ".join(stmt.split())))
            return conn

        monkeypatch.setattr(db, "connect", traced_connect)
        conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=TEST_T0_US)
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM schema_migrations"
            ).fetchone()[0] == 1
            assert tuple(
                conn.execute(
                    "SELECT mode, test_now_utc_us FROM runtime_clock WHERE singleton_id = 1"
                ).fetchone()
            ) == ("test", TEST_T0_US)
        finally:
            conn.close()

        # trace を BEGIN..COMMIT（または ROLLBACK）ごとの transaction に分割する
        txns: list[list[str]] = []
        current: list[str] | None = None
        for stmt in log:
            upper = stmt.upper()
            if upper.startswith("BEGIN"):
                current = []
            elif upper.startswith(("COMMIT", "ROLLBACK")):
                if current is not None:
                    txns.append(current)
                current = None
            elif current is not None:
                current.append(stmt)

        # Clock 行の INSERT を含む transaction は全体で1つだけであり、
        # その中に 001 の DDL（例: participants）と schema_migrations への
        # 適用記録も同乗している
        clock_txns = [
            t for t in txns if any(s.startswith("INSERT INTO runtime_clock") for s in t)
        ]
        assert len(clock_txns) == 1, txns
        clock_txn = clock_txns[0]
        assert any(
            s.startswith("CREATE TABLE") and "participants" in s for s in clock_txn
        ), clock_txn
        assert any(
            s.startswith("INSERT INTO schema_migrations") for s in clock_txn
        ), clock_txn

        # 新規 DB の初期化 transaction は中途半端に commit されない:
        # 途中で失敗すれば migration 履歴も Clock 行も両方残らない
        path2 = tmp_path / "atomic2.sqlite3"
        conn2 = db.connect(path2)
        try:
            with pytest.raises(sqlite3.IntegrityError):
                with db.transaction(conn2, immediate=True):
                    conn2.execute(
                        "CREATE TABLE schema_migrations ("
                        " name TEXT PRIMARY KEY,"
                        " applied_at_us INTEGER NOT NULL CHECK (applied_at_us >= 0))"
                    )
                    for statement in _statements_of_001(conn2):
                        conn2.execute(statement)
                    conn2.execute(
                        "INSERT INTO runtime_clock (singleton_id, mode, test_now_utc_us)"
                        " VALUES (1, 'test', ?)",
                        (TEST_T0_US,),
                    )
                    # 同一 transaction 内での二重 INSERT は no_replace で拒否
                    conn2.execute(
                        "INSERT INTO runtime_clock (singleton_id, mode, test_now_utc_us)"
                        " VALUES (1, 'test', ?)",
                        (TEST_T0_US,),
                    )
            # rollback 後は 001 適用記録も Clock 行も残らない
            # （schema_migrations も同一 transaction 内で作ったため消える）
            assert (
                conn2.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
                    " AND name IN ('schema_migrations', 'runtime_clock')"
                ).fetchone()[0] == 0
            )
        finally:
            conn2.close()
        # 同じパスを正しい設定で初期化し直せる
        conn3 = clock.initialize_database(path2, ClockMode.TEST, test_now_us=TEST_T0_US)
        try:
            assert clock.read_mode(conn3) == ClockMode.TEST
        finally:
            conn3.close()

    def test_initialization_after_partial_migration(self, tmp_path: Path) -> None:
        """migration だけ適用された旧状態の DB を正しい設定で再開できる。

        新規初期化では Clock 行は 001 と同時確定するが、migration だけが
        適用された DB（migration 直後に停止した旧フローや旧バージョンで
        作られた DB）でも、Clock 行の確定は後から1つの transaction で行える。
        """
        path = tmp_path / "partial.sqlite3"
        conn = db.connect(path)
        db.migrate(conn)
        assert (
            conn.execute("SELECT COUNT(*) FROM runtime_clock").fetchone()[0] == 0
        )
        conn.close()

        conn2 = clock.initialize_database(path, ClockMode.TEST, test_now_us=TEST_T0_US)
        try:
            assert clock.read_mode(conn2) == ClockMode.TEST
            assert clock.now_for_read_snapshot(conn2) == TEST_T0_US
        finally:
            conn2.close()

    def test_reinitialize_existing_test_db_keeps_stored_time(self, test_db: DbHandle) -> None:
        """既存 test DB へ別の test_now で起動しても保存時刻は変わらない。

        mode は一致するため MODE_MISMATCH にはならないが、初期時刻の確定は
        行作成時だけなので保存値が書き換えられることはない。
        """
        conn = clock.initialize_database(
            test_db.path, ClockMode.TEST, test_now_us=TEST_T0_US + 1
        )
        try:
            assert clock.read_mode(conn) == ClockMode.TEST
            assert clock.now_for_read_snapshot(conn) == TEST_T0_US
        finally:
            conn.close()


class TestModeImmutability:
    def test_fresh_realtime_db_cannot_become_test_via_sql(self, realtime_db: DbHandle) -> None:
        """未使用の realtime DB でも SQL では test へ変更できない。"""
        with pytest.raises(sqlite3.IntegrityError):
            with db.transaction(realtime_db.conn, immediate=True):
                realtime_db.conn.execute(
                    "UPDATE runtime_clock SET mode = 'test', test_now_utc_us = 1"
                    " WHERE singleton_id = 1"
                )
        assert clock.read_mode(realtime_db.conn) == ClockMode.REALTIME

    def test_existing_test_db_cannot_revert_to_realtime(self, test_db: DbHandle) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            with db.transaction(test_db.conn, immediate=True):
                test_db.conn.execute(
                    "UPDATE runtime_clock SET mode = 'realtime', test_now_utc_us = NULL"
                    " WHERE singleton_id = 1"
                )
        assert clock.read_mode(test_db.conn) == ClockMode.TEST

    def test_clock_row_cannot_be_deleted(self, realtime_db: DbHandle) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            with db.transaction(realtime_db.conn, immediate=True):
                realtime_db.conn.execute("DELETE FROM runtime_clock WHERE singleton_id = 1")


class TestModeCannotBeReplaced:
    """レビュー指摘B: INSERT OR REPLACE 等の conflict 解決で既存行の mode を
    差し替えられる抜け道を塞ぐ（recursive_triggers が無効な接続では削除
    トリガーが OR REPLACE の暗黙 DELETE を発火させないため、BEFORE INSERT
    トリガーで拒否する）。"""

    def test_realtime_db_cannot_be_replaced_with_test_row(self, realtime_db: DbHandle) -> None:
        """realtime DB の行を test 行へ差し替えられない。"""
        for conflict in ("OR REPLACE", "OR IGNORE", "OR ABORT", ""):
            with pytest.raises(sqlite3.IntegrityError):
                with db.transaction(realtime_db.conn, immediate=True):
                    realtime_db.conn.execute(
                        f"INSERT {conflict} INTO runtime_clock"
                        " (singleton_id, mode, test_now_utc_us) VALUES (1, 'test', 100)"
                    )
        assert clock.read_mode(realtime_db.conn) == ClockMode.REALTIME

    def test_test_db_cannot_be_replaced_with_realtime_row(self, test_db: DbHandle) -> None:
        """test DB の行を realtime 行へ差し替えられない。"""
        for conflict in ("OR REPLACE", "OR IGNORE", "OR ABORT", ""):
            with pytest.raises(sqlite3.IntegrityError):
                with db.transaction(test_db.conn, immediate=True):
                    test_db.conn.execute(
                        f"INSERT {conflict} INTO runtime_clock"
                        " (singleton_id, mode, test_now_utc_us) VALUES (1, 'realtime', NULL)"
                    )
        assert clock.read_mode(test_db.conn) == ClockMode.TEST

    def test_replaced_test_row_cannot_pass_mode_check(self, realtime_db: DbHandle) -> None:
        """差し替えが拒否されるため、test mode での再接続も通らない。

        修正前（OR REPLACE が成功する実装）なら行が test へ置き換わり、
        test mode での起動設定照合が成功してしまう経路。
        """
        conn = realtime_db.conn
        with pytest.raises(sqlite3.IntegrityError):
            with db.transaction(conn, immediate=True):
                conn.execute(
                    "INSERT OR REPLACE INTO runtime_clock"
                    " (singleton_id, mode, test_now_utc_us) VALUES (1, 'test', 100)"
                )
        # realtime のまま
        assert clock.read_mode(conn) == ClockMode.REALTIME
        # test 起動設定では MODE_MISMATCH
        with pytest.raises(OjpError) as excinfo:
            clock.initialize_database(realtime_db.path, ClockMode.TEST, test_now_us=100)
        assert excinfo.value.code == ErrorCode.MODE_MISMATCH

    def test_recursive_triggers_setting_does_not_affect_protection(
        self, realtime_db: DbHandle
    ) -> None:
        """トリガーによる保護は recursive_triggers 設定に依存しない。"""
        for value in ("0", "1", "OFF", "ON"):
            conn = realtime_db.fresh_conn()
            try:
                conn.execute(f"PRAGMA recursive_triggers = {value}")
                with pytest.raises(sqlite3.IntegrityError):
                    with db.transaction(conn, immediate=True):
                        conn.execute(
                            "INSERT OR REPLACE INTO runtime_clock"
                            " (singleton_id, mode, test_now_utc_us)"
                            " VALUES (1, 'test', 100)"
                        )
            finally:
                conn.close()

CONCURRENT_INIT_SCRIPT = textwrap.dedent(
    """
    import os, sys, time
    from ojp import clock
    from ojp.domain import ClockMode

    db_path, mode, test_now, ready_dir, go_path = (
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    )

    # import と事前準備が済んだことを親へ通知する
    ready_path = os.path.join(ready_dir, os.environ["OJP_CHILD_ID"])
    with open(ready_path, "w") as f:
        f.write("ready")
    # 親が全子の ready を確認してから go を作るため、ここでの待機は
    # 確実に初期化直前（ロック取得の直前）である
    while not os.path.exists(go_path):
        time.sleep(0.002)

    try:
        if mode == "test":
            conn = clock.initialize_database(
                db_path, ClockMode.TEST, test_now_us=int(test_now)
            )
        else:
            conn = clock.initialize_database(db_path, ClockMode.REALTIME)
        conn.close()
        print("OK")
    except Exception as exc:
        code = getattr(exc, "code", type(exc).__name__)
        print(f"FAIL:{code}:{exc}", file=sys.stderr)
        sys.exit(3)
    """
)


def _spawn_init_children(
    tmp_path: Path, path: Path, modes: list[str]
) -> list[subprocess.Popen[str]]:
    """全子プロセスを、初期化直前まで準備完了した状態で起動する。

    子は import と事前準備を済ませてから ready ファイルを作り、親が go
    ファイルを作るのを待つ。親は全子の ready を確認してから go を作るため、
    全子プロセスが初期化（ロック取得）を同時に開始することが保証される
    （レビュー指摘C）。
    """
    ready_dir = tmp_path / "ready"
    ready_dir.mkdir()
    go_path = tmp_path / "go"
    procs: list[subprocess.Popen[str]] = []
    for i, mode in enumerate(modes):
        env = dict(os.environ, OJP_CHILD_ID=str(i))
        procs.append(
            subprocess.Popen(
                [
                    sys.executable, "-c", CONCURRENT_INIT_SCRIPT,
                    str(path), mode, str(TEST_T0_US), str(ready_dir), str(go_path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )
        )
    try:
        _wait_all_ready(procs, ready_dir, len(modes))
        go_path.touch()
    except BaseException:
        go_path.touch()  # 子がブロックしないように解除してから例外を投げ直す
        for p in procs:
            if p.poll() is None:
                p.kill()
        raise
    return procs


def _statements_of_001(conn: sqlite3.Connection) -> list[str]:
    """001_initial.sql のステートメント列（runner と同じ分割方法）。"""
    from importlib import resources

    sql = (
        resources.files("ojp")
        .joinpath("migrations")
        .joinpath("001_initial.sql")
        .read_text(encoding="utf-8")
    )
    return db._split_statements(sql)


def _wait_all_ready(
    procs: list[subprocess.Popen[str]], ready_dir: Path, count: int,
    timeout: float = 30.0,
) -> None:
    """全子プロセスの ready を待つ。子が異常終了したら即座に失敗させる。"""
    deadline = time.monotonic() + timeout
    while True:
        if all((ready_dir / str(i)).exists() for i in range(count)):
            return
        if all(p.poll() is not None for p in procs):
            raise AssertionError("all children exited before becoming ready")
        if time.monotonic() > deadline:
            raise TimeoutError("children did not become ready in time")
        time.sleep(0.005)


def _collect(procs: list[subprocess.Popen[str]]) -> list[tuple[int, str, str]]:
    results = []
    for p in procs:
        out, err = p.communicate(timeout=60)
        results.append((p.returncode, out.strip(), err.strip()))
    return results


class TestConcurrentInitialization:
    def test_multiple_processes_initialize_same_new_db(self, tmp_path: Path) -> None:
        """複数プロセスが同じ新規 DB へ同時に初期化を試みても全員成功する。

        全子は import・準備完了を親へ通知したうえで、親の解除を受けて
        初期化（ロック取得）を同時に開始する。旧実装（ロック取得前に
        適用履歴を読む版）なら CREATE TABLE 競合などで失敗する負荷条件。
        """
        path = tmp_path / "race.sqlite3"
        procs = _spawn_init_children(tmp_path, path, ["test"] * 6)
        try:
            results = _collect(procs)
        finally:
            for p in procs:
                if p.poll() is None:
                    p.kill()

        for rc, out, err in results:
            assert rc == 0, results
            assert out == "OK", results

        conn = db.connect(path)
        try:
            assert clock.read_mode(conn) == ClockMode.TEST
            assert conn.execute(
                "SELECT COUNT(*) FROM schema_migrations"
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT test_now_utc_us FROM runtime_clock WHERE singleton_id = 1"
            ).fetchone()["test_now_utc_us"] == TEST_T0_US
        finally:
            conn.close()

    def test_old_implementation_fails_same_race(self, tmp_path: Path) -> None:
        """旧実装（BEGIN 前に読んだ適用状況で適用を決める版）に差し替えると、
        同じ同期条件で確実に失敗することを検証する（レビュー指摘C）。

        git show 3f1b653:src/ojp/db.py の旧 migrate を取り出し、テスト内で
        再現して同じバリア条件で実行する。旧実装は全プロセスが「未適用」と
        判断したまま一斉に BEGIN IMMEDIATE するため、DDL 競合で複数プロセスが
        失敗する（現行実装は全員成功する）。
        """
        import subprocess as sp

        # 旧実装を git から取り出す
        old_db = sp.run(
            ["git", "show", "3f1b653:src/ojp/db.py"],
            capture_output=True, text=True, check=True,
        ).stdout
        old_clock = sp.run(
            ["git", "show", "3f1b653:src/ojp/clock.py"],
            capture_output=True, text=True, check=True,
        ).stdout
        old_domain = sp.run(
            ["git", "show", "3f1b653:src/ojp/domain.py"],
            capture_output=True, text=True, check=True,
        ).stdout
        old_sql = sp.run(
            ["git", "show", "3f1b653:src/ojp/migrations/001_initial.sql"],
            capture_output=True, text=True, check=True,
        ).stdout

        old_pkg = tmp_path / "oldpkg" / "ojp"
        (old_pkg / "migrations").mkdir(parents=True)
        (old_pkg / "__init__.py").write_text("")
        (old_pkg / "db.py").write_text(old_db)
        (old_pkg / "clock.py").write_text(old_clock)
        (old_pkg / "domain.py").write_text(old_domain)
        (old_pkg / "migrations" / "001_initial.sql").write_text(old_sql)

        path = tmp_path / "race.sqlite3"
        count = 6
        sync_dir = tmp_path / "mig-sync"
        sync_dir.mkdir()
        ready_dir = tmp_path / "ready"
        ready_dir.mkdir()
        go_path = tmp_path / "go"
        go_path.touch()

        # 旧実装の initialize_database は、内部で db.migrate を呼ぶ。
        # 旧 migrate は BEGIN 前に applied を読むが、テスト同期フックを
        # 持たないため、テスト側で同じ「migration 状態読取後・BEGIN IMMEDIATE 前」
        # のバリアを再現する。
        script = textwrap.dedent(
            """
            import os, sys, time
            from ojp import clock, db
            from ojp.domain import ClockMode
            db_path, mode, test_now = sys.argv[1], sys.argv[2], sys.argv[3]
            try:
                conn = db.connect(db_path)
                # 旧実装の migrate 本体（BEGIN 前に適用状況を読む）
                applied = set()
                if db._migrations_table_exists(conn):
                    applied = set(db.applied_migrations(conn))
                names = db._migration_names()
                # バリア: migration 状態読取後・BEGIN IMMEDIATE 前
                token = f"{os.getpid()}.{time.monotonic_ns()}"
                open(os.path.join(os.environ["OJP_MIGRATION_TEST_SYNC"], token), "w").write("r")
                expected = int(os.environ["OJP_MIGRATION_TEST_SYNC_COUNT"])
                deadline = time.monotonic() + 30.0
                while len(os.listdir(os.environ["OJP_MIGRATION_TEST_SYNC"])) < expected:
                    if time.monotonic() > deadline:
                        raise RuntimeError("sync timeout")
                    time.sleep(0.005)
                for name in names:
                    if name in applied:
                        continue
                    sql = db._load_migration_sql(name)
                    with db.transaction(conn, immediate=True):
                        if not db._migrations_table_exists(conn):
                            conn.execute(
                                "CREATE TABLE schema_migrations ( name TEXT PRIMARY KEY,"
                                " applied_at_us INTEGER NOT NULL CHECK (applied_at_us >= 0))"
                            )
                        for stmt in db._split_statements(sql):
                            conn.execute(stmt)
                        conn.execute(
                            "INSERT INTO schema_migrations (name, applied_at_us) VALUES (?, ?)",
                            (name, int(time.time() * 1_000_000)),
                        )
                # 旧実装は Clock 行を別 transaction で作る
                row = conn.execute(
                    "SELECT 1 FROM runtime_clock WHERE singleton_id = 1"
                ).fetchone()
                if row is None:
                    with db.transaction(conn, immediate=True):
                        conn.execute(
                            "INSERT INTO runtime_clock (singleton_id, mode, test_now_utc_us)"
                            " VALUES (1, 'test', ?)", (int(test_now),))
                conn.close()
                print("OK")
            except Exception as exc:
                print(f"FAIL:{type(exc).__name__}:{exc}", file=sys.stderr)
                sys.exit(3)
            """
        )
        procs: list[subprocess.Popen[str]] = []
        for i in range(count):
            env = dict(
                os.environ,
                PYTHONPATH=str(tmp_path / "oldpkg"),
                OJP_CHILD_ID=str(i),
                OJP_MIGRATION_TEST_SYNC=str(sync_dir),
                OJP_MIGRATION_TEST_SYNC_COUNT=str(count),
            )
            procs.append(
                subprocess.Popen(
                    [sys.executable, "-c", script, str(path), "test", str(TEST_T0_US)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                )
            )
        try:
            results = _collect(procs)
        finally:
            for p in procs:
                if p.poll() is None:
                    p.kill()

        ok = [r for r in results if r[0] == 0]
        failed = [r for r in results if r[0] != 0]
        # 旧実装では複数プロセスが確実に失敗する（全員成功はありえない）
        assert len(failed) >= 2, (
            f"old implementation should fail for multiple processes, got {results}"
        )
        assert len(ok) + len(failed) == count

    def test_concurrent_processes_mixed_modes(self, tmp_path: Path) -> None:
        """test 起動と realtime 起動が同時に同じ新規 DB を初期化しようとしても、
        先に確定した設定だけが残り、後続は MODE_MISMATCH で拒否される。"""
        path = tmp_path / "mixed.sqlite3"
        modes = ["test", "realtime", "test", "realtime", "test"]
        procs = _spawn_init_children(tmp_path, path, modes)
        try:
            results = _collect(procs)
        finally:
            for p in procs:
                if p.poll() is None:
                    p.kill()

        oks = [r for r in results if r[0] == 0]
        mismatches = [r for r in results if r[0] == 3 and "MODE_MISMATCH" in r[2]]
        assert len(oks) >= 1, results
        assert len(oks) + len(mismatches) == len(results), results

        conn = db.connect(path)
        try:
            final_mode = clock.read_mode(conn)
            for mode, (rc, _out, _err) in zip(modes, results):
                if rc == 0:
                    assert ClockMode(mode) == final_mode
        finally:
            conn.close()

    def test_all_writers_synchronize_before_write_lock(self, tmp_path: Path) -> None:
        """旧実装が壊れる箇所（migration 状態読取後・書込ロック取得前）の競合を
        確実に踏む同時初期化テスト（レビュー指摘C）。

        migrate の BEGIN IMMEDIATE 直前（適用状況の読み取り後）に置いた
        テスト専用フック（環境変数でのみ有効・通常経路は no-op）で、全子プロセスを
        「migration 状態を読み終えたがまだ書込ロックを取っていない」状態で揃える。
        現行実装はロック内で適用状況を再確認するため全員成功するが、旧実装
        （BEGIN 前に読んだ適用状況で適用を決める版）ではこの位置に到達した時点で
        全員が未適用と判断済みであり、全員が一斉に BEGIN IMMEDIATE して
        DDL/UNIQUE 競合で失敗する。
        """
        path = tmp_path / "race.sqlite3"
        count = 6
        sync_dir = tmp_path / "mig-sync"
        sync_dir.mkdir()
        ready_dir = tmp_path / "ready-unused"
        ready_dir.mkdir()
        go_path = tmp_path / "go-unused"
        go_path.touch()  # 子の ready/go 待機を即通過させ、ロック内同期だけを使う
        procs: list[subprocess.Popen[str]] = []
        for i in range(count):
            env = dict(
                os.environ,
                OJP_CHILD_ID=str(i),
                OJP_MIGRATION_TEST_SYNC=str(sync_dir),
                OJP_MIGRATION_TEST_SYNC_COUNT=str(count),
            )
            procs.append(
                subprocess.Popen(
                    [
                        sys.executable, "-c", CONCURRENT_INIT_SCRIPT,
                        str(path), "test", str(TEST_T0_US),
                        str(ready_dir), str(go_path),
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                )
            )
        try:
            results = _collect(procs)
        finally:
            for p in procs:
                if p.poll() is None:
                    p.kill()

        for rc, out, err in results:
            assert rc == 0, results
            assert out == "OK", results

        conn = db.connect(path)
        try:
            assert clock.read_mode(conn) == ClockMode.TEST
            assert conn.execute(
                "SELECT COUNT(*) FROM schema_migrations"
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT test_now_utc_us FROM runtime_clock WHERE singleton_id = 1"
            ).fetchone()["test_now_utc_us"] == TEST_T0_US
        finally:
            conn.close()
