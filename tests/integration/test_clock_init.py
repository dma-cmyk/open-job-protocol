"""Clock の初期化・mode 不変性・同時初期化の Integration テスト（レビュー指摘 2・3）。

- 入力検証は DB に触れる前に完了し、初期 mode・時刻は単一 transaction で確定する
- 失敗（指定漏れ・不正値・migration 適用後の途中停止）後も正しい設定で再試行できる
- 既存 DB の mode は SQL トリガーで本当に変更不可（realtime → test も不可）
- 複数プロセスが同じ新規 DB へ同時に初期化を試みても競合しない
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import textwrap
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

    def test_initialization_after_partial_migration(self, tmp_path: Path) -> None:
        """migration 適用後・Clock 行確定前に停止した DB を正しい設定で再開できる。"""
        path = tmp_path / "partial.sqlite3"
        conn = db.connect(path)
        db.migrate(conn)
        conn.close()

        conn2 = clock.initialize_database(path, ClockMode.TEST, test_now_us=TEST_T0_US)
        try:
            assert clock.read_mode(conn2) == ClockMode.TEST
            assert clock.now_for_read_snapshot(conn2) == TEST_T0_US
        finally:
            conn2.close()

    def test_clock_row_created_in_single_transaction(self, tmp_path: Path) -> None:
        """migration 適用だけでは Clock 行は作られず、初期化で作られる。"""
        path = tmp_path / "v.sqlite3"
        conn = db.connect(path)
        db.migrate(conn)
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM runtime_clock"
            ).fetchone()[0] == 0
        )
        conn.close()

        conn2 = clock.initialize_database(path, ClockMode.REALTIME)
        try:
            row = conn2.execute(
                "SELECT mode, test_now_utc_us FROM runtime_clock WHERE singleton_id = 1"
            ).fetchone()
            assert row is not None
            assert row["mode"] == "realtime"
            assert row["test_now_utc_us"] is None
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


CONCURRENT_INIT_SCRIPT = textwrap.dedent(
    """
    import sys
    from ojp import clock
    from ojp.domain import ClockMode

    db_path, mode, test_now = sys.argv[1], sys.argv[2], sys.argv[3]

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

WAIT_FOR_BARRIER = (
    "import sys, time, os\n"
    "while not os.path.exists(sys.argv[4]):\n"
    "    time.sleep(0.005)\n"
)


class TestConcurrentInitialization:
    def test_multiple_processes_initialize_same_new_db(self, tmp_path: Path) -> None:
        """複数プロセスが同じ新規 DB へ同時に初期化を試みても全員成功する。"""
        path = tmp_path / "race.sqlite3"
        barrier_path = tmp_path / "go"
        procs = [
            subprocess.Popen(
                [
                    sys.executable, "-c",
                    WAIT_FOR_BARRIER + CONCURRENT_INIT_SCRIPT,
                    str(path), "test", str(TEST_T0_US), str(barrier_path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for _ in range(6)
        ]
        try:
            barrier_path.touch()
            results = []
            for p in procs:
                out, err = p.communicate(timeout=60)
                results.append((p.returncode, out.strip(), err.strip()))
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

    def test_concurrent_processes_mixed_modes(self, tmp_path: Path) -> None:
        """test 起動と realtime 起動が同時に同じ新規 DB を初期化しようとしても、
        先に確定した設定だけが残り、後続は MODE_MISMATCH で拒否される。"""
        path = tmp_path / "mixed.sqlite3"
        barrier_path = tmp_path / "go"
        modes = ("test", "realtime", "test", "realtime", "test")
        procs = [
            subprocess.Popen(
                [
                    sys.executable, "-c",
                    WAIT_FOR_BARRIER + CONCURRENT_INIT_SCRIPT,
                    str(path), mode, str(TEST_T0_US), str(barrier_path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for mode in modes
        ]
        try:
            barrier_path.touch()
            results = []
            for p in procs:
                out, err = p.communicate(timeout=60)
                results.append((p.returncode, out.strip(), err.strip()))
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
