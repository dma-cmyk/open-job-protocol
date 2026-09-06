"""共有 Clock の Integration テスト（計画書 第7節・第20節 Phase 1）。

- 実ファイル SQLite と実 subprocess を使い、メモリDBや同一プロセス内呼び出しで代替しない
- 独立した複数プロセスが同じ注入時刻を観測し、全プロセス停止・再起動後も固定時刻が保持される
- 時刻の後退と、起動設定と DB の mode 不一致が拒否される
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import sqlite3

from ojp import clock, db
from ojp.domain import ClockMode, ErrorCode, OjpError
from tests.conftest import TEST_T0_US, DbHandle

OBSERVE_SCRIPT = textwrap.dedent(
    """
    import sys
    from ojp import clock, db
    from ojp.domain import ClockMode, OjpError

    db_path, mode, operation, arg = (
        sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    )
    try:
        conn = db.connect(db_path)
        clock.assert_mode(conn, ClockMode(mode))
        if operation == "read":
            print(clock.now_for_read_snapshot(conn))
        elif operation == "write":
            with db.transaction(conn, immediate=True):
                print(clock.now_for_write_transaction(conn))
        elif operation == "set":
            print(clock.set_test_now(conn, int(arg)))
        elif operation == "write-then-read":
            with db.transaction(conn, immediate=True):
                now1 = clock.now_for_write_transaction(conn)
            now2 = clock.now_for_read_snapshot(conn)
            print(now1, now2)
    except OjpError as exc:
        print(f"OJP_ERROR:{exc.code}:{exc.message}", file=sys.stderr)
        sys.exit(3)
    """
)


def run_observer(db_path: Path, mode: str, operation: str, arg: str = "0") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", OBSERVE_SCRIPT, str(db_path), mode, operation, arg],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


class TestSharedClockAcrossProcesses:
    def test_multiple_processes_observe_same_injected_time(self, test_db: DbHandle) -> None:
        """独立した複数プロセスが同じ注入時刻を観測する。"""
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", OBSERVE_SCRIPT, str(test_db.path), "test", "write-then-read", "0"],
                stdout=subprocess.PIPE,
                text=True,
            )
            for _ in range(4)
        ]
        outputs = [p.communicate(timeout=60)[0].strip() for p in procs]
        for p in procs:
            assert p.returncode == 0

        for output in outputs:
            write_now, read_now = output.split()
            assert int(write_now) == TEST_T0_US
            assert int(read_now) == TEST_T0_US
        assert len({output.split()[0] for output in outputs}) == 1

    def test_time_persists_after_all_processes_stop_and_restart(self, test_db: DbHandle) -> None:
        """全プロセス停止・再起動後も固定時刻が保持される。"""
        advanced = TEST_T0_US + 61_000_000
        result = run_observer(test_db.path, "test", "set", str(advanced))
        assert result.returncode == 0
        assert result.stdout.strip() == "True"
        # subprocess.run の完了 = 上述プロセスは全て停止済み。
        # 独立プロセス（test_db.conn 以外の新接続）から読み直して保持を確認する。
        fresh = db.connect(test_db.path)
        assert (
            fresh.execute(
                "SELECT test_now_utc_us FROM runtime_clock WHERE singleton_id = 1"
            ).fetchone()["test_now_utc_us"]
            == advanced
        )
        fresh.close()

        after = run_observer(test_db.path, "test", "read")
        assert after.returncode == 0
        assert int(after.stdout.strip()) == advanced

        again = run_observer(test_db.path, "test", "write")
        assert again.returncode == 0
        assert int(again.stdout.strip()) == advanced

    def test_realtime_mode_uses_wall_clock(self, realtime_db: DbHandle) -> None:
        before = clock.utc_now_us()
        result = run_observer(realtime_db.path, "realtime", "write")
        assert result.returncode == 0
        observed = int(result.stdout.strip())
        after = clock.utc_now_us()
        assert before <= observed <= after


class TestClockAdvanceRules:
    def test_backward_rejected(self, test_db: DbHandle) -> None:
        result = run_observer(test_db.path, "test", "set", str(TEST_T0_US - 1))
        assert result.returncode != 0
        assert "CLOCK_BACKWARD" in result.stderr
        assert "cannot move backward" in result.stderr

        current = run_observer(test_db.path, "test", "read")
        assert int(current.stdout.strip()) == TEST_T0_US

    def test_same_time_is_noop(self, test_db: DbHandle) -> None:
        result = run_observer(test_db.path, "test", "set", str(TEST_T0_US))
        assert result.returncode == 0
        assert result.stdout.strip() == "False"

    def test_forward_advances(self, test_db: DbHandle) -> None:
        advanced = TEST_T0_US + 1
        result = run_observer(test_db.path, "test", "set", str(advanced))
        assert result.returncode == 0
        assert result.stdout.strip() == "True"
        observed = run_observer(test_db.path, "test", "read")
        assert int(observed.stdout.strip()) == advanced

    def test_set_test_now_rejected_on_realtime_db(self, realtime_db: DbHandle) -> None:
        with pytest.raises(OjpError) as excinfo:
            clock.set_test_now(realtime_db.conn, TEST_T0_US)
        assert excinfo.value.code == ErrorCode.CLOCK_ERROR


class TestModeMismatchRejected:
    def test_startup_test_on_realtime_db_rejected(self, realtime_db: DbHandle) -> None:
        result = run_observer(realtime_db.path, "test", "read")
        assert result.returncode != 0
        assert "MODE_MISMATCH" in result.stderr

    def test_startup_realtime_on_test_db_rejected(self, test_db: DbHandle) -> None:
        result = run_observer(test_db.path, "realtime", "read")
        assert result.returncode != 0
        assert "MODE_MISMATCH" in result.stderr

    def test_initialize_database_enforces_mode(self, test_db: DbHandle) -> None:
        with pytest.raises(OjpError) as excinfo:
            clock.initialize_database(test_db.path, ClockMode.REALTIME)
        assert excinfo.value.code == ErrorCode.MODE_MISMATCH

    def test_mode_is_immutable_in_existing_db(self, test_db: DbHandle) -> None:
        # mode を realtime へ戻す操作はトリガーで拒否される
        with pytest.raises(sqlite3.IntegrityError):
            with db.transaction(test_db.conn, immediate=True):
                test_db.conn.execute(
                    "UPDATE runtime_clock SET mode = 'realtime' WHERE singleton_id = 1"
                )
        # rollback 後も mode は test のまま
        assert clock.read_mode(test_db.conn) == ClockMode.TEST

    def test_fresh_realtime_db_cannot_become_test_via_sql(self, realtime_db: DbHandle) -> None:
        # realtime のまま test_now を保存することも拒否される
        with pytest.raises(sqlite3.IntegrityError):
            with db.transaction(realtime_db.conn, immediate=True):
                realtime_db.conn.execute(
                    "UPDATE runtime_clock SET test_now_utc_us = 1 WHERE singleton_id = 1"
                )
