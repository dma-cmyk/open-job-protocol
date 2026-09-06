"""接続時 PRAGMA（WAL 化の有限性）の Integration テスト（レビュー指摘D）。

- PRAGMA journal_mode = WAL が例外を出さず非 WAL を返す DB（メモリ DB 等）
  でも、初期化が有限時間で必ず終わる（無限ループ・ハングしない）
- 実ファイル DB では WAL が有効になることを保証する
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from ojp import db
from ojp.domain import OjpError


class TestWalJournalMode:
    def test_wal_enabled_on_file_db(self, tmp_path: Path) -> None:
        """実ファイル DB では journal_mode が WAL になる。"""
        conn = db.connect(tmp_path / "wal.sqlite3")
        try:
            assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        finally:
            conn.close()

    def test_memory_db_rejected_quickly(self) -> None:
        """メモリ DB のように WAL 化できない DB は無限ループせず有限時間で拒否する。

        修正前の実装（例外が出ない限り期限判定も sleep もしない再試行）では
        この呼び出しは戻らない。テスト自体がハングしないよう subprocess に
        timeout を付けて検証する。
        """
        script = textwrap.dedent(
            """
            import sys, time
            from ojp import db
            from ojp.domain import OjpError

            start = time.monotonic()
            try:
                db._ensure_wal(__import__("sqlite3").connect(":memory:"))
            except OjpError as exc:
                elapsed = time.monotonic() - start
                assert elapsed < 5.0, f"took too long: {elapsed}"
                assert "WAL" in exc.message or "wal" in exc.message
                print("OK")
            else:
                print("NO_ERROR", file=sys.stderr)
                sys.exit(1)
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "OK"

    def test_old_implementation_hangs_on_memory_db(self) -> None:
        """修正前の実装（例外が出ない限り期限判定も sleep もしない再試行）では
        メモリ DB でハングすることを検証する（レビュー指摘D）。

        修正前の _ensure_wal のロジックをテスト内で再現し、timeout コマンドで
        実行する。timeout（終了コード 124）で終われば「修正前はハングする」ことが
        証明できる。テスト自身は timeout コマンドで必ず終わる。
        """
        old_logic = textwrap.dedent(
            """
            import sqlite3, time

            def ensure_wal(conn):
                deadline = time.monotonic() + 5.0
                while True:
                    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
                    if mode.lower() == "wal":
                        return
                    try:
                        conn.execute("PRAGMA journal_mode = WAL")
                    except sqlite3.OperationalError:
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(0.02)

            ensure_wal(sqlite3.connect(":memory:"))
            print("returned (unexpected)")
            """
        )
        result = subprocess.run(
            ["timeout", "5", sys.executable, "-c", old_logic],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        # timeout コマンドによる kill（終了コード 124）を確認する
        assert result.returncode == 124, (
            f"expected timeout (124), got rc={result.returncode} "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )
        assert "returned" not in result.stdout

    def test_non_wal_after_deadline_rejected(self, monkeypatch, tmp_path: Path) -> None:
        """例外を出さずに非 WAL を返し続ける場合でも、期限後は有限時間で抜ける。

        journal_mode の読み取りを常に非 WAL へ差し替えて、デッドライン経過後に
        必ず例外で終わることを確認する（テストがハングしないよう timeout も設定）。
        """
        import sqlite3

        class NeverWalConnection(sqlite3.Connection):
            def execute(self, sql, parameters=(), /):
                if str(sql).startswith("PRAGMA journal_mode"):
                    class _Row:
                        def fetchone(self):
                            return ("delete",)
                    return _Row()
                return super().execute(sql, parameters)

        conn = sqlite3.connect(
            str(tmp_path / "never-wal.sqlite3"),
            isolation_level=None,
            factory=NeverWalConnection,
        )
        try:
            monkeypatch.setattr(db, "JOURNAL_MODE_RETRY_SECONDS", 0.1)

            start = time.monotonic()
            with pytest.raises(OjpError, match="WAL"):
                db._ensure_wal(conn)
            assert time.monotonic() - start < 2.0
        finally:
            conn.close()
