"""migration runner の Integration テスト（レビュー指摘E）。

- runtime_clock 行の作成は新規 DB 初期化時に 1 回だけ
- 将来の migration（002 等）を追加して既存 DB へ適用しても Clock 行は二重に作られない
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ojp import clock, db
from ojp.domain import ClockMode
from tests.conftest import TEST_T0_US, DbHandle


class TestClockRowCreatedOnce:
    def test_clock_row_not_duplicated_by_future_migration(
        self, test_db: DbHandle, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """将来の migration（003 以降）を追加した場合を模し、既存 DB への後続適用で Clock 行の
        INSERT が走らないことを確認する。

        ダミー migration（003_future.sql）をテスト内で一時的に差し込み、
        既存 test DB に対して migrate を実行する。修正前（clock_row INSERT が
        全未適用 migration のループ内にある実装）なら 2 回目の INSERT が走り、
        runtime_clock_no_replace トリガーで失敗する。
        """
        dummy_sql = (
            "-- dummy future migration\n"
            "CREATE TABLE future_dummy (id TEXT PRIMARY KEY);\n"
        )
        monkeypatch.setattr(
            db,
            "_migration_names",
            lambda: ["001_initial.sql", "002_ledger_uniqueness.sql", "003_future.sql"],
        )
        monkeypatch.setattr(
            db,
            "_load_migration_sql",
            lambda name: dummy_sql if name == "003_future.sql" else _real_load(name),
        )

        newly = db.migrate(
            test_db.conn, clock_row=(ClockMode.TEST.value, TEST_T0_US)
        )
        assert newly == ["003_future.sql"]
        assert (
            test_db.conn.execute(
                "SELECT COUNT(*) FROM runtime_clock WHERE singleton_id = 1"
            ).fetchone()[0]
            == 1
        )
        # 既存行は保持される
        row = test_db.conn.execute(
            "SELECT mode, test_now_utc_us FROM runtime_clock WHERE singleton_id = 1"
        ).fetchone()
        assert row["mode"] == "test"
        assert row["test_now_utc_us"] == TEST_T0_US
        # 002 の適用記録とダミーテーブルは残る
        assert (
            test_db.conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE name = '003_future.sql'"
            ).fetchone()[0]
            == 1
        )
        assert (
            test_db.conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
                " AND name='future_dummy'"
            ).fetchone()[0]
            == 1
        )

    def test_fresh_db_clock_row_and_migration_are_atomic(self, tmp_path: Path) -> None:
        """新規 DB では 001 適用・Clock 行作成・schema_migrations 記録が同一
        transaction で確定し、途中失敗ならスキーマも Clock 行も残らない
        （1 ラウンド前の保証の退行防止）。"""
        path = tmp_path / "atomic.sqlite3"
        conn = db.connect(path)
        with pytest.raises(sqlite3.IntegrityError):
            with db.transaction(conn, immediate=True):
                conn.execute(
                    "CREATE TABLE schema_migrations ("
                    " name TEXT PRIMARY KEY,"
                    " applied_at_us INTEGER NOT NULL CHECK (applied_at_us >= 0))"
                )
                for statement in _statements_of_001():
                    conn.execute(statement)
                conn.execute(
                    "INSERT INTO runtime_clock (singleton_id, mode, test_now_utc_us)"
                    " VALUES (1, 'test', ?)",
                    (TEST_T0_US,),
                )
                # 途中失敗を模す（同一 transaction 内での二重 INSERT）
                conn.execute(
                    "INSERT INTO runtime_clock (singleton_id, mode, test_now_utc_us)"
                    " VALUES (1, 'test', ?)",
                    (TEST_T0_US,),
                )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
                " AND name IN ('schema_migrations', 'runtime_clock')"
            ).fetchone()[0]
            == 0
        )
        conn.close()
        # 正しい設定で初期化し直せる
        conn2 = clock.initialize_database(path, ClockMode.TEST, test_now_us=TEST_T0_US)
        try:
            assert clock.read_mode(conn2) == ClockMode.TEST
        finally:
            conn2.close()


def _real_load(name: str) -> str:
    from importlib import resources

    return (
        resources.files("ojp")
        .joinpath("migrations")
        .joinpath(name)
        .read_text(encoding="utf-8")
    )


def _statements_of_001() -> list[str]:
    return db._split_statements(_real_load("001_initial.sql"))
