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
from tests.integration.test_db_schema import (
    insert_operation,
    insert_participant,
    insert_root_job,
)


class TestBudgetAccountReferentialIntegrity:
    """002 の参照整合性トリガー（INSERT だけでなく UPDATE も検査し、参照されて
    いる jobs / participants 行の削除を拒否する）の検証。"""

    def _insert_account(self, conn, aid: str, jid: str, amount: int = 100) -> None:
        conn.execute(
            "INSERT INTO budget_accounts (id, root_id, owner_job_id, bucket, amount_units)"
            " VALUES (?, ?, ?, 'available', ?)",
            (aid, jid, jid, amount),
        )

    def _insert_wallet_ledger_account(self, conn, pid: str, aid: str) -> None:
        conn.execute(
            "INSERT INTO mock_wallets (participant_id, asset, balance_units)"
            " VALUES (?, 'mock-USDC', 10)",
            (pid,),
        )
        conn.execute(
            "INSERT INTO budget_accounts"
            " (id, root_id, owner_job_id, bucket, amount_units, source_key)"
            " VALUES (?, ?, ?, 'available', 10, 'wallet-ledger')",
            (aid, pid, pid),
        )
        conn.execute(
            "INSERT INTO wallet_ledger_accounts (account_id, participant_id)"
            " VALUES (?, ?)",
            (aid, pid),
        )

    def test_account_update_cannot_break_job_reference(
        self, realtime_db: DbHandle
    ) -> None:
        """root_id / owner_job_id の UPDATE も INSERT と同じ規則で検査される
        （存在しない jobs への書き換えは拒否）。"""
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        self._insert_account(conn, "a1", "j1")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE budget_accounts SET root_id = 'ghost' WHERE id = 'a1'")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE budget_accounts SET owner_job_id = 'ghost' WHERE id = 'a1'"
            )
        # 正当な jobs への書き換えは許可される
        insert_root_job(conn, "j2")
        conn.execute("UPDATE budget_accounts SET root_id = 'j2' WHERE id = 'a1'")

    def test_source_key_update_cannot_cross_account_kind(
        self, realtime_db: DbHandle
    ) -> None:
        """source_key の書き換えで通常口座と Wallet 台帳口座の区別を壊せない。

        通常口座（jobs 参照）へ wallet-ledger を書き込むと participants 規則の
        検査に落ち、Wallet 台帳口座（participants 参照）から wallet-ledger を
        外すと jobs 規則の検査に落ちる。
        """
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        self._insert_account(conn, "a1", "j1")
        # 通常口座の source_key を wallet-ledger へ: root_id='j1' は participant
        # ではないため拒否される
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE budget_accounts SET source_key = 'wallet-ledger'"
                " WHERE id = 'a1'"
            )
        # Wallet 台帳口座の source_key を外す: root_id=participant は job では
        # ないため拒否される
        insert_participant(conn, "w1")
        self._insert_wallet_ledger_account(conn, "w1", "wa1")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE budget_accounts SET source_key = NULL WHERE id = 'wa1'"
            )

    def test_referenced_job_delete_is_rejected(self, realtime_db: DbHandle) -> None:
        """budget_accounts から参照されている jobs 行の削除は拒否される
        （孤児口座を作らせない）。"""
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        self._insert_account(conn, "a1", "j1")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM jobs WHERE id = 'j1'")
        # 参照が無い Job の削除は拒否されない
        insert_root_job(conn, "j2")
        conn.execute("DELETE FROM jobs WHERE id = 'j2'")

    def test_referenced_participant_delete_is_rejected(
        self, realtime_db: DbHandle
    ) -> None:
        """Wallet 台帳口座・beneficiary から参照されている participants 行の
        削除は拒否される。"""
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        insert_participant(conn, "w1")
        self._insert_wallet_ledger_account(conn, "w1", "wa1")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM participants WHERE id = 'w1'")
        # beneficiary_id 参照も同様
        conn.execute(
            "INSERT INTO budget_accounts (id, root_id, owner_job_id, bucket,"
            " amount_units, beneficiary_id)"
            " VALUES ('b1', 'j1', 'j1', 'child_payout', 10, 'w1')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM participants WHERE id = 'w1'")

    def test_receipt_recreation_guard_aborts_when_not_empty(
        self, realtime_db: DbHandle, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """003 のガード: transfer_receipts が空でなければ migration 全体が
        ROLLBACK する（中途半端な作り直しを残さない）。"""
        conn = realtime_db.conn
        # 003 を未適用に巻き戻し、Receipt 行を 1 件作る
        insert_root_job(conn, "j1")
        insert_operation(conn, "op1")
        insert_participant(conn, "w1")
        self._insert_account(conn, "acc1", "j1")
        conn.execute(
            "INSERT INTO payment_operations (operation_id, business_key, root_id,"
            " job_id, source_account_id, amount_units, payee_id, kind, status)"
            " VALUES ('op1', 'payout:j1', 'j1', 'j1', 'acc1', 10, 'w1', 'payout',"
            " 'PENDING')"
        )
        conn.execute(
            "INSERT INTO transfer_receipts (receipt_id, operation_id, amount_units,"
            " payee_id, asset, source_account_id)"
            " VALUES ('r1', 'op1', 10, 'w1', 'mock-USDC', 'acc1')"
        )
        conn.execute(
            "DELETE FROM schema_migrations WHERE name = '003_receipt_source_account.sql'"
        )
        monkeypatch.setattr(
            db,
            "_migration_names",
            lambda: ["003_receipt_source_account.sql"],
        )
        with pytest.raises(sqlite3.IntegrityError):
            db.migrate(conn)
        # ROLLBACK: ガード用の一時テーブルも残らず、Receipt 行と適用記録は不変
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
                " AND name IN ('transfer_receipts_recreate_guard',"
                "              'transfer_receipts_new')"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM transfer_receipts").fetchone()[0] == 1
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM schema_migrations"
                " WHERE name = '003_receipt_source_account.sql'"
            ).fetchone()[0]
            == 0
        )


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
