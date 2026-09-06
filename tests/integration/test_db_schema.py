"""DB 作成・外部キー・UNIQUE・制約の Integration テスト（計画書 第5節・第16節）。

メモリDBではなく実ファイル SQLite を使う。
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from ojp import db
from tests.conftest import DbHandle


def insert_participant(conn: sqlite3.Connection, pid: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO participants (id, label, kind) VALUES (?, ?, ?)",
        (pid, pid, "agent"),
    )


def insert_root_job(conn: sqlite3.Connection, jid: str, requester: str = "p1") -> None:
    insert_participant(conn, requester)
    conn.execute(
        "INSERT INTO jobs (id, root_id, parent_id, requester_id, state, row_version, created_at_us)"
        " VALUES (?, ?, NULL, ?, 'DRAFT', 0, ?)",
        (jid, jid, requester, 1),
    )


def insert_job_version(conn: sqlite3.Connection, jid: str, vid: str) -> None:
    conn.execute(
        "INSERT INTO job_versions (id, job_id, version, title, budget_units, asset,"
        " subcontract_policy, task_catalog, timing_policy, deadline_us)"
        " VALUES (?, ?, 1, 't', 100000000, 'mock-USDC', '{}', '[]', '{}', 9999999999999999)",
        (vid, jid),
    )


def insert_lease(
    conn: sqlite3.Connection,
    lid: str,
    jid: str,
    vid: str,
    worker: str = "w1",
    generation: int = 1,
    closed_reason: str | None = None,
) -> None:
    insert_participant(conn, worker)
    conn.execute(
        "INSERT INTO leases (id, job_id, worker_id, version_id, generation,"
        " claimed_at_us, heartbeat_at_us, expires_at_us, closed_reason)"
        " VALUES (?, ?, ?, ?, ?, 1, 1, 2, ?)",
        (lid, jid, worker, vid, generation, closed_reason),
    )


def insert_operation(conn: sqlite3.Connection, oid: str, actor: str = "p1") -> None:
    conn.execute(
        "INSERT INTO operations (operation_id, actor_id, kind, payload_hash, status, created_at_us)"
        " VALUES (?, ?, 'kind', 'hash', 'SUCCEEDED', 1)",
        (oid, actor),
    )


class TestNewDatabaseCreation:
    def test_migrate_creates_schema_and_clock_row(self, tmp_path: Path) -> None:
        path = tmp_path / "fresh.sqlite3"
        conn = db.connect(path)
        newly = db.migrate(conn)
        assert newly == ["001_initial.sql"]
        assert db.migrate(conn) == []

        tables = {
            r["name"]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for expected in (
            "participants", "jobs", "job_versions", "leases", "submissions",
            "submission_attempts", "budget_accounts", "journal_transactions",
            "journal_entries", "operations", "payment_operations", "mock_wallets",
            "transfer_receipts", "acceptances", "disputes", "events", "runtime_clock",
            "schema_migrations",
        ):
            assert expected in tables

        clock_row = conn.execute(
            "SELECT mode, test_now_utc_us FROM runtime_clock WHERE singleton_id = 1"
        ).fetchone()
        assert clock_row is not None
        assert clock_row["mode"] == "realtime"
        assert clock_row["test_now_utc_us"] is None

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO runtime_clock (singleton_id, mode) VALUES (2, 'realtime')"
            )

    def test_connection_pragmas(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.fresh_conn()
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == db.BUSY_TIMEOUT_MS

    def test_migrate_uses_explicit_transactions(self, tmp_path: Path) -> None:
        conn = db.connect(tmp_path / "tx.sqlite3")
        db.migrate(conn)
        assert not conn.in_transaction


class TestForeignKeys:
    def test_job_requires_existing_requester(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO jobs (id, root_id, requester_id, state, row_version, created_at_us)"
                " VALUES ('j1', 'j1', 'ghost', 'DRAFT', 0, 1)"
            )

    def test_lease_requires_existing_job_and_version(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        insert_job_version(conn, "j1", "v1")
        with pytest.raises(sqlite3.IntegrityError):
            insert_lease(conn, "l1", "missing-job", "v1")

    def test_root_job_invariants(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_participant(conn, "p1")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO jobs (id, root_id, requester_id, state, row_version, created_at_us)"
                " VALUES ('j1', 'other-root', 'p1', 'DRAFT', 0, 1)"
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO jobs (id, root_id, parent_id, requester_id, state, row_version, created_at_us)"
                " VALUES ('j2', 'j1', 'j1', 'p1', 'DRAFT', 0, 1)"
            )

    def test_child_parent_must_be_its_root(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "r1")
        conn.execute(
            "INSERT INTO jobs (id, root_id, parent_id, requester_id, state, row_version, created_at_us)"
            " VALUES ('c1', 'r1', 'r1', 'p1', 'DRAFT', 0, 1)"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO jobs (id, root_id, parent_id, requester_id, state, row_version, created_at_us)"
                " VALUES ('c2', 'r1', 'c1', 'p1', 'DRAFT', 0, 1)"
            )


class TestUniqueConstraints:
    def test_one_active_lease_per_job(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        insert_job_version(conn, "j1", "v1")
        insert_lease(conn, "l1", "j1", "v1")
        with pytest.raises(sqlite3.IntegrityError):
            insert_lease(conn, "l2", "j1", "v1", worker="w2")

        conn.execute("UPDATE leases SET closed_reason = 'expired' WHERE id = 'l1'")
        insert_lease(conn, "l2", "j1", "v1", worker="w2", generation=2)
        with pytest.raises(sqlite3.IntegrityError):
            insert_lease(conn, "l3", "j1", "v1", worker="w3", generation=2)

    def test_job_version_unique_per_job(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        insert_job_version(conn, "j1", "v1")
        with pytest.raises(sqlite3.IntegrityError):
            insert_job_version(conn, "j1", "v2")

    def test_one_valid_submission_per_job(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        insert_job_version(conn, "j1", "v1")
        insert_lease(conn, "l1", "j1", "v1")
        base = (
            "INSERT INTO submissions (id, job_id, lease_id, version_id, artifact_json,"
            " artifact_hash, verification_result, verification_evidence,"
            " submitted_at_us, valid_at_us, review_due_at_us)"
            " VALUES (?, 'j1', 'l1', 'v1', '{}', 'h', 'PASS', 'e', 1, 1, 2)"
        )
        conn.execute(base, ("s1",))
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(base, ("s2",))

    def test_operation_id_unique(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        insert_operation(conn, "op1")
        with pytest.raises(sqlite3.IntegrityError):
            insert_operation(conn, "op1")

    def test_acceptance_one_per_job(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        insert_job_version(conn, "j1", "v1")
        insert_lease(conn, "l1", "j1", "v1")
        conn.execute(
            "INSERT INTO submissions (id, job_id, lease_id, version_id, artifact_json,"
            " artifact_hash, verification_result, verification_evidence,"
            " submitted_at_us, valid_at_us, review_due_at_us)"
            " VALUES ('s1', 'j1', 'l1', 'v1', '{}', 'h', 'PASS', 'e', 1, 1, 2)"
        )
        conn.execute(
            "INSERT INTO acceptances (job_id, submission_id, decision, decided_by, decided_at_us)"
            " VALUES ('j1', 's1', 'APPROVED', 'p1', 1)"
        )
        # Job ごとに最終判定は1つ（jobs PK 経由で拒否）
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO acceptances (job_id, submission_id, decision, decided_by, decided_at_us)"
                " VALUES ('j1', 's1', 'REJECTED', 'p1', 1)"
            )

    def test_dispute_one_per_submission(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        insert_job_version(conn, "j1", "v1")
        insert_lease(conn, "l1", "j1", "v1")
        conn.execute(
            "INSERT INTO submissions (id, job_id, lease_id, version_id, artifact_json,"
            " artifact_hash, verification_result, verification_evidence,"
            " submitted_at_us, valid_at_us, review_due_at_us)"
            " VALUES ('s1', 'j1', 'l1', 'v1', '{}', 'h', 'PASS', 'e', 1, 1, 2)"
        )
        base = (
            "INSERT INTO disputes (id, job_id, submission_id, opened_by, reason_code,"
            " condition_id, opened_at_us, due_at_us, status)"
            " VALUES (?, 'j1', 's1', 'p1', 'RC', 'C1', 1, 2, 'OPEN')"
        )
        conn.execute(base, ("d1",))
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(base, ("d2",))

    def test_receipt_operation_id_unique(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        insert_operation(conn, "op1")
        insert_participant(conn, "w1")
        conn.execute(
            "INSERT INTO budget_accounts (id, root_id, owner_job_id, bucket, amount_units)"
            " VALUES ('acc1', 'j1', 'j1', 'available', 100)"
        )
        conn.execute(
            "INSERT INTO payment_operations (operation_id, business_key, root_id, job_id,"
            " source_account_id, amount_units, payee_id, kind, status)"
            " VALUES ('op1', 'payout:j1', 'j1', 'j1', 'acc1', 10, 'w1', 'payout', 'PENDING')"
        )
        base = (
            "INSERT INTO transfer_receipts (receipt_id, operation_id, amount_units, payee_id, asset)"
            " VALUES (?, 'op1', 10, 'w1', 'mock-USDC')"
        )
        conn.execute(base, ("r1",))
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(base, ("r2",))

    def test_journal_entry_unique_within_operation(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        conn.execute(
            "INSERT INTO journal_transactions (operation_id, reason, created_at_us)"
            " VALUES ('op1', 'test', 1)"
        )
        conn.execute(
            "INSERT INTO budget_accounts (id, root_id, owner_job_id, bucket, amount_units)"
            " VALUES ('a1', 'j1', 'j1', 'available', 100)"
        )
        base = (
            "INSERT INTO journal_entries (operation_id, entry_no, account_id, delta_units)"
            " VALUES ('op1', ?, 'a1', ?)"
        )
        conn.execute(base, (0, -100))
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(base, (0, 100))


class TestNonNegativeAndCheckConstraints:
    def test_budget_account_negative_rejected(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO budget_accounts (id, root_id, owner_job_id, bucket, amount_units)"
                " VALUES ('a1', 'j1', 'j1', 'available', -1)"
            )

    def test_wallet_negative_rejected(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_participant(conn, "w1")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO mock_wallets (participant_id, asset, balance_units)"
                " VALUES ('w1', 'mock-USDC', -5)"
            )

    def test_payment_amount_must_be_positive(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_root_job(conn, "j1")
        insert_operation(conn, "op1")
        insert_participant(conn, "w1")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO payment_operations (operation_id, business_key, root_id, job_id,"
                " source_account_id, amount_units, payee_id, kind, status)"
                " VALUES ('op1', 'bk', 'j1', 'j1', 'acc1', 0, 'w1', 'payout', 'PENDING')"
            )

    def test_job_state_checked(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        insert_participant(conn, "p1")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO jobs (id, root_id, requester_id, state, row_version, created_at_us)"
                " VALUES ('j1', 'j1', 'p1', 'CANCELLED', 0, 1)"
            )

    def test_clock_mode_checked(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE runtime_clock SET mode = 'frozen' WHERE singleton_id = 1")


class TestTransactionBoundary:
    def test_rollback_leaves_no_partial_writes(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        with pytest.raises(RuntimeError):
            with db.transaction(conn, immediate=True) as tx:
                insert_participant(tx, "temp")
                insert_root_job(tx, "j-temp")
                raise RuntimeError("fail")
        assert conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0

    def test_commit_persists_to_new_connection(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        with db.transaction(conn, immediate=True):
            insert_participant(conn, "persisted")
        other = realtime_db.fresh_conn()
        assert other.execute("SELECT COUNT(*) FROM participants").fetchone()[0] == 1

    def test_nested_transaction_rejected(self, realtime_db: DbHandle) -> None:
        conn = realtime_db.conn
        with db.transaction(conn, immediate=True):
            with pytest.raises(Exception):
                with db.transaction(conn, immediate=True):
                    pass

    def test_schema_migrations_recorded_once(self, realtime_db: DbHandle) -> None:
        rows = realtime_db.conn.execute(
            "SELECT name FROM schema_migrations"
        ).fetchall()
        assert [r["name"] for r in rows] == ["001_initial.sql"]
        stamp = realtime_db.conn.execute(
            "SELECT applied_at_us FROM schema_migrations"
        ).fetchone()["applied_at_us"]
        assert 0 < stamp <= int(time.time() * 1_000_000) + 1_000_000
