"""SQLite 接続・明示トランザクション・migration 実行（計画書 第16節）。

- 接続ごとに foreign_keys 有効化、WAL、synchronous=FULL、有限 busy_timeout を適用
- 自動開始と明示的 BEGIN を混在させないため isolation_level=None（autocommit）にし、
  明示 SQL の BEGIN / BEGIN IMMEDIATE / COMMIT / ROLLBACK に統一する
- トランザクション内から別接続の書き込みや外部呼出しを行わない
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import resources
from pathlib import Path

from .domain import ErrorCode, OjpError

MIGRATIONS_PACKAGE = "ojp"

BUSY_TIMEOUT_MS = 5_000
DB_BUSY_RETRY_SECONDS = 5.0
JOURNAL_MODE_RETRY_SECONDS = 5.0


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    # busy_timeout を最初に設定する: journal_mode の切替は書込ロックを
    # 必要とし、busy handler が効かない経路で即座に database is locked に
    # なることがあるため、以降の PRAGMA と初期化はこの待機の内側で行う
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_wal(conn)
    conn.execute("PRAGMA synchronous = FULL")


def _ensure_wal(conn: sqlite3.Connection) -> None:
    """journal_mode を WAL にする。

    既に WAL なら no-op。他プロセスが初期化 transaction を保持している間は
    busy handler が効かず database is locked になり得るため、有限時間の
    再試行で待つ（先行プロセスの初期化が終われば WAL へ切り替わる）。
    """
    deadline = time.monotonic() + JOURNAL_MODE_RETRY_SECONDS
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


def connect(db_path: str | Path) -> sqlite3.Connection:
    """明示的トランザクション専用の接続を開く。"""
    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=BUSY_TIMEOUT_MS / 1000)
    _apply_pragmas(conn)
    conn.row_factory = sqlite3.Row
    return conn


def migrate(
    conn: sqlite3.Connection,
    *,
    clock_row: tuple[str, int | None] | None = None,
) -> list[str]:
    """番号付き SQL migration を未適用分だけ適用する。

    未適用の migration があるときは各々独立した transaction で実行する。
    clock_row を渡すと、未適用の migration 適用と同じ transaction で
    runtime_clock 行を作成する（新規 DB の初期化では 001 の適用・Clock 行作成・
    schema_migrations への適用記録が原子的に確定する）。途中で失敗したら
    スキーマも Clock 行も残らない。

    適用状況は BEGIN IMMEDIATE 取得後に書込ロック内で再確認するため、
    複数プロセスが同じ新規 DB を同時に初期化しても後続は CREATE 競合せず、
    適用済みを読むだけで済む。schema_migrations は 001 の前に作成するため、
    最初の migration 実行前に存在確認して作る。
    """
    names = _migration_names()
    newly: list[str] = []
    for name in names:
        sql = _load_migration_sql(name)
        with transaction(conn, immediate=True):
            if not _migrations_table_exists(conn):
                conn.execute(
                    "CREATE TABLE schema_migrations ("
                    " name TEXT PRIMARY KEY,"
                    " applied_at_us INTEGER NOT NULL CHECK (applied_at_us >= 0))"
                )
            # 書込ロック取得後に適用状況を再確認する（同時初期化の直列化）
            already = conn.execute(
                "SELECT 1 FROM schema_migrations WHERE name = ?", (name,)
            ).fetchone()
            if already is not None:
                continue
            for statement in _split_statements(sql):
                conn.execute(statement)
            if clock_row is not None:
                mode, test_now_us = clock_row
                conn.execute(
                    "INSERT INTO runtime_clock (singleton_id, mode, test_now_utc_us)"
                    " VALUES (1, ?, ?)",
                    (mode, test_now_us),
                )
            conn.execute(
                "INSERT INTO schema_migrations (name, applied_at_us) VALUES (?, ?)",
                (name, int(time.time() * 1_000_000)),
            )
        newly.append(name)
    return newly


def _migrations_table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
    ).fetchone()
    return row is not None


def _migration_names() -> list[str]:
    migrations_dir = resources.files(MIGRATIONS_PACKAGE).joinpath("migrations")
    return sorted(
        child.name
        for child in migrations_dir.iterdir()
        if child.name.endswith(".sql") and child.is_file()
    )


def _load_migration_sql(name: str) -> str:
    res = resources.files(MIGRATIONS_PACKAGE).joinpath("migrations").joinpath(name)
    return res.read_text(encoding="utf-8")


_TRANSACTION_KEYWORDS = ("BEGIN", "COMMIT", "ROLLBACK", "END TRANSACTION")


def _split_statements(sql: str) -> list[str]:
    """SQL スクリプトを個別ステートメントへ分割する。

    トランザクション文は runner が所有するため除外する。executescript は
    暗黙の COMMIT を発行するため使わない。CREATE TRIGGER の BEGIN..END; 内の
    セミコロンでは分割しない。
    """
    statements: list[str] = []
    buffer: list[str] = []
    in_trigger = False
    for line in sql.splitlines(keepends=True):
        stripped = line.strip()
        upper = stripped.upper()
        if not in_trigger and any(
            upper == kw or upper.startswith(kw + " ") for kw in _TRANSACTION_KEYWORDS
        ):
            continue
        buffer.append(line)
        if upper.startswith("CREATE TRIGGER"):
            in_trigger = True
        elif in_trigger:
            if upper == "END;" or upper.startswith("END;"):
                statement = _strip_comments("".join(buffer))
                if statement:
                    statements.append(statement)
                buffer = []
                in_trigger = False
            continue
        candidate = "".join(buffer)
        if sqlite3.complete_statement(candidate):
            no_comments = _strip_comments(candidate)
            if no_comments and no_comments != "PRAGMA foreign_keys = ON;":
                statements.append(no_comments)
            buffer = []
    tail = _strip_comments("".join(buffer))
    if tail and tail != "PRAGMA foreign_keys = ON;":
        statements.append(tail)
    return statements


def _strip_comments(sql: str) -> str:
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return "\n".join(lines).strip()


@contextmanager
def transaction(conn: sqlite3.Connection, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
    """明示 SQL で BEGIN / COMMIT / ROLLBACK を制御するトランザクション。

    DB_BUSY は SQLite の busy_timeout と例外送出で呼び出し側へ伝える。
    """
    if conn.in_transaction:
        raise OjpError(ErrorCode.INVALID_STATE, "nested transaction is not allowed")
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


def is_db_busy(exc: sqlite3.Error) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc).lower()
