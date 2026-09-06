"""SQLite 接続・明示トランザクション・migration 実行（計画書 第16節）。

- 接続ごとに foreign_keys 有効化、WAL、synchronous=FULL、有限 busy_timeout を適用
- 自動開始と明示的 BEGIN を混在させないため isolation_level=None（autocommit）にし、
  明示 SQL の BEGIN / BEGIN IMMEDIATE / COMMIT / ROLLBACK に統一する
- トランザクション内から別接続の書き込みや外部呼出しを行わない
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from importlib import resources
from pathlib import Path

from .domain import ErrorCode, OjpError

MIGRATIONS_PACKAGE = "ojp"

BUSY_TIMEOUT_MS = 5_000
DB_BUSY_RETRY_SECONDS = 5.0
JOURNAL_MODE_RETRY_SECONDS = 5.0

# 同時初期化テストが注入する同期フックの seam。本番コードは環境変数・
# ファイル I/O・待機を一切持たず、テスト（子プロセスのスクリプト）が
# ojp.db を import してこの属性へ barrier 関数を代入する。通常経路では
# None のままで、migrate() 内の None 判定 1 回以外のコストは発生しない。
_pre_write_lock_hook: Callable[[], None] | None = None


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

    メモリ DB のように WAL へ切り替えられない DB は、非 WAL のまま使わず
    エラーにする。計画書 第16節はローカル DB への WAL 適用を要件としており、
    切替不能な DB を黙って許容すると要件を満たさない接続が紛れ込むため。
    いずれの失敗経路も有限時間で必ず抜ける（レビュー指摘D）。
    """
    deadline = time.monotonic() + JOURNAL_MODE_RETRY_SECONDS
    while True:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if mode.lower() == "wal":
            return
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.OperationalError:
            # ロック競合などの一時障害: 期限まで再試行し、超えたら再送出
            if time.monotonic() > deadline:
                raise
            time.sleep(0.02)
            continue
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if mode.lower() == "wal":
            return
        if mode.lower() == "memory":
            # メモリ DB は WAL をサポートしない。例外も出さず永遠に
            # 切り替わらないため、再試行せず即座に拒否する。
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "WAL journal mode is required but this database cannot use WAL"
                " (journal_mode=memory); use a file-backed database",
            )
        # その他の非 WAL（delete 等）: ロック競合で切替が延期されている
        # 可能性があるため期限まで再試行する。期限を超えても WAL 化
        # できないなら非 WAL のまま進めずエラーにする。
        if time.monotonic() > deadline:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"WAL journal mode is required but could not be enabled"
                f" within {JOURNAL_MODE_RETRY_SECONDS}s (journal_mode={mode})",
            )
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
    clock_row を渡すと、runtime_clock 行がまだ無い場合（＝新規 DB の初期化）
    に限り、未適用の migration 適用と同じ transaction で runtime_clock 行を
    作成する（新規 DB の初期化では 001 の適用・Clock 行作成・schema_migrations
    への適用記録が原子的に確定する）。途中で失敗したらスキーマも Clock 行も
    残らない。既存 DB に後続 migration（将来の 002 等）を適用するときは
    runtime_clock 行は既に存在するため INSERT は走らず、二重作成されない
    （レビュー指摘E）。

    適用状況は BEGIN IMMEDIATE 取得後に書込ロック内で再確認するため、
    複数プロセスが同じ新規 DB を同時に初期化しても後続は CREATE 競合せず、
    適用済みを読むだけで済む。schema_migrations は 001 の前に作成するため、
    最初の migration 実行前に存在確認して作る。
    """
    names = _migration_names()
    newly: list[str] = []
    for name in names:
        sql = _load_migration_sql(name)
        # 同時初期化テストがフックを注入している場合だけ、書込ロック取得
        # （BEGIN IMMEDIATE）の直前で呼び出す。通常経路では None のため
        # 分岐 1 つ以外のコストは発生しない（レビュー指摘C）。
        if _pre_write_lock_hook is not None:
            _pre_write_lock_hook()
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
            # Clock 行の作成は新規 DB 初期化時に 1 回だけ。001 適用直後は
            # 必ず 0 行であり、後続 migration（将来の 002 等）の適用時には
            # 既に 1 行存在するため INSERT は走らない。
            if clock_row is not None and _runtime_clock_missing(conn):
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


def _runtime_clock_missing(conn: sqlite3.Connection) -> bool:
    """runtime_clock 行が未作成か。テーブル自体が無ければ「未作成」とみなす。

    001 適用直後の同一 transaction 内ではテーブルは存在し 0 行、既存 DB での
    後続 migration 適用時には 1 行存在する。テーブルが無い状況（001 より前の
    migration が将来増えた場合等）では INSERT 自体が失敗するため呼ばれない想定。
    """
    table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_clock'"
    ).fetchone()
    if table is None:
        return True
    row = conn.execute(
        "SELECT 1 FROM runtime_clock WHERE singleton_id = 1"
    ).fetchone()
    return row is None


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
