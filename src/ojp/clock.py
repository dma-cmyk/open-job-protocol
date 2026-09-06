"""全プロセスで共有する Clock（計画書 第7節）。

- 書込処理は BEGIN IMMEDIATE 取得後に RuntimeClock を読み、通常は UTC 実時刻、
  test mode では保存済み test_now_utc_us をその transaction の now として
  1回だけ採取する
- 読取処理も同じ DB snapshot の Clock を使い、プロセス単位で固定時刻を
  キャッシュしない
- mode は DB 初期化時に固定し既存 DB では変更不可。起動設定と DB の mode が
  違えば起動を拒否する
- 時刻前進（set_test_now）はテスト harness 専用で、後退は拒否、同じ時刻への
  設定は no-op。時刻更新 transaction に Job 処理を混在させない
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path

from . import db
from .domain import ClockMode, ErrorCode, OjpError, TimestampUs


def utc_now_us() -> TimestampUs:
    now = dt.datetime.now(dt.timezone.utc)
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    delta = now - epoch
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def initialize_database(
    db_path: str | Path,
    mode: ClockMode,
    *,
    test_now_us: TimestampUs | None = None,
) -> sqlite3.Connection:
    """DB を作成（または既存 DB を開き）、Clock mode を検証して接続を返す。

    - 新規 DB: migration を適用し、mode が test なら RuntimeClock を初期化する
    - 既存 DB: mode の変更はできず、起動設定と異なれば MODE_MISMATCH で拒否する
    """
    conn = db.connect(db_path)
    try:
        newly = db.migrate(conn)
        if newly and mode == ClockMode.TEST:
            if test_now_us is None:
                raise OjpError(
                    ErrorCode.CLOCK_ERROR,
                    "test mode requires an initial test_now_us",
                )
            with db.transaction(conn, immediate=True):
                cursor = conn.execute(
                    "UPDATE runtime_clock SET mode = 'test', test_now_utc_us = ? "
                    "WHERE singleton_id = 1 AND mode = 'realtime' AND test_now_utc_us IS NULL",
                    (int(test_now_us),),
                )
                if cursor.rowcount != 1:
                    raise OjpError(
                        ErrorCode.INVALID_STATE, "runtime_clock row is not initializable"
                    )
        # mode='test' に初期化した直後の DB は triggers により test_now_utc_us を
        # NULL へ戻せない。起動検査の前に既存 DB なら mode を照合する。
        assert_mode(conn, mode)
    except BaseException:
        conn.close()
        raise
    return conn


def read_mode(conn: sqlite3.Connection) -> ClockMode:
    row = conn.execute(
        "SELECT mode FROM runtime_clock WHERE singleton_id = 1"
    ).fetchone()
    if row is None:
        raise OjpError(ErrorCode.INVALID_STATE, "runtime_clock row is missing")
    return ClockMode(row["mode"])


def assert_mode(conn: sqlite3.Connection, expected: ClockMode) -> ClockMode:
    """起動設定と DB の mode が違えば拒否する。"""
    actual = read_mode(conn)
    if actual != expected:
        raise OjpError(
            ErrorCode.MODE_MISMATCH,
            f"clock mode mismatch: startup expects {expected.value!r} but DB has {actual.value!r}",
        )
    return actual


def now_for_write_transaction(conn: sqlite3.Connection) -> TimestampUs:
    """BEGIN IMMEDIATE 取得済みの書込 transaction 内で now を1回だけ採取する。

    realtime なら UTC 実時刻、test なら保存済み test_now_utc_us。
    同じ transaction 内の期限比較・記録日時はこの値で統一する。
    """
    row = conn.execute(
        "SELECT mode, test_now_utc_us FROM runtime_clock WHERE singleton_id = 1"
    ).fetchone()
    if row is None:
        raise OjpError(ErrorCode.INVALID_STATE, "runtime_clock row is missing")
    mode = ClockMode(row["mode"])
    if mode == ClockMode.TEST:
        test_now = row["test_now_utc_us"]
        if test_now is None:
            raise OjpError(
                ErrorCode.CLOCK_ERROR, "test mode requires test_now_utc_us to be set"
            )
        return int(test_now)
    return utc_now_us()


def now_for_read_snapshot(conn: sqlite3.Connection) -> TimestampUs:
    """読取 snapshot 内で Clock を読む。プロセス単位のキャッシュは行わない。"""
    return now_for_write_transaction(conn)


def set_test_now(conn: sqlite3.Connection, new_now_us: TimestampUs) -> bool:
    """テスト harness 専用: 書込 transaction 内で単一行を指定 UTC 時刻へ進める。

    後退は拒否、同じ時刻への設定は no-op（False を返す）。進んだ場合 True。
    時刻更新 transaction に Job 処理を混在させない。
    """
    if new_now_us < 0:
        raise OjpError(ErrorCode.CLOCK_BACKWARD, "test time must be non-negative")
    with db.transaction(conn, immediate=True):
        row = conn.execute(
            "SELECT mode, test_now_utc_us FROM runtime_clock WHERE singleton_id = 1"
        ).fetchone()
        if row is None:
            raise OjpError(ErrorCode.INVALID_STATE, "runtime_clock row is missing")
        if ClockMode(row["mode"]) != ClockMode.TEST:
            raise OjpError(
                ErrorCode.CLOCK_ERROR, "time control is only available in test mode"
            )
        current = row["test_now_utc_us"]
        if current is None:
            raise OjpError(ErrorCode.CLOCK_ERROR, "test_now_utc_us is not set")
        if int(new_now_us) < int(current):
            raise OjpError(
                ErrorCode.CLOCK_BACKWARD,
                f"test clock cannot move backward: {new_now_us} < {current}",
            )
        if int(new_now_us) == int(current):
            return False
        conn.execute(
            "UPDATE runtime_clock SET test_now_utc_us = ? WHERE singleton_id = 1",
            (int(new_now_us),),
        )
    return True
