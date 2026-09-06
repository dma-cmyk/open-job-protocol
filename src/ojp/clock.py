"""全プロセスで共有する Clock（計画書 第7節）。

- 書込処理は BEGIN IMMEDIATE 取得後に RuntimeClock を読み、通常は UTC 実時刻、
  test mode では保存済み test_now_utc_us をその transaction の now として
  1回だけ採取する
- 読取処理も同じ DB snapshot の Clock を使い、プロセス単位で固定時刻を
  キャッシュしない
- mode は runtime_clock 行の作成時に一度だけ確定し、以降は UPDATE で一切変更
  できない（トリガーで強制）。起動設定と DB の mode が違えば起動を拒否する
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


def _validate_startup_args(
    mode: ClockMode, test_now_us: TimestampUs | None
) -> TimestampUs | None:
    """起動設定を DB に触れる前に検証する。失敗しても DB には痕跡が残らない。"""
    if not isinstance(mode, ClockMode):
        raise OjpError(ErrorCode.INVALID_ARGUMENT, f"invalid clock mode: {mode!r}")
    if mode == ClockMode.REALTIME:
        if test_now_us is not None:
            raise OjpError(
                ErrorCode.INVALID_ARGUMENT,
                "test_now_us must not be specified for realtime mode",
            )
        return None
    if test_now_us is None:
        raise OjpError(
            ErrorCode.CLOCK_ERROR, "test mode requires an initial test_now_us"
        )
    if isinstance(test_now_us, bool) or not isinstance(test_now_us, int):
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"test_now_us must be int, got {type(test_now_us).__name__}",
        )
    if test_now_us < 0:
        raise OjpError(ErrorCode.INVALID_ARGUMENT, "test_now_us must be non-negative")
    return test_now_us


def initialize_database(
    db_path: str | Path,
    mode: ClockMode,
    *,
    test_now_us: TimestampUs | None = None,
) -> sqlite3.Connection:
    """DB を作成（または既存 DB を開き）、Clock mode を検証して接続を返す。

    - 入力検証は DB に触れる前に完了する
    - 新規 DB では 001 の適用・runtime_clock 行の作成・schema_migrations への
      適用記録を 1 つの transaction で確定する。途中で失敗したら DB には
      スキーマも Clock 行も残らない（またはどちらも残らない状態から再試行できる）
    - 既存 DB: mode の変更はできず、起動設定と異なれば MODE_MISMATCH で拒否する
    """
    validated_test_now = _validate_startup_args(mode, test_now_us)
    conn = db.connect(db_path)
    try:
        clock_row = (
            (mode.value, validated_test_now)
            if mode == ClockMode.TEST
            else (mode.value, None)
        )
        # 新規 DB: migration 適用と同じ transaction で Clock 行まで作る。
        # 既存 DB: 001 は適用済みのため clock_row は使われない
        db.migrate(conn, clock_row=clock_row)
        _ensure_clock_row(conn, mode, validated_test_now)
        assert_mode(conn, mode)
    except BaseException:
        conn.close()
        raise
    return conn


def _ensure_clock_row(
    conn: sqlite3.Connection, mode: ClockMode, test_now_us: TimestampUs | None
) -> None:
    """runtime_clock 行を、無ければ単一 transaction の INSERT で確定する。

    複数プロセスが同時に初期化する場合は INSERT の PRIMARY KEY が並行を
    直列化し、先行 commit が確定させる。後続は挿入競合を既存行の確認へ
    変換する。
    """
    existing = conn.execute(
        "SELECT mode, test_now_utc_us FROM runtime_clock WHERE singleton_id = 1"
    ).fetchone()
    if existing is not None:
        return
    try:
        with db.transaction(conn, immediate=True):
            # 書込ロック内で再確認する（同時初期化の直列化後）
            row = conn.execute(
                "SELECT mode, test_now_utc_us FROM runtime_clock WHERE singleton_id = 1"
            ).fetchone()
            if row is not None:
                return
            if mode == ClockMode.TEST:
                conn.execute(
                    "INSERT INTO runtime_clock (singleton_id, mode, test_now_utc_us)"
                    " VALUES (1, 'test', ?)",
                    (test_now_us,),
                )
            else:
                conn.execute(
                    "INSERT INTO runtime_clock (singleton_id, mode, test_now_utc_us)"
                    " VALUES (1, 'realtime', NULL)"
                )
    except sqlite3.IntegrityError as exc:
        if "runtime_clock" in str(exc):
            # 別プロセスが同時に初期化した。commit 済み行の mode は assert_mode で検証
            return
        raise


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
