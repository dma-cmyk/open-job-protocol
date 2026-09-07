"""tick（計画書 第12節・第14節・第15節 Lifecycle）。

`expire_due_leases` → `approve_due_submissions` → `resolve_due_disputes` →
`reserve_refundable_balance` → `process_payments` の順で 1 回分の期限処理を
実行する。`--once` と `--watch` は同じ `tick_once` を呼ぶ（別経路を作らない）。
`watch` は `max_iterations` / `stop` で有限回に止められる（Job 期限を実時間
sleep で待つ実装にしない）。

独立プロセスから `python -m ojp.scheduler --root <project-root> --once --json`
で実行でき、全プロセス停止後も期限は DB に残るため、再起動後の tick が
過期限分を処理する。`--clock-mode`（既定 realtime）で期待する DB の Clock
mode を指定し、DB の mode と違えば Lifecycle を 1 つも実行せず
MODE_MISMATCH で起動を拒否する（計画書 第7節。test DB の利用には明示的な
`--clock-mode test` が必要）。終了コードは第14節どおり: 成功 0、
入力・権限・状態違反 2、一時障害 3。`--json` のとき stdout には単一 JSON
だけを出す（ログを混ぜない）。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Callable

from . import clock, db, domain, ledger, service
from .domain import ErrorCode, OjpError

DEFAULT_ACTOR_ID = "pt-system"


def _resolve_db_path(root: Path) -> Path:
    """プロジェクトルートから SQLite ファイルのパスを解決する。"""
    return root / "data" / "ojp.sqlite3"


def _refundable_root_ids(conn: sqlite3.Connection) -> list[str]:
    """終端 Root で返金可能な available がある Root を DB から導出する。

    reserve_refundable_balance は root_id を要求するため、tick は対象 Root
    をこの導出で決める。対象が無ければ何もしない。
    """
    rows = conn.execute(
        "SELECT j.id AS root_id FROM jobs j"
        " WHERE j.parent_id IS NULL AND j.root_id = j.id"
        " AND j.state IN (?, ?)"
        " AND (SELECT COALESCE(SUM(b.amount_units), 0) FROM budget_accounts b"
        "      WHERE b.root_id = j.id AND b.bucket = ?"
        "      AND (b.source_key IS NULL OR b.source_key != ?)) > 0"
        " ORDER BY j.id",
        (
            domain.JobState.FAILED.value,
            domain.JobState.EXPIRED.value,
            domain.Bucket.AVAILABLE.value,
            ledger.WALLET_LEDGER_SOURCE_KEY,
        ),
    ).fetchall()
    return [str(row["root_id"]) for row in rows]


def tick_once(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    escrow: ledger.EscrowPort | None = None,
) -> dict[str, Any]:
    """1 回分の期限処理（第15節 Lifecycle をこの順序で呼ぶ）。

    戻り値は各 Lifecycle の件数・結果の dict。1 つの Lifecycle が
    OjpError で失敗しても、先に確定した Lifecycle の結果は失われない
    （呼び出し側は error を参照して終了コードを決める）。
    """
    expired: list[Any] = []
    approved: list[Any] = []
    resolved: list[Any] = []
    reserved: list[Any] = []
    payments: list[Any] = []
    error: dict[str, str] | None = None
    try:
        expired = service.expire_due_leases(conn, actor_id=actor_id, escrow=escrow)
    except OjpError as exc:
        error = {"code": exc.code, "message": exc.message}
    try:
        approved = service.approve_due_submissions(
            conn, actor_id=actor_id, escrow=escrow
        )
        resolved = service.resolve_due_disputes(
            conn, actor_id=actor_id, escrow=escrow
        )
    except OjpError as exc:
        if error is None:
            error = {"code": exc.code, "message": exc.message}
    try:
        for root_id in _refundable_root_ids(conn):
            reserved.append(
                service.reserve_refundable_balance(
                    conn, actor_id=actor_id, root_id=root_id
                )
            )
        payments = service.process_payments(conn, escrow=escrow)
    except OjpError as exc:
        if error is None:
            error = {"code": exc.code, "message": exc.message}
    return {
        "expired_leases": [r.data for r in expired],
        "approved_submissions": [r.data for r in approved],
        "resolved_disputes": [r.data for r in resolved],
        "reserved_refunds": [r.data for r in reserved],
        "processed_payments": [r.data for r in payments],
        "counts": {
            "expired_leases": len(expired),
            "approved_submissions": len(approved),
            "resolved_disputes": len(resolved),
            "reserved_refunds": len(reserved),
            "processed_payments": len(payments),
        },
        "error": error,
    }


def watch(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    escrow: ledger.EscrowPort | None = None,
    interval_seconds: float = 1.0,
    max_iterations: int | None = None,
    stop: Callable[[], bool] | None = None,
) -> None:
    """tick_once を一定間隔で繰り返す。max_iterations / stop で有限回に止まる。

    `--watch` と `--once` は同じ tick_once を呼ぶ（別経路を作らない）。
    実時間の sleep はループの待機だけに使い、Job の期限を待つために
    使わない（期限は DB に保存され、tick ごとに DB から判定する）。
    """
    iterations = 0
    while max_iterations is None or iterations < max_iterations:
        if stop is not None and stop():
            return
        tick_once(conn, actor_id=actor_id, escrow=escrow)
        iterations += 1
        if max_iterations is not None and iterations >= max_iterations:
            return
        time.sleep(interval_seconds)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m ojp.scheduler",
        description="OJP tick: 期限処理・固定裁定・送金処理（system 専用）",
    )
    parser.add_argument(
        "--root",
        required=True,
        help="プロジェクトルートのパス（data/ojp.sqlite3 を解決する）",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--once", action="store_true", help="tick を 1 回だけ実行する"
    )
    mode.add_argument(
        "--watch", action="store_true", help="tick を一定間隔で繰り返す"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="--watch の tick 間隔（秒。既定 1.0）",
    )
    parser.add_argument(
        "--actor",
        default=DEFAULT_ACTOR_ID,
        help="system Actor の Participant ID（既定: %(default)s）",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="結果を単一の JSON で stdout へ出力する",
    )
    parser.add_argument(
        "--clock-mode",
        choices=[m.value for m in domain.ClockMode],
        default=domain.ClockMode.REALTIME.value,
        help="起動時に期待する DB の Clock mode（既定: %(default)s）。"
        "起動設定と DB の mode が違えば MODE_MISMATCH で起動を拒否する"
        "（計画書 第7節。test DB の利用には明示的な test 指定が必要）",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point。終了コード: 成功 0 / 入力・権限・状態違反 2 / 一時障害 3。"""
    args = _parse_args(argv)
    root = Path(args.root).expanduser()
    db_path = _resolve_db_path(root)
    conn: sqlite3.Connection | None = None
    try:
        # 既存 DB を開き、起動設定と DB の Clock mode を照合する（計画書
        # 第7節「起動設定と DB の mode が違えば起動を拒否する」）。不一致なら
        # MODE_MISMATCH で起動を拒否し、Lifecycle を 1 つも実行しない
        conn = db.connect(db_path)
        clock.assert_mode(conn, domain.ClockMode(args.clock_mode))
        if args.once:
            result = tick_once(conn, actor_id=args.actor)
        else:
            # --watch は同じ tick_once を使う（main からは無限ループ）
            watch(conn, actor_id=args.actor, interval_seconds=args.interval)
            result = {}
        if args.json:
            # stdout には単一 JSON だけ（余計なログを混ぜない）
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        error = result.get("error") if isinstance(result, dict) else None
        if error is not None:
            code = error.get("code")
            if code in (ErrorCode.DB_BUSY.value,):
                return 3
            return 2
        return 0
    except OjpError as exc:
        if args.json:
            print(
                json.dumps(
                    {"error": {"code": exc.code, "message": exc.message}},
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
        if exc.code in (ErrorCode.DB_BUSY,):
            return 3
        return 2
    except sqlite3.Error as exc:
        if args.json:
            print(
                json.dumps(
                    {"error": {"code": "DB_ERROR", "message": str(exc)}},
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
        return 3
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
