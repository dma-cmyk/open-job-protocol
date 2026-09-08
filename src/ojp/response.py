"""共通応答封筒と終了コード（計画書 第13節・第14節）。

CLI と MCP が同じ応答構造を返し、再試行可能な一時障害と
確定的な違反を統一的に区別する。
"""

from __future__ import annotations
import sqlite3
from typing import Any

from .domain import ErrorCode, OjpError

EXIT_OK = 0
EXIT_VIOLATION = 2
EXIT_RETRYABLE = 3

DB_ERROR_CODE = "DB_ERROR"   # busy でない確定的な DB エラー。retryable ではない

RETRYABLE_CODES: frozenset[str] = frozenset(
    {
        ErrorCode.DB_BUSY.value,
        ErrorCode.VERIFICATION_UNAVAILABLE.value,
    }
)


def is_retryable(code: str) -> bool:
    """エラーコードが一時障害（再試行可能）か判定する。"""
    return code in RETRYABLE_CODES


def success(
    data: Any,
    *,
    operation_id: str | None = None,
    replayed: bool | None = None,
) -> dict[str, Any]:
    """成功時の共通応答封筒を構築する。"""
    res: dict[str, Any] = {
        "ok": True,
        "data": data,
    }
    if operation_id is not None:
        res["operation_id"] = operation_id
    if replayed is not None:
        res["replayed"] = replayed
    return res


def failure_payload(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """失敗封筒を code/message から組む。retryable は is_retryable(code) で導出する。"""
    return {
        "ok": False,
        "error": {
            "code": code,
            "message": message,
            "retryable": is_retryable(code),
            "details": details,
        },
    }


def failure(exc: OjpError) -> dict[str, Any]:
    """失敗時の共通応答封筒を構築する（failure_payload へ委譲）。"""
    return failure_payload(exc.code, exc.message, details=exc.details)


def db_error_payload(exc: sqlite3.Error, *, busy: bool) -> dict[str, Any]:
    """sqlite3.Error を封筒へ。busy なら DB_BUSY、そうでなければ DB_ERROR。"""
    code = ErrorCode.DB_BUSY.value if busy else DB_ERROR_CODE
    return failure_payload(code, str(exc))


def exit_code(code: str) -> int:
    """エラーコードに対応する CLI 終了コードを返す（一時障害なら 3、それ以外は 2）。"""
    return EXIT_RETRYABLE if is_retryable(code) else EXIT_VIOLATION
