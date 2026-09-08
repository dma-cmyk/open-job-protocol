"""共通応答封筒と終了コード（計画書 第13節・第14節）。

CLI と MCP が同じ応答構造を返し、再試行可能な一時障害と
確定的な違反を統一的に区別する。
"""

from __future__ import annotations

from typing import Any

from .domain import ErrorCode, OjpError

EXIT_OK = 0
EXIT_VIOLATION = 2
EXIT_RETRYABLE = 3

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


def failure(exc: OjpError) -> dict[str, Any]:
    """失敗時の共通応答封筒を構築する。"""
    return {
        "ok": False,
        "error": {
            "code": exc.code,
            "message": exc.message,
            "retryable": is_retryable(exc.code),
            "details": exc.details,
        },
    }


def exit_code(code: str) -> int:
    """エラーコードに対応する CLI 終了コードを返す（一時障害なら 3、それ以外は 2）。"""
    return EXIT_RETRYABLE if is_retryable(code) else EXIT_VIOLATION
