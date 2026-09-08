"""Open Job Protocol (OJP) MCP stdio サーバー（計画書 第13節）。

8 tools（list_jobs, get_job, claim_job, heartbeat, create_child_job, submit,
approve, dispute）を stdio 経由で公開する。

- Actor は起動引数からのみ解決し、tool 引数に actor_id / payee_id を受け付けない。
- 書込 6 件は operation_id を必須とし、冪等な再送を同一結果で返す。
- 読取 2 件は operation_id を受け付けない。
- ドメインエラー（OjpError）は ToolError(json.dumps(response.failure(exc))) として raise し、
  SDK の is_error=True と JSON 封筒の両方を満たす。
- 返金・tick・入金・Root 作成・障害注入・決済再試行・時刻操作は MCP に公開しない。
- stdout にログを一行も流さない。
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator
import sqlite3

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from . import __version__, clock, db, domain, response, scheduler, service
from .cli import parse_timestamp_iso
from .domain import ErrorCode, OjpError


@dataclass(frozen=True)
class ServerConfig:
    """MCP サーバーの起動設定（Actor および DB 接続情報）。"""

    actor_id: str
    db_path: Path


@contextlib.contextmanager
def _open_session(config: ServerConfig) -> Iterator[sqlite3.Connection]:
    """DB 接続を開き、終了時に確実に閉じる。OjpError と sqlite3.Error は ToolError へ変換する。"""
    conn = db.connect(config.db_path)
    try:
        yield conn
    except OjpError as exc:
        raise ToolError(
            json.dumps(response.failure(exc), ensure_ascii=False, sort_keys=True)
        ) from exc
    except sqlite3.Error as exc:
        is_busy = db.is_db_busy(exc)
        payload = response.db_error_payload(exc, busy=is_busy)
        raise ToolError(
            json.dumps(payload, ensure_ascii=False, sort_keys=True)
        ) from exc
    finally:
        conn.close()


def create_server(*, actor_id: str, root: Path) -> MCPServer:
    """指定された Actor と root 設定で MCP サーバーを構築し、8 tools を登録する。"""
    db_path = scheduler.resolve_db_path(root)
    config = ServerConfig(actor_id=actor_id, db_path=db_path)

    server = MCPServer(
        name="ojp",
        description="Open Job Protocol PoC",
        version=__version__,
    )

    # 1. ojp_list_jobs
    @server.tool(
        name="ojp_list_jobs",
        description=(
            "List job cards with keyset pagination. Readable by any participant "
            "to inspect OPEN jobs and their specification summaries."
        ),
    )
    def ojp_list_jobs(
        state: str | None = None,
        parent_id: str | None = None,
        limit: int = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        with _open_session(config) as conn:
            data = service.list_jobs(
                conn,
                actor_id=config.actor_id,
                state=state,
                parent_id=parent_id,
                limit=limit,
                cursor=cursor,
            )
            return response.success(data)

    # 2. ojp_get_job
    @server.tool(
        name="ojp_get_job",
        description=(
            "Get detailed job status, version specifications, lease, budget, children, "
            "and review/payment outcomes. Artifact JSON and verification evidence are "
            "included only if the caller has reading permissions."
        ),
    )
    def ojp_get_job(
        job_id: str,
    ) -> dict[str, Any]:
        with _open_session(config) as conn:
            data = service.get_job(
                conn,
                actor_id=config.actor_id,
                job_id=job_id,
            )
            return response.success(data)

    # 3. ojp_claim_job
    @server.tool(
        name="ojp_claim_job",
        description=(
            "Claim an OPEN job to acquire an exclusive lease. Requires Worker role. "
            "Replay with the same operation_id returns the existing lease."
        ),
    )
    def ojp_claim_job(
        job_id: str,
        expected_version_id: str,
        operation_id: str,
    ) -> dict[str, Any]:
        with _open_session(config) as conn:
            result = service.claim(
                conn,
                actor_id=config.actor_id,
                job_id=job_id,
                expected_version_id=expected_version_id,
                operation_id=operation_id,
            )
            return response.success(
                result.data,
                operation_id=result.operation_id,
                replayed=result.replayed,
            )

    # 4. ojp_heartbeat
    @server.tool(
        name="ojp_heartbeat",
        description=(
            "Extend active lease expiration before deadline. Requires current lease holder Worker."
        ),
    )
    def ojp_heartbeat(
        job_id: str,
        lease_id: str,
        generation: int,
        operation_id: str,
    ) -> dict[str, Any]:
        with _open_session(config) as conn:
            result = service.heartbeat(
                conn,
                actor_id=config.actor_id,
                job_id=job_id,
                lease_id=lease_id,
                generation=generation,
                operation_id=operation_id,
            )
            return response.success(
                result.data,
                operation_id=result.operation_id,
                replayed=result.replayed,
            )

    # 5. ojp_create_child_job
    @server.tool(
        name="ojp_create_child_job",
        description=(
            "Create and publish a child job within permitted budget constraints. "
            "Requires active Parent Worker lease. Budget is a 6-decimal string, "
            "and deadline is an ISO-8601 UTC string."
        ),
    )
    def ojp_create_child_job(
        parent_job_id: str,
        lease_id: str,
        task_key: str,
        budget: str,
        deadline: str,
        operation_id: str,
    ) -> dict[str, Any]:
        with _open_session(config) as conn:
            budget_units = domain.parse_amount_units(budget)
            deadline_us = parse_timestamp_iso(deadline)
            result = service.create_child(
                conn,
                actor_id=config.actor_id,
                parent_job_id=parent_job_id,
                lease_id=lease_id,
                task_key=task_key,
                budget_units=budget_units,
                deadline_us=deadline_us,
                operation_id=operation_id,
            )
            return response.success(
                result.data,
                operation_id=result.operation_id,
                replayed=result.replayed,
            )

    # 6. ojp_submit
    @server.tool(
        name="ojp_submit",
        description=(
            "Submit artifact JSON for review before lease expiration. "
            "Requires active lease holder Worker."
        ),
    )
    def ojp_submit(
        job_id: str,
        lease_id: str,
        version_id: str,
        artifact_json: str,
        operation_id: str,
    ) -> dict[str, Any]:
        with _open_session(config) as conn:
            result = service.submit(
                conn,
                actor_id=config.actor_id,
                job_id=job_id,
                lease_id=lease_id,
                version_id=version_id,
                artifact_json=artifact_json,
                operation_id=operation_id,
            )
            return response.success(
                result.data,
                operation_id=result.operation_id,
                replayed=result.replayed,
            )

    # 7. ojp_approve
    @server.tool(
        name="ojp_approve",
        description=(
            "Approve submitted artifact passing verification and schedule payment. "
            "Requires Job Requester."
        ),
    )
    def ojp_approve(
        job_id: str,
        submission_id: str,
        operation_id: str,
    ) -> dict[str, Any]:
        with _open_session(config) as conn:
            result = service.approve(
                conn,
                actor_id=config.actor_id,
                job_id=job_id,
                submission_id=submission_id,
                operation_id=operation_id,
            )
            return response.success(
                result.data,
                operation_id=result.operation_id,
                replayed=result.replayed,
            )

    # 8. ojp_dispute
    @server.tool(
        name="ojp_dispute",
        description=(
            "Raise a dispute against a submission under fixed conditions within the review window. "
            "Requires Job Requester or Root Requester."
        ),
    )
    def ojp_dispute(
        job_id: str,
        submission_id: str,
        condition_id: str,
        reason_code: str,
        operation_id: str,
        evidence: str | None = None,
    ) -> dict[str, Any]:
        with _open_session(config) as conn:
            result = service.dispute(
                conn,
                actor_id=config.actor_id,
                job_id=job_id,
                submission_id=submission_id,
                condition_id=condition_id,
                reason_code=reason_code,
                evidence=evidence,
                operation_id=operation_id,
            )
            return response.success(
                result.data,
                operation_id=result.operation_id,
                replayed=result.replayed,
            )

    return server


def main(*, actor_id: str, root: Path, clock_mode: str = "realtime") -> int:
    """OJP MCP stdio サーバーのエントリポイント。

    DB の存在と Clock mode を検査し、問題がなければ stdio トランスポートで起動する。
    """
    db_path = scheduler.resolve_db_path(root)
    if not db_path.exists():
        sys.stderr.write(
            f"database file not found: {db_path}. run 'ojp demo init' first\n"
        )
        return response.exit_code(ErrorCode.INVALID_STATE.value)

    try:
        conn = db.connect(db_path)
        try:
            clock.assert_mode(conn, domain.ClockMode(clock_mode))
        finally:
            conn.close()
    except OjpError as exc:
        sys.stderr.write(f"clock mode error: {exc}\n")
        return response.exit_code(exc.code)

    server = create_server(actor_id=actor_id, root=root)
    server.run(transport="stdio")
    return response.EXIT_OK


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="OJP MCP stdio server",
    )
    parser.add_argument(
        "--root",
        required=True,
        help="Project root path (resolves data/ojp.sqlite3)",
    )
    parser.add_argument(
        "--actor",
        required=True,
        help="Trusted local operator Participant ID",
    )
    parser.add_argument(
        "--clock-mode",
        choices=[m.value for m in domain.ClockMode],
        default=domain.ClockMode.REALTIME.value,
        help="Expected DB clock mode (realtime or test)",
    )
    args = parser.parse_args()
    sys.exit(
        main(
            actor_id=args.actor,
            root=Path(args.root),
            clock_mode=args.clock_mode,
        )
    )
