"""MCP SDK 2.x の stdio 接続スモークテスト（計画書 第13節・Phase 5）。

実 subprocess の stdio サーバーへ実クライアントで initialize / tools/list を行う。
CLI 経由（ojp mcp）で起動し、8 tools が正常に公開されていることを検証する。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from ojp import __version__, cli

EXPECTED_TOOL_NAMES = [
    "ojp_approve",
    "ojp_claim_job",
    "ojp_create_child_job",
    "ojp_dispute",
    "ojp_get_job",
    "ojp_heartbeat",
    "ojp_list_jobs",
    "ojp_submit",
]


@pytest.mark.anyio
async def test_stdio_initialize_and_tools_list(tmp_path: Path) -> None:
    # DB を先に初期化
    init_code = cli.main(
        ["--root", str(tmp_path), "--clock-mode", "test", "demo", "init"]
    )
    assert init_code == 0

    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "ojp.cli",
            "--root",
            str(tmp_path),
            "--clock-mode",
            "test",
            "mcp",
            "--actor",
            "pt-requester",
        ],
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            assert init.server_info.name == "ojp"
            assert init.server_info.version == __version__

            tools = await session.list_tools()
            tool_names = sorted(t.name for t in tools.tools)
            assert tool_names == EXPECTED_TOOL_NAMES


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
