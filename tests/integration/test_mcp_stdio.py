"""MCP SDK 2.x の stdio 接続スモークテスト（計画書 第2節・第20節 Phase 1）。

実 subprocess の stdio サーバーへ実クライアントで initialize / tools/list を行う。
"""

from __future__ import annotations

import sys

import anyio
import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from ojp import __version__


@pytest.mark.anyio
async def test_stdio_initialize_and_tools_list() -> None:
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "ojp.mcp_server"],
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            assert init.server_info.name == "ojp"
            assert init.server_info.version == __version__

            tools = await session.list_tools()
            assert list(tools.tools) == []


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
