"""OJP MCP stdio サーバー（Phase 1: 接続互換性確認用の最小構成）。

8 tools の本体は Phase 5 で実装する。この段階では initialize / tools/list が
 SDK 2.x (MCPServer) の stdio で応答することだけを提供する。
"""

from __future__ import annotations

from mcp.server.mcpserver import MCPServer

from . import __version__

mcp = MCPServer(
    name="ojp",
    description="Open Job Protocol PoC",
    version=__version__,
)


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
