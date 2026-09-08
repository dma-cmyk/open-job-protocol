"""CLI と MCP の等価性・Clock 共有・エラー対応テスト（計画書 第13節・第14節）。

1. CLI と MCP の等価性:
   独立した 2 つの DB で、片方は CLI だけ、もう片方は MCP だけで
   claim -> create_child -> submit -> approve を行い、
   最終的な job.state, budget 全キー, children[].state, children[].budget が一致することを検証。
2. DB 共有 Clock:
   --clock-mode test の DB で、CLI 経由と MCP 経由の get_job の now が同じ固定時刻を返し、
   ojp tick --once を挟んでも now が進まないことを検証。
3. 通常経路に時刻操作の入口がないこと:
   8 tools の全 input_schema に set_now / test_now / advance 等の引数が一切存在しないことを検証。
4. 終了コードと MCP エラーの対応:
   同じ違反操作に対して CLI が exit 2、MCP が is_error=True かつ同一の error.code を返すことを検証。
"""

from __future__ import annotations

import contextlib
import json
import sys
from pathlib import Path
from typing import Any, AsyncIterator

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from ojp import cli, scheduler

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
ROOT_CARD_PATH = FIXTURES_DIR / "poc_root_card.json"


@contextlib.asynccontextmanager
async def _connect_mcp(
    root: Path,
    actor_id: str,
    *,
    clock_mode: str = "test",
) -> AsyncIterator[ClientSession]:
    """CLI 経由で MCP stdio サーバーを実プロセス起動し、ClientSession を提供する。"""
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "ojp.cli",
            "--root",
            str(root),
            "--clock-mode",
            clock_mode,
            "mcp",
            "--actor",
            actor_id,
        ],
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


def _parse_content_payload(result: Any) -> dict[str, Any]:
    """ToolResult の content から JSON 封筒をパースする。"""
    assert result.content, "Result content must not be empty"
    text = result.content[0].text
    if ": " in text and not text.strip().startswith("{"):
        _, json_part = text.split(": ", 1)
    else:
        json_part = text
    return json.loads(json_part)


def _init_and_create_funded_root(root: Path, op_suffix: str = "1", capsys: Any | None = None) -> str:
    """demo init および Root の作成・入金を CLI 経由で行う。"""
    assert (
        cli.main(["--root", str(root), "--clock-mode", "test", "demo", "init"])
        == 0
    )
    code_create = cli.main(
        [
            "--root",
            str(root),
            "--actor",
            "pt-requester",
            "--clock-mode",
            "test",
            "--operation-id",
            f"create:root-{op_suffix}",
            "job",
            "create",
            "--card",
            str(ROOT_CARD_PATH),
        ]
    )
    assert code_create == 0
    job_id = f"job:create:root-{op_suffix}"

    code_fund = cli.main(
        [
            "--root",
            str(root),
            "--actor",
            "pt-requester",
            "--clock-mode",
            "test",
            "--operation-id",
            f"fund:root-{op_suffix}",
            "job",
            "fund",
            job_id,
            "--amount",
            "100.000000",
        ]
    )
    assert code_fund == 0
    if capsys is not None:
        capsys.readouterr()  # 後続のキャプチャに混入しないようバッファをクリア
    return job_id


def _run_cli(capsys: Any, argv: list[str]) -> tuple[int, dict[str, Any] | None, str]:
    """CLI コマンドを実行し、単一の JSON 応答を取得する。"""
    capsys.readouterr()  # 事前出力をクリア
    code = cli.main(argv)
    captured = capsys.readouterr()
    payload = None
    out_str = captured.out.strip()
    if out_str:
        try:
            payload = json.loads(out_str)
        except json.JSONDecodeError:
            # 複数ブロックがある場合は末尾の JSON を抽出
            idx = out_str.rfind('{\n  "ok":')
            if idx != -1:
                payload = json.loads(out_str[idx:])
    return code, payload, captured.err


# ---------------------------------------------------------------------------
# 1. CLI と MCP の等価性（Job 状態と金額）
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cli_and_mcp_parity(tmp_path: Path, capsys: Any) -> None:
    """同じ初期状態から始めた2つの独立した DB で、片方は CLI だけ、

    もう片方は MCP だけで一連のフローを実行し、最終状態の一致を検証する。
    """
    root_cli = tmp_path / "cli_run"
    root_mcp = tmp_path / "mcp_run"

    # --- 1. CLI 経路の実行 ---
    root_cli_job_id = _init_and_create_funded_root(root_cli, "cli", capsys=capsys)

    # (a) CLI: get version_id
    code_get = cli.main(
        ["--root", str(root_cli), "--clock-mode", "test", "--json", "job", "get", root_cli_job_id]
    )
    assert code_get == 0
    cli_root_info = json.loads(capsys.readouterr().out.strip())["data"]
    cli_root_version = cli_root_info["version"]["version_id"]

    # (b) CLI: Worker A claims Root
    code_claim = cli.main(
        [
            "--root",
            str(root_cli),
            "--actor",
            "pt-agent-a",
            "--clock-mode",
            "test",
            "--operation-id",
            "claim:root-cli",
            "--json",
            "job",
            "claim",
            root_cli_job_id,
            "--version",
            cli_root_version,
        ]
    )
    assert code_claim == 0
    claim_out = json.loads(capsys.readouterr().out.strip())["data"]
    cli_lease_id = claim_out["lease_id"]

    # (c) CLI: Worker A creates Child
    code_child = cli.main(
        [
            "--root",
            str(root_cli),
            "--actor",
            "pt-agent-a",
            "--clock-mode",
            "test",
            "--operation-id",
            "create:child-cli",
            "--json",
            "child",
            "create",
            root_cli_job_id,
            "--lease",
            cli_lease_id,
            "--task",
            "part-1",
            "--budget",
            "10.000000",
            "--deadline",
            "2027-01-15T12:00:00Z",
        ]
    )
    assert code_child == 0
    child_out = json.loads(capsys.readouterr().out.strip())["data"]
    cli_child_id = child_out["child_id"]
    cli_child_version = child_out["version_id"]

    # (d) CLI: Worker B claims Child
    code_c_claim = cli.main(
        [
            "--root",
            str(root_cli),
            "--actor",
            "pt-agent-b",
            "--clock-mode",
            "test",
            "--operation-id",
            "claim:child-cli",
            "--json",
            "job",
            "claim",
            cli_child_id,
            "--version",
            cli_child_version,
        ]
    )
    assert code_c_claim == 0
    c_claim_out = json.loads(capsys.readouterr().out.strip())["data"]
    cli_c_lease_id = c_claim_out["lease_id"]

    # (e) CLI: Worker B submits artifact
    artifact_path = root_cli / "artifact.json"
    artifact_path.write_text(json.dumps({"sum": 6}), encoding="utf-8")
    code_submit = cli.main(
        [
            "--root",
            str(root_cli),
            "--actor",
            "pt-agent-b",
            "--clock-mode",
            "test",
            "--operation-id",
            "submit:child-cli",
            "--json",
            "job",
            "submit",
            cli_child_id,
            "--lease",
            cli_c_lease_id,
            "--version",
            cli_child_version,
            "--artifact",
            str(artifact_path),
        ]
    )
    assert code_submit == 0
    submit_out = json.loads(capsys.readouterr().out.strip())["data"]
    cli_submission_id = submit_out["submission_id"]

    # (f) CLI: Worker A approves Child
    code_app = cli.main(
        [
            "--root",
            str(root_cli),
            "--actor",
            "pt-agent-a",
            "--clock-mode",
            "test",
            "--operation-id",
            "approve:child-cli",
            "--json",
            "job",
            "approve",
            cli_child_id,
            "--submission",
            cli_submission_id,
        ]
    )
    assert code_app == 0
    capsys.readouterr()  # drain stdout

    # (g) CLI: Final Job Get
    code_final = cli.main(
        [
            "--root",
            str(root_cli),
            "--actor",
            "pt-requester",
            "--clock-mode",
            "test",
            "--json",
            "job",
            "get",
            root_cli_job_id,
        ]
    )
    assert code_final == 0
    final_cli_data = json.loads(capsys.readouterr().out.strip())["data"]

    # --- 2. MCP 経路の実行 ---
    root_mcp_job_id = _init_and_create_funded_root(root_mcp, "mcp")

    # (a) MCP: get version_id
    async with _connect_mcp(root_mcp, "pt-agent-a") as session_a:
        res_get = await session_a.call_tool("ojp_get_job", arguments={"job_id": root_mcp_job_id})
        assert not res_get.is_error
        mcp_root_version = _parse_content_payload(res_get)["data"]["version"]["version_id"]

        # (b) MCP: Worker A claims Root
        res_claim = await session_a.call_tool(
            "ojp_claim_job",
            arguments={
                "job_id": root_mcp_job_id,
                "expected_version_id": mcp_root_version,
                "operation_id": "claim:root-mcp",
            },
        )
        assert not res_claim.is_error
        mcp_lease_id = _parse_content_payload(res_claim)["data"]["lease_id"]

        # (c) MCP: Worker A creates Child
        res_child = await session_a.call_tool(
            "ojp_create_child_job",
            arguments={
                "parent_job_id": root_mcp_job_id,
                "lease_id": mcp_lease_id,
                "task_key": "part-1",
                "budget": "10.000000",
                "deadline": "2027-01-15T12:00:00Z",
                "operation_id": "create:child-mcp",
            },
        )
        assert not res_child.is_error
        mcp_child_out = _parse_content_payload(res_child)["data"]
        mcp_child_id = mcp_child_out["child_id"]
        mcp_child_version = mcp_child_out["version_id"]

    # (d) MCP: Worker B claims Child and submits artifact
    async with _connect_mcp(root_mcp, "pt-agent-b") as session_b:
        res_c_claim = await session_b.call_tool(
            "ojp_claim_job",
            arguments={
                "job_id": mcp_child_id,
                "expected_version_id": mcp_child_version,
                "operation_id": "claim:child-mcp",
            },
        )
        assert not res_c_claim.is_error
        mcp_c_lease_id = _parse_content_payload(res_c_claim)["data"]["lease_id"]

        res_submit = await session_b.call_tool(
            "ojp_submit",
            arguments={
                "job_id": mcp_child_id,
                "lease_id": mcp_c_lease_id,
                "version_id": mcp_child_version,
                "artifact_json": json.dumps({"sum": 6}),
                "operation_id": "submit:child-mcp",
            },
        )
        assert not res_submit.is_error
        mcp_submission_id = _parse_content_payload(res_submit)["data"]["submission_id"]

    # (e) MCP: Worker A approves Child
    async with _connect_mcp(root_mcp, "pt-agent-a") as session_a2:
        res_app = await session_a2.call_tool(
            "ojp_approve",
            arguments={
                "job_id": mcp_child_id,
                "submission_id": mcp_submission_id,
                "operation_id": "approve:child-mcp",
            },
        )
        assert not res_app.is_error

    # (f) MCP: Final Job Get
    async with _connect_mcp(root_mcp, "pt-requester") as session_req:
        res_final = await session_req.call_tool("ojp_get_job", arguments={"job_id": root_mcp_job_id})
        assert not res_final.is_error
        final_mcp_data = _parse_content_payload(res_final)["data"]

    # --- 3. Parity Comparison ---
    # 除外対象:
    # - ID群 (job_id, root_id, version_id, lease_id, submission_id, child_id)
    # - タイムスタンプ群 (created_at, deadline, expires_at, now)
    # 検証対象:
    # - job.state
    # - budget 全キー (deposit, escrow, available, locked, locked_breakdown, paid, refunded)
    # - children の件数、各 child の state, budget, task_key
    assert final_cli_data["job"]["state"] == final_mcp_data["job"]["state"]
    assert final_cli_data["budget"] == final_mcp_data["budget"]

    assert len(final_cli_data["children"]) == len(final_mcp_data["children"])
    for c_cli, c_mcp in zip(final_cli_data["children"], final_mcp_data["children"]):
        assert c_cli["state"] == c_mcp["state"]
        assert c_cli["budget"] == c_mcp["budget"]
        assert c_cli["task_key"] == c_mcp["task_key"]


# ---------------------------------------------------------------------------
# 2. CLI・MCP・tick が DB 共有 Clock を使うこと
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_shared_db_clock_across_cli_mcp_tick(tmp_path: Path, capsys: Any) -> None:
    root_id = _init_and_create_funded_root(tmp_path, "clock", capsys=capsys)

    # CLI 経由で now 取得
    code_get = cli.main(
        ["--root", str(tmp_path), "--clock-mode", "test", "--json", "job", "get", root_id]
    )
    assert code_get == 0
    cli_now = json.loads(capsys.readouterr().out.strip())["data"]["now"]

    # MCP 経由で now 取得
    async with _connect_mcp(tmp_path, "pt-requester") as session:
        res = await session.call_tool("ojp_get_job", arguments={"job_id": root_id})
        assert not res.is_error
        mcp_now = _parse_content_payload(res)["data"]["now"]

    # 同一の固定時刻（2027-01-15T08:00:00Z）であること
    assert cli_now == mcp_now
    assert cli_now == "2027-01-15T08:00:00.000000Z"

    # tick --once を実行
    code_tick = cli.main(
        ["--root", str(tmp_path), "--clock-mode", "test", "--json", "tick", "--once"]
    )
    assert code_tick == 0
    capsys.readouterr()

    # tick 実行後も test mode の now が進んでいないこと
    async with _connect_mcp(tmp_path, "pt-requester") as session2:
        res2 = await session2.call_tool("ojp_get_job", arguments={"job_id": root_id})
        assert not res2.is_error
        mcp_now_after_tick = _parse_content_payload(res2)["data"]["now"]

    assert mcp_now_after_tick == cli_now


# ---------------------------------------------------------------------------
# 3. 通常経路に時刻操作の入口がないこと
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_no_clock_manipulation_in_public_surfaces(tmp_path: Path) -> None:
    _init_and_create_funded_root(tmp_path, "noclock")

    async with _connect_mcp(tmp_path, "pt-requester") as session:
        tools = await session.list_tools()

        disallowed_param_substrings = ["now", "time", "clock", "advance", "set_now", "test_now"]

        for tool in tools.tools:
            schema = getattr(tool, "input_schema", None) or getattr(tool, "inputSchema", {})
            props = schema.get("properties", {})
            for param_name in props.keys():
                # deadline は業務上の締切引数なので許容
                if param_name == "deadline":
                    continue
                for substring in disallowed_param_substrings:
                    assert substring not in param_name.lower(), (
                        f"Tool {tool.name!r} exposes disallowed time manipulation parameter {param_name!r}"
                    )


# ---------------------------------------------------------------------------
# 4. 終了コードと MCP エラーの対応
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_exit_code_and_mcp_error_correspondence(tmp_path: Path, capsys: Any) -> None:
    """同じ違反操作（権限のない approve）で CLI が exit 2、MCP が同一の error.code を返すことを検証。"""
    root_id = _init_and_create_funded_root(tmp_path, "errcorr")

    # CLI で無権限の approve 実行 (Actor: pt-agent-a)
    code_cli = cli.main(
        [
            "--root",
            str(tmp_path),
            "--actor",
            "pt-agent-a",
            "--clock-mode",
            "test",
            "--operation-id",
            "approve:err-cli",
            "job",
            "approve",
            root_id,
            "--submission",
            "sub-fake",
        ]
    )
    # exit 2 (FORBIDDEN / INVALID_TARGET / INVALID_STATE)
    assert code_cli == 2
    captured = capsys.readouterr()
    cli_err_output = captured.err

    # MCP で同一の違反操作を実行
    async with _connect_mcp(tmp_path, "pt-agent-a") as session:
        res = await session.call_tool(
            "ojp_approve",
            arguments={
                "job_id": root_id,
                "submission_id": "sub-fake",
                "operation_id": "approve:err-mcp",
            },
        )
        assert res.is_error is True
        payload = _parse_content_payload(res)
        assert payload["ok"] is False
        mcp_error_code = payload["error"]["code"]

    # CLI のエラー出力に MCP と同じエラーコードが含まれていること
    assert mcp_error_code in cli_err_output


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
