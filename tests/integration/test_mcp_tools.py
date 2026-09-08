"""MCP SDK 2.x Tools 統合テスト（計画書 第13節・第14節・第15節・第5節）。

実 subprocess の stdio サーバーへ実クライアントで接続し、8 tools の契約を検証する:
1. tools/list が第13節の8件と完全一致
2. 返金・任意 fail/cancel tool が存在しない
3. 8 tools すべてを一つのセッション等で実際に呼び出せる
4. Actor 引数（actor_id, payee_id）を受け付けず、注入された actor_id が実効 Actor にならないこと
5. operation_id が書込6件で必須、読取2件に存在しない
6. 同一 Actor・同一 payload の再送が同一結果（replayed=True）を返す
7. ドメインエラーが SDK の tool エラー（is_error=True）かつ {"ok": false, "error": {...}} として識別できる
8. 金額が6桁小数形式（^[0-9]+\\.[0-9]{6}$）である
9. stderr に出力があっても stdio の JSON-RPC が壊れない
10. 権限付き取得（artifact_readable）が MCP 経由でも効く
11. DB 未存在で起動拒否され DB が新規作成されない
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, AsyncIterator

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from ojp import cli, db, domain, scheduler, service, verification

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
ROOT_CARD_PATH = FIXTURES_DIR / "poc_root_card.json"

EXPECTED_EIGHT_TOOLS = [
    "ojp_approve",
    "ojp_claim_job",
    "ojp_create_child_job",
    "ojp_dispute",
    "ojp_get_job",
    "ojp_heartbeat",
    "ojp_list_jobs",
    "ojp_submit",
]


@contextlib.asynccontextmanager
async def _connect_mcp(
    root: Path,
    actor_id: str,
    *,
    errlog: Any = None,
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
    kwargs: dict[str, Any] = {}
    if errlog is not None:
        kwargs["errlog"] = errlog

    async with stdio_client(params, **kwargs) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


def _parse_content_payload(result: Any) -> dict[str, Any]:
    """ToolResult の content から JSON 封筒（{"ok": ...}）をパースする。"""
    assert result.content, "Result content must not be empty"
    text = result.content[0].text
    # SDK 2.1.1 の ToolError ラッパー 'Error executing tool <name>: <payload>' を処理
    if ": " in text and not text.strip().startswith("{"):
        _, json_part = text.split(": ", 1)
    else:
        json_part = text
    return json.loads(json_part)


def _init_db(root: Path) -> None:
    """テスト用 DB を初期化する。"""
    code = cli.main(["--root", str(root), "--clock-mode", "test", "demo", "init"])
    assert code == 0


def _create_and_fund_root(root: Path, op_suffix: str = "1") -> str:
    """Root Job を作成・入金して OPEN 状態にする。"""
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
    return job_id


# ---------------------------------------------------------------------------
# 1. tools/list が第13節の8件と完全一致
# 2. 返金・任意 fail/cancel tool が存在しない
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_tools_list_exact_match(tmp_path: Path) -> None:
    _init_db(tmp_path)
    async with _connect_mcp(tmp_path, "pt-requester") as session:
        tools = await session.list_tools()
        names = sorted(t.name for t in tools.tools)
        # 1. 8件完全一致
        assert names == EXPECTED_EIGHT_TOOLS

        # 2. 返金・任意 fail/cancel tool が存在しない
        disallowed_substrings = [
            "refund",
            "fail",
            "cancel",
            "tick",
            "fund",
            "retry",
            "set_now",
            "failpoint",
        ]
        for name in names:
            for dis in disallowed_substrings:
                assert dis not in name, f"Tool {name!r} contains disallowed substring {dis!r}"


# ---------------------------------------------------------------------------
# 4. Actor 引数を受け付けず、注入された actor_id が実効 Actor にならないこと
# 5. operation_id が書込6件で必須、読取2件に存在しない
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_schema_constraints_and_actor_rejection(tmp_path: Path) -> None:
    _init_db(tmp_path)
    root_id = _create_and_fund_root(tmp_path)

    async with _connect_mcp(tmp_path, "pt-agent-a") as session:
        tools = await session.list_tools()
        tool_map = {t.name: t for t in tools.tools}

        write_tools = [
            "ojp_claim_job",
            "ojp_heartbeat",
            "ojp_create_child_job",
            "ojp_submit",
            "ojp_approve",
            "ojp_dispute",
        ]
        read_tools = ["ojp_list_jobs", "ojp_get_job"]

        for name, tool in tool_map.items():
            schema = tool.input_schema
            props = schema.get("properties", {})
            req = schema.get("required", [])

            # 4. actor_id と payee_id が properties に存在しない
            assert "actor_id" not in props, f"{name} must not have actor_id in properties"
            assert "payee_id" not in props, f"{name} must not have payee_id in properties"

            # 5. operation_id の必須性
            if name in write_tools:
                assert "operation_id" in req, f"{name} must require operation_id"
            if name in read_tools:
                assert "operation_id" not in props, f"{name} must not accept operation_id"

        # バージョン取得
        res_get = await session.call_tool("ojp_get_job", arguments={"job_id": root_id})
        assert not res_get.is_error
        version_id = _parse_content_payload(res_get)["data"]["version"]["version_id"]

        # 起動 Actor (pt-agent-a) と異なる actor_id="pt-agent-b" を余分に渡して claim_job を呼ぶ
        res_claim = await session.call_tool(
            "ojp_claim_job",
            arguments={
                "job_id": root_id,
                "expected_version_id": version_id,
                "operation_id": "claim:actor-inject-test",
                "actor_id": "pt-agent-b",
            },
        )
        # 注入された actor_id（pt-agent-b）が実効 Actor にならないことを検証する。
        # 現行 SDK は schema 外の余分引数を無視して起動 Actor (pt-agent-a) で実行する。
        assert res_claim.is_error is False
        payload = _parse_content_payload(res_claim)
        assert payload["ok"] is True
        # 当該 Job を取得し、実効 Worker が起動 Actor（pt-agent-a）であり注入した pt-agent-b ではないことを検証
        res_after = await session.call_tool("ojp_get_job", arguments={"job_id": root_id})
        assert not res_after.is_error
        lease = _parse_content_payload(res_after)["data"]["lease"]
        assert lease is not None
        assert lease["worker_id"] == "pt-agent-a"
        assert lease["worker_id"] != "pt-agent-b"

# ---------------------------------------------------------------------------
# 3. 8 tools すべてを実際に呼べる（一つのセッションで7件順次 + dispute実呼）
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_all_eight_tools_in_single_session(tmp_path: Path) -> None:
    _init_db(tmp_path)

    # 前準備（CLI / service 経由）
    # Root-A: Child-1 (approve用) と Child-2 (dispute用) を持つ
    root_a_id = _create_and_fund_root(tmp_path, "A")
    # Root-B: Claim / Heartbeat / Create Child テスト用
    root_b_id = _create_and_fund_root(tmp_path, "B")
    # Root-C: Worker A が直接成果物を提出する用（Child を作らない）
    root_c_id = _create_and_fund_root(tmp_path, "C")

    db_path = scheduler.resolve_db_path(tmp_path)
    conn = db.connect(db_path)
    try:
        # Root-A を Worker A が Claim して Child-1, Child-2 を作成
        res_claim_a = service.claim(
            conn,
            actor_id="pt-agent-a",
            job_id=root_a_id,
            expected_version_id=service.get_job(conn, actor_id="pt-agent-a", job_id=root_a_id)[
                "version"
            ]["version_id"],
            operation_id="claim:root-a",
        )
        lease_a_id = res_claim_a.data["lease_id"]

        # Child-1 (part-1)
        res_c1 = service.create_child(
            conn,
            actor_id="pt-agent-a",
            parent_job_id=root_a_id,
            lease_id=lease_a_id,
            task_key="part-1",
            budget_units=10_000_000,
            deadline_us=1_800_003_600_000_000,
            operation_id="create:c1",
        )
        child_1_id = res_c1.data["child_id"]
        c1_version = res_c1.data["version_id"]

        # Child-2 (part-2)
        res_c2 = service.create_child(
            conn,
            actor_id="pt-agent-a",
            parent_job_id=root_a_id,
            lease_id=lease_a_id,
            task_key="part-2",
            budget_units=10_000_000,
            deadline_us=1_800_003_600_000_000,
            operation_id="create:c2",
        )
        child_2_id = res_c2.data["child_id"]
        c2_version = res_c2.data["version_id"]

        # Worker B が Child-1 を Claim して成果物を提出
        res_claim_c1 = service.claim(
            conn,
            actor_id="pt-agent-b",
            job_id=child_1_id,
            expected_version_id=c1_version,
            operation_id="claim:c1",
        )
        sub_c1 = service.submit(
            conn,
            actor_id="pt-agent-b",
            job_id=child_1_id,
            lease_id=res_claim_c1.data["lease_id"],
            version_id=c1_version,
            artifact_json=json.dumps({"sum": 6}),
            operation_id="submit:c1",
        )
        child_1_sub_id = sub_c1.data["submission_id"]

        # Worker B が Child-2 を Claim して成果物を提出
        res_claim_c2 = service.claim(
            conn,
            actor_id="pt-agent-b",
            job_id=child_2_id,
            expected_version_id=c2_version,
            operation_id="claim:c2",
        )
        sub_c2 = service.submit(
            conn,
            actor_id="pt-agent-b",
            job_id=child_2_id,
            lease_id=res_claim_c2.data["lease_id"],
            version_id=c2_version,
            artifact_json=json.dumps({"sum": 4}),
            operation_id="submit:c2",
        )
        child_2_sub_id = sub_c2.data["submission_id"]

        # Root-C を Worker A が Claim しておく
        res_claim_c = service.claim(
            conn,
            actor_id="pt-agent-a",
            job_id=root_c_id,
            expected_version_id=service.get_job(conn, actor_id="pt-agent-a", job_id=root_c_id)[
                "version"
            ]["version_id"],
            operation_id="claim:root-c",
        )
        lease_c_id = res_claim_c.data["lease_id"]
        version_c_id = res_claim_c.data["version_id"]
    finally:
        conn.close()

    # Worker A (`pt-agent-a`) のセッションで 8 tools を順次呼び出す
    async with _connect_mcp(tmp_path, "pt-agent-a") as session:
        # 1. ojp_list_jobs
        res1 = await session.call_tool("ojp_list_jobs", arguments={"limit": 10})
        assert not res1.is_error
        p1 = _parse_content_payload(res1)
        assert p1["ok"] is True
        assert "jobs" in p1["data"]

        # 2. ojp_get_job
        res2 = await session.call_tool("ojp_get_job", arguments={"job_id": root_b_id})
        assert not res2.is_error
        p2 = _parse_content_payload(res2)
        assert p2["ok"] is True
        assert p2["data"]["job"]["job_id"] == root_b_id
        version_b_id = p2["data"]["version"]["version_id"]

        # 3. ojp_claim_job
        res3 = await session.call_tool(
            "ojp_claim_job",
            arguments={
                "job_id": root_b_id,
                "expected_version_id": version_b_id,
                "operation_id": "claim:mcp-b",
            },
        )
        assert not res3.is_error
        p3 = _parse_content_payload(res3)
        assert p3["ok"] is True
        lease_b_id = p3["data"]["lease_id"]

        # 4. ojp_heartbeat
        res4 = await session.call_tool(
            "ojp_heartbeat",
            arguments={
                "job_id": root_b_id,
                "lease_id": lease_b_id,
                "generation": 1,
                "operation_id": "heartbeat:mcp-b",
            },
        )
        assert not res4.is_error
        p4 = _parse_content_payload(res4)
        assert p4["ok"] is True
        assert "expires_at" in p4["data"]

        # 5. ojp_create_child_job
        res5 = await session.call_tool(
            "ojp_create_child_job",
            arguments={
                "parent_job_id": root_b_id,
                "lease_id": lease_b_id,
                "task_key": "part-1",
                "budget": "10.000000",
                "deadline": "2027-01-15T12:00:00Z",
                "operation_id": "create:mcp-child-b",
            },
        )
        assert not res5.is_error
        p5 = _parse_content_payload(res5)
        assert p5["ok"] is True
        assert "child_id" in p5["data"]

        # 6. ojp_submit (Root-C: Child を持たないため直ちに提出可能)
        res6 = await session.call_tool(
            "ojp_submit",
            arguments={
                "job_id": root_c_id,
                "lease_id": lease_c_id,
                "version_id": version_c_id,
                "artifact_json": json.dumps({"sum": 15}),
                "operation_id": "submit:mcp-c",
            },
        )
        assert not res6.is_error
        p6 = _parse_content_payload(res6)
        assert p6["ok"] is True
        assert "submission_id" in p6["data"]

        # 7. ojp_approve (Child-1: Worker A が Requester)
        res7 = await session.call_tool(
            "ojp_approve",
            arguments={
                "job_id": child_1_id,
                "submission_id": child_1_sub_id,
                "operation_id": "approve:mcp-c1",
            },
        )
        assert not res7.is_error
        p7 = _parse_content_payload(res7)
        assert p7["ok"] is True
        assert (p7["data"].get("job_state") or p7["data"].get("state")) == "DONE"

        # 8. ojp_dispute (Child-2: Worker A が Requester として異議提出)
        res8 = await session.call_tool(
            "ojp_dispute",
            arguments={
                "job_id": child_2_id,
                "submission_id": child_2_sub_id,
                "condition_id": "sum",
                "reason_code": "CONDITION_MISMATCH",
                "operation_id": "dispute:mcp-c2",
            },
        )
        assert not res8.is_error
        p8 = _parse_content_payload(res8)
        assert p8["ok"] is True
        assert p8["data"]["state"] == "DISPUTED"


# ---------------------------------------------------------------------------
# 6. 同一 Actor・同一 payload の再送が同じ結果を返す
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_idempotent_replay_same_payload(tmp_path: Path) -> None:
    _init_db(tmp_path)
    root_id = _create_and_fund_root(tmp_path)

    async with _connect_mcp(tmp_path, "pt-agent-a") as session:
        # version 取得
        res_get = await session.call_tool("ojp_get_job", arguments={"job_id": root_id})
        version_id = _parse_content_payload(res_get)["data"]["version"]["version_id"]

        op_id = "claim:idempotent-1"
        args = {
            "job_id": root_id,
            "expected_version_id": version_id,
            "operation_id": op_id,
        }

        # 1回目
        res1 = await session.call_tool("ojp_claim_job", arguments=args)
        assert not res1.is_error
        p1 = _parse_content_payload(res1)
        assert p1["ok"] is True
        assert p1.get("replayed") is False

        # 2回目（同一 payload）
        res2 = await session.call_tool("ojp_claim_job", arguments=args)
        assert not res2.is_error
        p2 = _parse_content_payload(res2)
        assert p2["ok"] is True
        assert p2.get("replayed") is True
        assert p2["data"] == p1["data"]


# ---------------------------------------------------------------------------
# 7. ドメインエラーが SDK の tool エラーとして識別できる
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_domain_error_mapped_to_sdk_tool_error(tmp_path: Path) -> None:
    _init_db(tmp_path)
    root_id = _create_and_fund_root(tmp_path)

    async with _connect_mcp(tmp_path, "pt-agent-a") as session:
        # 権限のない approve（Root の Requester は pt-requester だが pt-agent-a で呼ぶ）
        res = await session.call_tool(
            "ojp_approve",
            arguments={
                "job_id": root_id,
                "submission_id": "sub-non-existent",
                "operation_id": "approve:fail-1",
            },
        )
        assert res.is_error is True

        payload = _parse_content_payload(res)
        assert payload["ok"] is False
        assert "error" in payload
        err = payload["error"]
        assert "code" in err
        assert err["retryable"] is False
        assert "message" in err


# ---------------------------------------------------------------------------
# 8. 金額は6桁小数形式
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_get_job_monetary_keys_format(tmp_path: Path) -> None:
    _init_db(tmp_path)
    root_id = _create_and_fund_root(tmp_path)

    async with _connect_mcp(tmp_path, "pt-requester") as session:
        res = await session.call_tool("ojp_get_job", arguments={"job_id": root_id})
        assert not res.is_error
        payload = _parse_content_payload(res)

        decimal_pattern = re.compile(r"^[0-9]+\.[0-9]{6}$")
        budget = payload["data"]["budget"]
        for key in ["deposit", "escrow", "available", "locked", "paid", "refunded"]:
            val = budget.get(key)
            assert val is not None
            assert decimal_pattern.match(val), f"Budget key {key} = {val!r} does not match 6-decimal format"


# ---------------------------------------------------------------------------
# 9. stdout にログが混じらない（errlog を取得しても JSON-RPC が壊れない）
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_stdout_pure_and_stderr_non_interfering(tmp_path: Path) -> None:
    _init_db(tmp_path)
    root_id = _create_and_fund_root(tmp_path)

    errlog_path = tmp_path / "mcp_stderr.log"
    with open(errlog_path, "w+", encoding="utf-8") as errlog_file:
        async with _connect_mcp(tmp_path, "pt-requester", errlog=errlog_file) as session:
            res = await session.call_tool("ojp_get_job", arguments={"job_id": root_id})
            assert not res.is_error
            payload = _parse_content_payload(res)
            assert payload["ok"] is True


# ---------------------------------------------------------------------------
# 10. 権限付き取得が MCP 経由でも効く
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_authorized_get_job_artifact_permission(tmp_path: Path) -> None:
    _init_db(tmp_path)
    root_id = _create_and_fund_root(tmp_path)

    db_path = scheduler.resolve_db_path(tmp_path)
    conn = db.connect(db_path)
    try:
        # 無関係 Actor `pt-agent-c` を登録
        conn.execute(
            "INSERT INTO participants (id, label, kind) VALUES (?, ?, ?)",
            ("pt-agent-c", "pt-agent-c", "agent"),
        )
        conn.commit()

        # Worker A が Root を Claim して Child-1 を作成
        v_id = service.get_job(conn, actor_id="pt-agent-a", job_id=root_id)["version"]["version_id"]
        res_claim_r = service.claim(
            conn,
            actor_id="pt-agent-a",
            job_id=root_id,
            expected_version_id=v_id,
            operation_id="claim:r",
        )
        res_c = service.create_child(
            conn,
            actor_id="pt-agent-a",
            parent_job_id=root_id,
            lease_id=res_claim_r.data["lease_id"],
            task_key="part-1",
            budget_units=10_000_000,
            deadline_us=1_800_003_600_000_000,
            operation_id="create:c",
        )
        child_id = res_c.data["child_id"]
        c_ver = res_c.data["version_id"]

        # Worker B が Child を Claim して提出
        res_claim_c = service.claim(
            conn,
            actor_id="pt-agent-b",
            job_id=child_id,
            expected_version_id=c_ver,
            operation_id="claim:c",
        )
        artifact_content = json.dumps({"sum": 6})
        service.submit(
            conn,
            actor_id="pt-agent-b",
            job_id=child_id,
            lease_id=res_claim_c.data["lease_id"],
            version_id=c_ver,
            artifact_json=artifact_content,
            operation_id="submit:c",
        )
    finally:
        conn.close()

    # (a) Root Requester (`pt-requester`) サーバーから取得:
    # Root Requester は Child の成果物を閲覧可能
    async with _connect_mcp(tmp_path, "pt-requester") as session_req:
        res_req = await session_req.call_tool("ojp_get_job", arguments={"job_id": child_id})
        assert not res_req.is_error
        p_req = _parse_content_payload(res_req)
        sub_req = p_req["data"]["submission"]
        assert sub_req is not None
        assert sub_req["artifact_readable"] is True
        assert "artifact_json" in sub_req
        assert json.loads(sub_req["artifact_json"]) == json.loads(artifact_content)

    # (b) 無関係 Actor (`pt-agent-c`) サーバーから取得:
    # 第三者は閲覧不可
    async with _connect_mcp(tmp_path, "pt-agent-c") as session_other:
        res_other = await session_other.call_tool("ojp_get_job", arguments={"job_id": child_id})
        assert not res_other.is_error
        p_other = _parse_content_payload(res_other)
        sub_other = p_other["data"]["submission"]
        assert sub_other is not None
        assert sub_other["artifact_readable"] is False
        assert "artifact_json" not in sub_other
        assert "artifact_hash" not in sub_other


@pytest.mark.anyio
async def test_get_job_filters_dispute_resolution_over_mcp(tmp_path: Path) -> None:
    """MCP の共通応答封筒でも resolution は JSON object であり、
    成果物を読めない Actor には提出物由来の判定結果（outcome / reason /
    condition_matched / 実値 / 証跡）を返さない。"""
    _init_db(tmp_path)
    root_id = _create_and_fund_root(tmp_path, "resolution")

    conn = db.connect(scheduler.resolve_db_path(tmp_path))
    try:
        conn.execute(
            "INSERT INTO participants (id, label, kind) VALUES (?, ?, ?)",
            ("pt-agent-c", "pt-agent-c", "agent"),
        )
        conn.commit()
        root_version = service.get_job(
            conn, actor_id="pt-agent-a", job_id=root_id
        )["version"]["version_id"]
        root_claim = service.claim(
            conn,
            actor_id="pt-agent-a",
            job_id=root_id,
            expected_version_id=root_version,
            operation_id="claim:resolution-root",
        )
        child = service.create_child(
            conn,
            actor_id="pt-agent-a",
            parent_job_id=root_id,
            lease_id=root_claim.data["lease_id"],
            task_key="part-1",
            budget_units=10_000_000,
            deadline_us=1_800_003_600_000_000,
            operation_id="create:resolution-child",
        )
        child_id = child.data["child_id"]
        child_claim = service.claim(
            conn,
            actor_id="pt-agent-b",
            job_id=child_id,
            expected_version_id=child.data["version_id"],
            operation_id="claim:resolution-child",
        )
        submission = service.submit(
            conn,
            actor_id="pt-agent-b",
            job_id=child_id,
            lease_id=child_claim.data["lease_id"],
            version_id=child.data["version_id"],
            artifact_json=json.dumps({"sum": 6}),
            operation_id="submit:resolution-child",
        )
        service.dispute(
            conn,
            actor_id="pt-agent-a",
            job_id=child_id,
            submission_id=submission.data["submission_id"],
            condition_id="sum",
            reason_code="CONDITION_MISMATCH",
            operation_id="dispute:resolution-child",
        )
        verification.arbiter_stored_artifact_override = lambda label: '{"sum": 7}'
        try:
            resolved = service.resolve_due_disputes(conn, actor_id="pt-system")
        finally:
            verification.arbiter_stored_artifact_override = None
        assert resolved[0].data["resolution"] == "FAIL"
    finally:
        conn.close()

    async with _connect_mcp(tmp_path, "pt-agent-c") as session:
        result = await session.call_tool("ojp_get_job", arguments={"job_id": child_id})
        assert not result.is_error
        payload = _parse_content_payload(result)
        resolution = payload["data"]["verdict"]["dispute"]["resolution"]
        assert isinstance(resolution, dict)
        assert resolution["condition_id"] == "sum"
        for hidden in (
            "outcome",
            "reason",
            "condition_matched",
            "evidence",
            "actual_value",
            "failed_condition_id",
            "input_hash",
        ):
            assert hidden not in resolution
        assert payload["data"]["verdict"]["acceptance"]["reason"] is None


# ---------------------------------------------------------------------------
# 11. DB 未存在で起動拒否
# ---------------------------------------------------------------------------


def test_missing_database_rejection(tmp_path: Path) -> None:
    empty_root = tmp_path / "nonexistent"
    empty_root.mkdir(parents=True)
    db_file = empty_root / "data" / "ojp.sqlite3"
    assert not db_file.exists()

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "ojp.cli",
            "--root",
            str(empty_root),
            "--clock-mode",
            "test",
            "mcp",
            "--actor",
            "pt-requester",
        ],
        capture_output=True,
        text=True,
    )
    # exit 2 (INVALID_STATE)
    assert proc.returncode != 0
    # DB ファイルが新規作成されていないこと
    assert not db_file.exists()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
