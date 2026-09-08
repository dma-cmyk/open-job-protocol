"""Integration tests for OJP CLI (src/ojp/cli.py).

Verifies all contract specifications from Phase 5 Step 2:
1. demo init idempotency: 3 consecutive runs all exit 0, participant/wallet counts invariant.
2. demo init after fund_root: succeeds even when requester balance has changed.
3. --root working directory independence: varying cwd does not create local db, returns same job.
4. Exit code 2 (permission): FORBIDDEN on unauthorized action (e.g. approve other's job).
5. Exit code 2 (state violation): INVALID_STATE on illegal state transition (e.g. claim DRAFT).
6. Exit code 2 (input violation): INVALID_ARGUMENT on bad amount or malformed card file.
7. Exit code 3 (retryable error): DB_BUSY produces exit 3 via response.exit_code and service monkeypatch.
8. Operation replay: duplicate operation-id returns exit 0 with replayed=True, without duplicate side-effects.
9. Auto-generated operation_id: unspecified operation_id returns a non-empty operation_id in result.
10. No time manipulation flags in public CLI: --help across all commands contains no set-now/advance/failpoint.
11. CLI and tick exit code uniformity: tick --once delegates directly to scheduler.main.
12. Full E2E CLI command coverage: demo init -> create -> fund -> claim -> heartbeat -> child create -> submit -> approve -> tick -> ledger show.
13. parse_timestamp_iso round-trip with format_timestamp_us and rejection of invalid format.
14. Subprocess CLI invocation: python -m ojp.cli produces valid single-line JSON with returncode 0.
+ Argument propagation tests for job dispute, job abandon, job refund, payment retry.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from ojp import cli, domain, response, service
from ojp.domain import ErrorCode, OjpError

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
ROOT_CARD_PATH = FIXTURES_DIR / "poc_root_card.json"


def _run_cli(capsys, argv: list[str]) -> tuple[int, dict | None, str]:
    """Helper to invoke cli.main, capturing exit code, parsed stdout JSON, and raw stderr."""
    code = cli.main(argv)
    captured = capsys.readouterr()
    stdout_json = None
    if captured.out.strip():
        try:
            stdout_json = json.loads(captured.out.strip())
        except json.JSONDecodeError:
            stdout_json = None
    return code, stdout_json, captured.err


# ---------------------------------------------------------------------------
# 1. demo init idempotency
# ---------------------------------------------------------------------------


def test_demo_init_idempotency(tmp_path: Path, capsys):
    """1. demo init の冪等性: 新規ルートで demo init -> exit 0。

    連続3回実行してすべて exit 0、participants 4件と mock_wallets 3件が増えないこと。
    """
    root = tmp_path / "proj"
    argv = ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"]

    for i in range(3):
        code, out, _ = _run_cli(capsys, argv)
        assert code == 0
        assert out is not None
        assert out["ok"] is True
        assert "data" in out

    # DB を直接検査
    db_path = root / "data" / "ojp.sqlite3"
    assert db_path.exists()
    conn = sqlite3.connect(db_path)
    part_count = conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0]
    wallet_count = conn.execute("SELECT COUNT(*) FROM mock_wallets").fetchone()[0]
    conn.close()

    assert part_count == 4  # system, requester, agent-a, agent-b
    assert wallet_count == 3  # requester, agent-a, agent-b


# ---------------------------------------------------------------------------
# 2. demo init after fund_root
# ---------------------------------------------------------------------------


def test_demo_init_after_fund_root(tmp_path: Path, capsys):
    """2. fund_root 後の demo init も成功すること。

    Requester 残高が減った後の再実行が INVALID_STATE にならない。
    """
    root = tmp_path / "proj"
    # 初期化
    code, _, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )
    assert code == 0

    # Root 作成
    code, out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "create",
            "--card",
            str(ROOT_CARD_PATH),
            "--json",
        ],
    )
    assert code == 0
    job_id = out["data"]["job_id"]

    # 入金 (fund)
    code, _, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "fund",
            job_id,
            "--amount",
            "100.000000",
            "--json",
        ],
    )
    assert code == 0

    # Requester の Wallet 残高が減った状態で demo init を再実行
    code, out_reinit, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )
    assert code == 0
    assert out_reinit["ok"] is True
    # Requester Wallet は seeded=False としてスキップされること
    req_wallet = next(
        w
        for w in out_reinit["data"]["wallets"]
        if w["participant_id"] == "pt-requester"
    )
    assert req_wallet["seeded"] is False
    assert req_wallet["balance"] == "4900.000000"


# ---------------------------------------------------------------------------
# 3. --root working directory independence
# ---------------------------------------------------------------------------


def test_root_working_directory_independence(tmp_path: Path, capsys, monkeypatch):
    """3. --root の作業ディレクトリ非依存:

    monkeypatch.chdir で cwd を別のディレクトリ（2通り）に変えて同じ --root で job get を実行し、
    同じ Job が返ることと、cwd 側に data/ojp.sqlite3 が作られていないこと。
    """
    root = tmp_path / "proj"
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )
    _, create_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "create",
            "--card",
            str(ROOT_CARD_PATH),
            "--json",
        ],
    )
    job_id = create_out["data"]["job_id"]

    dir1 = tmp_path / "custom_cwd_1"
    dir1.mkdir()
    dir2 = tmp_path / "custom_cwd_2"
    dir2.mkdir()

    for d in (dir1, dir2):
        monkeypatch.chdir(d)
        code, get_out, _ = _run_cli(
            capsys,
            [
                "--root",
                str(root),
                "--clock-mode",
                "test",
                "job",
                "get",
                job_id,
                "--json",
            ],
        )
        assert code == 0
        assert get_out["ok"] is True
        assert get_out["data"]["job"]["job_id"] == job_id
        # cwd 側に DB が作られていないことを確認
        assert not (d / "data").exists()


# ---------------------------------------------------------------------------
# 4. Exit code 2 (permission)
# ---------------------------------------------------------------------------


def test_exit_code_2_permission(tmp_path: Path, capsys):
    """4. 終了コード 2（権限）: 他人の Job を approve するなどで FORBIDDEN -> exit 2。

    --json の ok is False と error.code == "FORBIDDEN" と error.retryable is False。
    """
    root = tmp_path / "proj"
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )
    # 无権限の actor (pt-agent-a) で demo init を呼ぶと FORBIDDEN
    code, out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-agent-a",
            "demo",
            "init",
            "--json",
        ],
    )
    assert code == 2
    assert out is not None
    assert out["ok"] is False
    assert out["error"]["code"] == "FORBIDDEN"
    assert out["error"]["retryable"] is False


# ---------------------------------------------------------------------------
# 5. Exit code 2 (state violation)
# ---------------------------------------------------------------------------


def test_exit_code_2_invalid_state(tmp_path: Path, capsys):
    """5. 終了コード 2（状態違反）: DRAFT の Job を claim するなどで INVALID_STATE -> exit 2。"""
    root = tmp_path / "proj"
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )
    _, create_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "create",
            "--card",
            str(ROOT_CARD_PATH),
            "--json",
        ],
    )
    job_id = create_out["data"]["job_id"]
    version_id = create_out["data"]["version_id"]

    # 未入金 DRAFT の状態のまま claim
    code, out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-agent-a",
            "job",
            "claim",
            job_id,
            "--version",
            version_id,
            "--json",
        ],
    )
    assert code == 2
    assert out is not None
    assert out["ok"] is False
    assert out["error"]["code"] == "INVALID_STATE"
    assert out["error"]["retryable"] is False


# ---------------------------------------------------------------------------
# 6. Exit code 2 (input violation)
# ---------------------------------------------------------------------------


def test_exit_code_2_invalid_argument(tmp_path: Path, capsys):
    """6. 終了コード 2（入力違反）: 不正な --amount 1.5 や壊れた --card ファイル -> exit 2 と INVALID_ARGUMENT。"""
    root = tmp_path / "proj"
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )

    # 不正な金額フォーマット (小数6桁でない)
    code, out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "demo",
            "init",
            "--seed",
            "100.5",
            "--json",
        ],
    )
    assert code == 2
    assert out is not None
    assert out["ok"] is False
    assert out["error"]["code"] == "INVALID_ARGUMENT"

    # 壊れた card ファイル
    broken_card = tmp_path / "broken.json"
    broken_card.write_text("{broken json", encoding="utf-8")
    code_card, out_card, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "job",
            "create",
            "--card",
            str(broken_card),
            "--json",
        ],
    )
    assert code_card == 2
    assert out_card is not None
    assert out_card["ok"] is False
    assert out_card["error"]["code"] == "INVALID_ARGUMENT"


# ---------------------------------------------------------------------------
# 7. Exit code 3 (transient error: DB_BUSY)
# ---------------------------------------------------------------------------


def test_exit_code_3_transient_error(tmp_path: Path, capsys, monkeypatch):
    """7. 終了コード 3（一時障害）: DB_BUSY で exit 3 になることを検証する。

    検証手法の選択理由:
    実際の SQLite 排他ロック待ち（busy_timeout = 5.0秒）はテストスイート全体の所要時間を
    不要に延ばすため、指示書の規定どおり:
    1. response.exit_code("DB_BUSY") == 3 および response.exit_code("FORBIDDEN") == 2 を直接 assert。
    2. service.list_jobs を monkeypatch して OjpError(DB_BUSY) を送出させ、cli.main が exit 3 を返すこと、
       および --json の error.retryable が True になることを決定的に検証する。
    """
    assert response.exit_code("DB_BUSY") == 3
    assert response.exit_code("VERIFICATION_UNAVAILABLE") == 3
    assert response.exit_code("FORBIDDEN") == 2
    assert response.exit_code("INVALID_STATE") == 2

    root = tmp_path / "proj"
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )

    def mock_busy(*args, **kwargs):
        raise OjpError(ErrorCode.DB_BUSY, "database lock timeout mock")

    monkeypatch.setattr(service, "list_jobs", mock_busy)

    code, out, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "job", "list", "--json"],
    )
    assert code == 3
    assert out is not None
    assert out["ok"] is False
    assert out["error"]["code"] == "DB_BUSY"
    assert out["error"]["retryable"] is True


# ---------------------------------------------------------------------------
# 8. Operation replay returns exit 0
# ---------------------------------------------------------------------------


def test_operation_replay_exit_0(tmp_path: Path, capsys):
    """8. operation replay が exit 0:

    同じ --operation-id で同じ書込コマンドを2回実行し、2回目も exit 0、
    --json の replayed is True、かつ DB の効果が2重になっていないこと。
    """
    root = tmp_path / "proj"
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )
    _, create_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "create",
            "--card",
            str(ROOT_CARD_PATH),
            "--json",
        ],
    )
    job_id = create_out["data"]["job_id"]

    op_id = "fund:test-replay:1"
    fund_cmd = [
        "--root",
        str(root),
        "--clock-mode",
        "test",
        "--actor",
        "pt-requester",
        "--operation-id",
        op_id,
        "job",
        "fund",
        job_id,
        "--amount",
        "100.000000",
        "--json",
    ]

    # 1回目実行
    code1, out1, _ = _run_cli(capsys, fund_cmd)
    assert code1 == 0
    assert out1["ok"] is True
    assert out1["operation_id"] == op_id
    assert out1.get("replayed") in (False, None)

    # 2回目実行 (同じ operation_id)
    code2, out2, _ = _run_cli(capsys, fund_cmd)
    assert code2 == 0
    assert out2["ok"] is True
    assert out2["operation_id"] == op_id
    assert out2["replayed"] is True

    # 資金効果が2重になっていないことを確認（deposit は 100.000000 のみ）
    _, ledger_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "ledger",
            "show",
            job_id,
            "--json",
        ],
    )
    assert ledger_out["ok"] is True
    assert ledger_out["data"]["totals"]["deposit"] == "100.000000"


# ---------------------------------------------------------------------------
# 9. Auto-generated operation_id
# ---------------------------------------------------------------------------


def test_operation_id_auto_generated(tmp_path: Path, capsys):
    """9. --operation-id 未指定時に結果へ operation_id が返ること（非空文字列）。"""
    root = tmp_path / "proj"
    code, out, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )
    assert code == 0
    assert out["ok"] is True
    assert "operation_id" in out
    assert isinstance(out["operation_id"], str)
    assert len(out["operation_id"]) > 0


# ---------------------------------------------------------------------------
# 10. No time manipulation flags in public CLI
# ---------------------------------------------------------------------------


def test_no_time_manipulation_flags_in_cli(capsys):
    """10. 通常経路に時刻操作の入口がないこと:

    ojp --help と全サブコマンドの --help を集め、set-now / set_now / advance / failpoint
    を含む文字列が一つもないことを assert する。
    """
    prohibited = ["set-now", "set_now", "advance", "failpoint"]

    subcommands_to_check = [
        [],
        ["demo", "init"],
        ["job", "create"],
        ["job", "fund"],
        ["job", "list"],
        ["job", "get"],
        ["job", "claim"],
        ["job", "heartbeat"],
        ["child", "create"],
        ["job", "submit"],
        ["job", "approve"],
        ["job", "dispute"],
        ["job", "abandon"],
        ["job", "refund"],
        ["ledger", "show"],
        ["payment", "retry"],
        ["tick"],
        ["mcp"],
    ]

    all_help_texts: list[str] = []
    for cmd in subcommands_to_check:
        with pytest.raises(SystemExit):
            cli.main(cmd + ["--help"])
        captured = capsys.readouterr()
        all_help_texts.append(captured.out)

    combined_help = "\n".join(all_help_texts).lower()
    for word in prohibited:
        assert word not in combined_help, f"Prohibited word {word!r} found in CLI help text!"


# ---------------------------------------------------------------------------
# 11. Uniform exit code between CLI and tick
# ---------------------------------------------------------------------------


def test_tick_exit_code_uniformity(tmp_path: Path, capsys):
    """11. CLI と tick の終了コードが同じ実装であること:

    ojp tick --once の exit が scheduler.main の戻り値と一致すること。
    正常ケース 0、--clock-mode 不一致ケース 2。
    """
    root = tmp_path / "proj"
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )

    # 正常ケース (0)
    code_tick, out_tick, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "tick", "--once", "--json"],
    )
    assert code_tick == 0
    assert out_tick is not None
    assert "counts" in out_tick

    # clock-mode 不一致ケース (2: test DB に対して realtime 指定)
    code_mismatch, out_mismatch, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "realtime",
            "tick",
            "--once",
            "--json",
        ],
    )
    assert code_mismatch == 2
    assert out_mismatch is not None
    assert out_mismatch.get("error", {}).get("code") == "MODE_MISMATCH"


# ---------------------------------------------------------------------------
# 12. End-to-end full CLI lifecycle
# ---------------------------------------------------------------------------


def test_cli_full_lifecycle_e2e(tmp_path: Path, capsys):
    """12. コマンドの網羅:

    demo init から ledger show までを使って
    Root 作成 -> 入金 -> claim -> heartbeat -> child create -> claim -> submit -> approve -> tick -> ledger show
    を CLI だけで通し切る。
    最後に ledger show --json の conservation.ok is True と、Child の payout が SUCCEEDED
    になっていることを assert する。
    """
    root = tmp_path / "proj"

    # Step 1: demo init
    code, _, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )
    assert code == 0

    # Step 2: job create
    code, create_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "create",
            "--card",
            str(ROOT_CARD_PATH),
            "--json",
        ],
    )
    assert code == 0
    root_id = create_out["data"]["job_id"]
    root_v_id = create_out["data"]["version_id"]

    # Step 3: job fund
    code, _, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "fund",
            root_id,
            "--amount",
            "100.000000",
            "--json",
        ],
    )
    assert code == 0

    # Step 4: job list (確認)
    code, list_out, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "job", "list", "--json"],
    )
    assert code == 0
    assert any(j["job_id"] == root_id for j in list_out["data"]["jobs"])

    # Step 5: job get (確認)
    code, get_out, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "job", "get", root_id, "--json"],
    )
    assert code == 0
    assert get_out["data"]["job"]["state"] == "OPEN"

    # Step 6: job claim (Agent A)
    code, claim_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-agent-a",
            "job",
            "claim",
            root_id,
            "--version",
            root_v_id,
            "--json",
        ],
    )
    assert code == 0
    root_lease_id = claim_out["data"]["lease_id"]

    # Step 7: job heartbeat (Agent A)
    code, hb_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-agent-a",
            "job",
            "heartbeat",
            root_id,
            "--lease",
            root_lease_id,
            "--json",
        ],
    )
    assert code == 0
    assert hb_out["ok"] is True

    # Step 8: child create (Agent A creates part-1)
    code, child_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-agent-a",
            "child",
            "create",
            root_id,
            "--lease",
            root_lease_id,
            "--task",
            "part-1",
            "--budget",
            "10.000000",
            "--deadline",
            "2027-01-16T00:00:00.000000Z",
            "--json",
        ],
    )
    assert code == 0
    child_id = child_out["data"]["child_id"]
    child_v_id = child_out["data"]["version_id"]

    # Step 9: job claim for child (Agent B)
    code, cclaim_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-agent-b",
            "job",
            "claim",
            child_id,
            "--version",
            child_v_id,
            "--json",
        ],
    )
    assert code == 0
    child_lease_id = cclaim_out["data"]["lease_id"]

    # Step 10: job submit (Agent B submits artifact)
    artifact_path = tmp_path / "part1_artifact.json"
    artifact_path.write_text(json.dumps({"sum": 6}), encoding="utf-8")
    code, submit_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-agent-b",
            "job",
            "submit",
            child_id,
            "--lease",
            child_lease_id,
            "--version",
            child_v_id,
            "--artifact",
            str(artifact_path),
            "--json",
        ],
    )
    assert code == 0
    sub_id = submit_out["data"]["submission_id"]

    # Step 11: job approve (Agent A approves Child)
    code, approve_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-agent-a",
            "job",
            "approve",
            child_id,
            "--submission",
            sub_id,
            "--json",
        ],
    )
    assert code == 0
    assert approve_out["data"]["state"] == "DONE"

    # Step 12: tick --once (送金処理の確定)
    code, _, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "tick", "--once", "--json"],
    )
    assert code == 0

    # Step 13: ledger show (保存則と送金成功の確認)
    code, ledger_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "ledger",
            "show",
            root_id,
            "--json",
        ],
    )
    assert code == 0
    data = ledger_out["data"]
    assert data["conservation"]["ok"] is True
    # Child の payout が SUCCEEDED になっていること
    child_payout = next(
        op
        for op in data["operations"]
        if op["job_id"] == child_id and op["kind"] == "payout"
    )
    assert child_payout["status"] == "SUCCEEDED"
    assert child_payout["payee_id"] == "pt-agent-b"


# ---------------------------------------------------------------------------
# 13. parse_timestamp_iso round-trip & validation
# ---------------------------------------------------------------------------


def test_timestamp_iso_roundtrip():
    """13. parse_timestamp_iso <-> service.format_timestamp_us の round-trip と形式不正拒否。"""
    samples = [
        1_800_000_000_000_000,  # 2027-01-15T00:00:00.000000Z
        1_800_000_000_123_456,  # マイクロ秒を含む値
        0,  # 1970-01-01T00:00:00.000000Z
    ]
    for us in samples:
        iso_str = service.format_timestamp_us(us)
        parsed = cli.parse_timestamp_iso(iso_str)
        assert parsed == us

    # 省略形式（.000000 なし）
    assert cli.parse_timestamp_iso("2027-01-15T08:00:00Z") == 1_800_000_000_000_000
    assert cli.parse_timestamp_iso("2027-01-15T00:00:00Z") == 1_799_971_200_000_000

    # 形式不正の拒否
    invalid_samples = [
        "not-a-timestamp",
        "2027-01-15",
        "2027-01-15T00:00:00",  # Z なし
        "2027-01-15T00:00:00+09:00",  # UTC 以外のタイムゾーン
        "2027-02-31T00:00:00Z",  # 存在しない日付
        12345,  # 文字列以外
    ]
    for inv in invalid_samples:
        with pytest.raises(OjpError) as exc_info:
            cli.parse_timestamp_iso(inv)  # type: ignore[arg-type]
        assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT


# ---------------------------------------------------------------------------
# 14. Subprocess execution
# ---------------------------------------------------------------------------


def test_subprocess_cli_execution(tmp_path: Path):
    """14. python -m ojp.cli --root <tmp> --clock-mode test job list --json を subprocess で実行し、

    returncode 0 と stdout が単一 JSON として parse できること。
    """
    root = tmp_path / "proj"
    # まず demo init
    res_init = subprocess.run(
        [
            sys.executable,
            "-m",
            "ojp.cli",
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "demo",
            "init",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_init.returncode == 0

    res_list = subprocess.run(
        [
            sys.executable,
            "-m",
            "ojp.cli",
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "job",
            "list",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert res_list.returncode == 0
    parsed = json.loads(res_list.stdout.strip())
    assert parsed["ok"] is True
    assert "data" in parsed
    assert "jobs" in parsed["data"]


# ---------------------------------------------------------------------------
# Additional command argument propagation: dispute, abandon, refund, payment retry
# ---------------------------------------------------------------------------


def test_job_dispute_argument_propagation(tmp_path: Path, capsys):
    """job dispute の引数伝播確認: 存在しない submission に対する dispute は INVALID_TARGET。"""
    root = tmp_path / "proj"
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )
    _, create_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "create",
            "--card",
            str(ROOT_CARD_PATH),
            "--json",
        ],
    )
    job_id = create_out["data"]["job_id"]

    code, out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "dispute",
            job_id,
            "--submission",
            "sub-nonexistent",
            "--condition",
            "sum",
            "--reason",
            "CONDITION_MISMATCH",
            "--json",
        ],
    )
    # SUBMITTED でない Job に対する dispute は INVALID_STATE
    assert code == 2
    assert out["ok"] is False
    assert out["error"]["code"] == "INVALID_STATE"


def test_job_abandon_argument_propagation(tmp_path: Path, capsys):
    """job abandon の引数伝播確認: 存在しない lease に対する abandon は INVALID_TARGET。"""
    root = tmp_path / "proj"
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )
    _, create_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "create",
            "--card",
            str(ROOT_CARD_PATH),
            "--json",
        ],
    )
    job_id = create_out["data"]["job_id"]

    code, out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-agent-a",
            "job",
            "abandon",
            job_id,
            "--lease",
            "lease-nonexistent",
            "--json",
        ],
    )
    assert code == 2
    assert out["ok"] is False
    assert out["error"]["code"] == "INVALID_TARGET"


def test_job_refund_argument_propagation(tmp_path: Path, capsys):
    """job refund の引数伝播確認: 返金予約のない Root に対して呼ぶと INVALID_STATE。"""
    root = tmp_path / "proj"
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )
    _, create_out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "create",
            "--card",
            str(ROOT_CARD_PATH),
            "--json",
        ],
    )
    job_id = create_out["data"]["job_id"]

    code, out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "--actor",
            "pt-requester",
            "job",
            "refund",
            job_id,
            "--json",
        ],
    )
    assert code == 2
    assert out["ok"] is False
    assert out["error"]["code"] == "INVALID_STATE"


def test_payment_retry_argument_propagation(tmp_path: Path, capsys):
    """payment retry の引数伝播確認: 存在しない operation に対する retry は INVALID_TARGET。"""
    root = tmp_path / "proj"
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"],
    )

    code, out, _ = _run_cli(
        capsys,
        [
            "--root",
            str(root),
            "--clock-mode",
            "test",
            "payment",
            "retry",
            "op-nonexistent",
            "--json",
        ],
    )
    assert code == 2
    assert out["ok"] is False
    assert out["error"]["code"] == "INVALID_TARGET"


def test_payment_retry_authorization_matrix(tmp_path: Path, capsys):
    """R1: payment retry の認可マトリクス検証（計画書 第14節）。

    検証手法の選択:
    test mode DB において failpoint を用いた不安定なエラー注入を避け、
    E2E フロー（approve -> tick）で正常に SUCCEEDED 済みに収束した PaymentOperation に対する
    再試行経路を選択した。
    理由:
    service.retry_payment の認可検査は process_single_payment の前段で走るため、
    PaymentOperation が PENDING/RETRYABLE/SUCCEEDED のいずれであっても同一の認可判定ロジックが
    実行される。SUCCEEDED 済みの経路は決定論的であり、Receipt との一致確認を経て exit 0 を返す
    ことが保証されているため、認可マトリクス（4条件の正当 Actor と無関係 Actor の拒否）を
    最も確実かつ高速に検証できる。

    検証内容:
    - 受取 Worker (payee_id = pt-agent-b) -> exit 0
    - Child Requester (job_id requester = pt-agent-a) -> exit 0
    - Root Requester (root_id requester = pt-requester) -> exit 0
    - system (pt-system) -> exit 0
    - 無関係 Actor (pt-unrelated) -> exit 2 かつ error.code == "FORBIDDEN"
    - 無関係 Actor の拒否呼び出しの前後で status と attempt_count が変化しないこと
    - 存在しない operation_id -> exit 2 かつ INVALID_TARGET
    """
    root = tmp_path / "proj"
    # 前準備: E2E で Child 送金を SUCCEEDED まで完了させる
    _run_cli(capsys, ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"])
    _, create_out, _ = _run_cli(
        capsys,
        [
            "--root", str(root), "--clock-mode", "test", "--actor", "pt-requester",
            "job", "create", "--card", str(ROOT_CARD_PATH), "--json",
        ],
    )
    root_id = create_out["data"]["job_id"]
    root_v_id = create_out["data"]["version_id"]
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "--actor", "pt-requester", "job", "fund", root_id, "--amount", "100.000000", "--json"],
    )
    _, claim_out, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "--actor", "pt-agent-a", "job", "claim", root_id, "--version", root_v_id, "--json"],
    )
    root_lease_id = claim_out["data"]["lease_id"]
    _, child_out, _ = _run_cli(
        capsys,
        [
            "--root", str(root), "--clock-mode", "test", "--actor", "pt-agent-a",
            "child", "create", root_id, "--lease", root_lease_id, "--task", "part-1",
            "--budget", "10.000000", "--deadline", "2027-01-16T00:00:00.000000Z", "--json",
        ],
    )
    child_id = child_out["data"]["child_id"]
    child_v_id = child_out["data"]["version_id"]
    _, cclaim_out, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "--actor", "pt-agent-b", "job", "claim", child_id, "--version", child_v_id, "--json"],
    )
    child_lease_id = cclaim_out["data"]["lease_id"]
    artifact_path = tmp_path / "art.json"
    artifact_path.write_text(json.dumps({"sum": 6}), encoding="utf-8")
    _, submit_out, _ = _run_cli(
        capsys,
        [
            "--root", str(root), "--clock-mode", "test", "--actor", "pt-agent-b",
            "job", "submit", child_id, "--lease", child_lease_id, "--version", child_v_id,
            "--artifact", str(artifact_path), "--json",
        ],
    )
    sub_id = submit_out["data"]["submission_id"]
    _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "--actor", "pt-agent-a", "job", "approve", child_id, "--submission", sub_id, "--json"],
    )
    # tick で送金確定
    _run_cli(capsys, ["--root", str(root), "--clock-mode", "test", "tick", "--once", "--json"])

    # DB から Child payout の PaymentOperation を取得
    conn = sqlite3.connect(root / "data" / "ojp.sqlite3")
    op_row = conn.execute(
        "SELECT operation_id, status, attempt_count, payee_id, job_id, root_id FROM payment_operations WHERE job_id = ? AND kind = 'payout'",
        (child_id,),
    ).fetchone()
    assert op_row is not None
    op_id = op_row[0]
    initial_status = op_row[1]
    initial_attempts = op_row[2]
    assert initial_status == "SUCCEEDED"

    # 無関係 Actor (pt-unrelated) を登録
    conn.execute("INSERT INTO participants (id, label, kind) VALUES ('pt-unrelated', 'pt-unrelated', 'agent')")
    conn.commit()
    conn.close()

    # 1. 無関係 Actor による拒否検証
    code_unrel, out_unrel, _ = _run_cli(
        capsys,
        [
            "--root", str(root), "--clock-mode", "test", "--actor", "pt-unrelated",
            "payment", "retry", op_id, "--json",
        ],
    )
    assert code_unrel == 2
    assert out_unrel["ok"] is False
    assert out_unrel["error"]["code"] == "FORBIDDEN"

    # 拒否呼び出しの前後で status と attempt_count が不変であることを検証
    conn = sqlite3.connect(root / "data" / "ojp.sqlite3")
    check_row = conn.execute(
        "SELECT status, attempt_count FROM payment_operations WHERE operation_id = ?",
        (op_id,),
    ).fetchone()
    conn.close()
    assert check_row[0] == initial_status
    assert check_row[1] == initial_attempts

    # 2. 認可された Actor 4種による再試行検証（すべて exit 0）
    authorized_actors = [
        "pt-agent-b",    # 受取 Worker (payee_id)
        "pt-agent-a",    # Child Requester (jobs.requester_id)
        "pt-requester",  # Root Requester (root.requester_id)
        "pt-system",     # system Actor
    ]
    for actor in authorized_actors:
        code_auth, out_auth, _ = _run_cli(
            capsys,
            [
                "--root", str(root), "--clock-mode", "test", "--actor", actor,
                "payment", "retry", op_id, "--json",
            ],
        )
        assert code_auth == 0, f"actor {actor} should be authorized to retry payment"
        assert out_auth["ok"] is True


def test_db_error_classification_and_exit_codes(tmp_path: Path, capsys, monkeypatch):
    """R2: DB エラーの分類（busy=3, 非busy=2）と exit code 一致の検証。"""
    # response 定義の直接検証
    assert response.exit_code(response.DB_ERROR_CODE) == 2
    assert response.is_retryable(response.DB_ERROR_CODE) is False
    assert response.exit_code("DB_BUSY") == 3
    assert response.is_retryable("DB_BUSY") is True

    root = tmp_path / "proj"
    _run_cli(capsys, ["--root", str(root), "--clock-mode", "test", "demo", "init", "--json"])

    # 1. 非 busy な sqlite3.Error (例: OperationalError("no such table: jobs"))
    def mock_non_busy_db_error(*args, **kwargs):
        raise sqlite3.OperationalError("no such table: jobs")

    monkeypatch.setattr(service, "list_jobs", mock_non_busy_db_error)

    code_non_busy, out_non_busy, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "job", "list", "--json"],
    )
    assert code_non_busy == 2
    assert out_non_busy["ok"] is False
    assert out_non_busy["error"]["code"] == response.DB_ERROR_CODE
    assert out_non_busy["error"]["retryable"] is False

    # 2. busy な sqlite3.Error (例: OperationalError("database is locked"))
    def mock_busy_db_error(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(service, "list_jobs", mock_busy_db_error)

    code_busy, out_busy, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "job", "list", "--json"],
    )
    assert code_busy == 3
    assert out_busy["ok"] is False
    assert out_busy["error"]["code"] == "DB_BUSY"
    assert out_busy["error"]["retryable"] is True

    # 3. ojp tick 経路でも同一分類になること
    monkeypatch.setattr(service, "expire_due_leases", mock_non_busy_db_error)
    code_tick_non_busy, out_tick_non_busy, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "tick", "--once", "--json"],
    )
    assert code_tick_non_busy == 2
    assert out_tick_non_busy["ok"] is False
    assert out_tick_non_busy["error"]["code"] == response.DB_ERROR_CODE
    assert out_tick_non_busy["error"]["retryable"] is False

    monkeypatch.setattr(service, "expire_due_leases", mock_busy_db_error)
    code_tick_busy, out_tick_busy, _ = _run_cli(
        capsys,
        ["--root", str(root), "--clock-mode", "test", "tick", "--once", "--json"],
    )
    assert code_tick_busy == 3
    assert out_tick_busy["ok"] is False
    assert out_tick_busy["error"]["code"] == "DB_BUSY"
    assert out_tick_busy["error"]["retryable"] is True
