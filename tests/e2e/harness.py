"""実プロセス E2E harness（計画書 第17節・第18節）。

Human 役は CLI（`python -m ojp.cli`）、A/B 役は MCP stdio クライアント
（`ojp mcp --actor ...` を A・B それぞれ別サーバープロセスで起動）、
期限処理・決済は独立 tick プロセス（`ojp tick --once` / `--watch`）で行う。
DB は tmp_path 配下の実ファイル SQLite。

harness はシナリオ固有の期待値を持たない（E01〜E04 で共用する）。

共有 Clock の前進は harness が実ファイル DB への独立接続から
`ojp.clock.set_test_now` で行う（CLI に時刻操作コマンドを追加しない。第14節）。
プロセス待機の timeout は OS の単調時計（`time.monotonic`）を使い、固定 Clock に
依存させない。実時間 sleep で Job 期限を待たない。
"""

from __future__ import annotations

import contextlib
import json
import re
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Iterator

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from ojp import clock, db, scheduler
from ojp.domain import ClockMode

from .report import ObservationRecord, ScenarioReport

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
ROOT_CARD_PATH = FIXTURES_DIR / "poc_root_card.json"

REQUESTER_ID = "pt-requester"
AGENT_A_ID = "pt-agent-a"
AGENT_B_ID = "pt-agent-b"
SYSTEM_ID = "pt-system"

# 終端 Job 状態（第6節）。期限処理の観測条件に使う。
TERMINAL_JOB_STATES = frozenset({"DONE", "FAILED", "EXPIRED"})

# 期限計算は `world.current_clock_us()`（共有 Clock の現在値）を起点に相対で行い、
# harness 側に t0 の値を焼き込まない。

PROCESS_TIMEOUT_SECONDS = 60.0


class HarnessError(AssertionError):
    """harness の期待に反する外部プロセスの挙動。"""


@dataclass(frozen=True)
class CliResult:
    """CLI 子プロセスの実行結果。"""

    argv: list[str]
    returncode: int
    stdout: str
    stderr: str
    payload: dict[str, Any] | None

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and bool(self.payload and self.payload.get("ok"))

    @property
    def data(self) -> dict[str, Any]:
        if self.payload is None or not self.payload.get("ok"):
            raise HarnessError(
                f"CLI command failed: {' '.join(self.argv)}\n"
                f"exit={self.returncode}\nstdout={self.stdout}\nstderr={self.stderr}"
            )
        return self.payload["data"]

    @property
    def operation_id(self) -> str | None:
        if self.payload is None:
            return None
        value = self.payload.get("operation_id")
        return str(value) if value is not None else None

    @property
    def error_code(self) -> str | None:
        if self.payload is None or self.payload.get("ok"):
            return None
        return str(self.payload["error"]["code"])


@dataclass(frozen=True)
class ToolResult:
    """MCP tool 呼び出しの結果封筒。"""

    tool: str
    actor: str
    is_error: bool
    payload: dict[str, Any]

    @property
    def ok(self) -> bool:
        return bool(self.payload.get("ok"))

    @property
    def data(self) -> dict[str, Any]:
        if not self.ok:
            raise HarnessError(
                f"MCP tool {self.tool} (actor={self.actor}) failed: {self.payload}"
            )
        return self.payload["data"]

    @property
    def operation_id(self) -> str | None:
        value = self.payload.get("operation_id")
        return str(value) if value is not None else None

    @property
    def error_code(self) -> str | None:
        if self.ok:
            return None
        return str(self.payload["error"]["code"])


_AMOUNT_RE = re.compile(r"^(-?)(\d+)\.(\d{6})$")


def parse_amount_string(value: str) -> int:
    """6 桁小数文字列を整数マイクロ単位へ変換する（0 と負値も受け付ける）。

    `ojp.domain.parse_amount_units` は業務入力用に 0 と負値を拒否するため、
    観測（残高・累計・非負検査）では harness 側の寛容な変換を使う。
    """
    match = _AMOUNT_RE.match(value)
    if match is None:
        raise HarnessError(f"unexpected amount format: {value!r}")
    sign, whole, frac = match.groups()
    units = int(whole) * 1_000_000 + int(frac)
    return -units if sign else units


def format_amount_units(units: int) -> str:
    """整数マイクロ単位を 6 桁小数文字列へ戻す（負値も表示できる）。"""
    sign = "-" if units < 0 else ""
    whole, frac = divmod(abs(units), 1_000_000)
    return f"{sign}{whole}.{frac:06d}"


# ---------------------------------------------------------------------------
# 台帳スナップショット
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Snapshot:
    """ある観測点での Root 全体の状態。

    `ojp ledger show ROOT --json`（実プロセスの CLI）の出力と、実ファイル DB への
    読み取り専用接続から得た Job 状態・Lease・Submission・Acceptance・
    PaymentOperation・Receipt・Wallet を 1 つの構造体にまとめる。
    金額は整数マイクロ単位で保持し、表示用の 6 桁小数文字列も併せて持つ。
    """

    label: str
    root_id: str
    # ledger show 由来
    deposit_units: int
    escrow_units: int
    available_units: int
    locked_units: int
    locked_breakdown_units: dict[str, int]
    paid_units: int
    refunded_units: int
    accounts: list[dict[str, Any]]
    operations: list[dict[str, Any]]
    conservation_ok_reported: bool
    # DB 由来
    job_states: dict[str, str]
    job_requesters: dict[str, str]
    leases: list[dict[str, Any]]
    submissions: list[dict[str, Any]]
    acceptances: list[dict[str, Any]]
    receipts: list[dict[str, Any]]
    wallet_units: dict[str, int]
    journal: list[dict[str, Any]]

    # -- 導出 -------------------------------------------------------------

    @property
    def paid_by_payee_units(self) -> dict[str, int]:
        """確定 Receipt（kind='payout'）の受取人別累計。"""
        return self._receipt_totals("payout")

    @property
    def refunded_by_payee_units(self) -> dict[str, int]:
        """確定 Receipt（kind='refund'）の受取人別累計。"""
        return self._receipt_totals("refund")

    def _receipt_totals(self, kind: str) -> dict[str, int]:
        totals: dict[str, int] = {}
        for receipt in self.receipts:
            if receipt["kind"] != kind:
                continue
            payee = str(receipt["payee_id"])
            totals[payee] = totals.get(payee, 0) + int(receipt["amount_units"])
        return totals

    def paid_to(self, participant_id: str) -> int:
        return self.paid_by_payee_units.get(participant_id, 0)

    def refunded_to(self, participant_id: str) -> int:
        return self.refunded_by_payee_units.get(participant_id, 0)

    def locked_for(self, bucket: str) -> int:
        return self.locked_breakdown_units.get(bucket, 0)

    def operations_for(self, job_id: str, kind: str) -> list[dict[str, Any]]:
        return [
            op
            for op in self.operations
            if op["job_id"] == job_id and op["kind"] == kind
        ]

    def active_lease_worker(self, job_id: str) -> str | None:
        for lease in self.leases:
            if lease["job_id"] == job_id and lease["closed_reason"] is None:
                return str(lease["worker_id"])
        return None

    def submission_worker(self, job_id: str) -> str | None:
        """その Job の有効 Submission を出した Lease の Worker。"""
        for submission in self.submissions:
            if submission["job_id"] != job_id:
                continue
            lease_id = submission["lease_id"]
            for lease in self.leases:
                if lease["id"] == lease_id:
                    return str(lease["worker_id"])
        return None

    def journal_moves(self, bucket: str) -> list[dict[str, Any]]:
        """指定 bucket の口座に対する Journal 明細（reason 付き）を古い順に返す。

        `available -X / refund +X` のような**内部移動**は、同じ tick round 内で
        送金まで進むと残高スナップショットには現れない。移動が実際に起きたことは
        Journal を正本として照合する（第9節の表）。
        """
        return [entry for entry in self.journal if entry["bucket"] == bucket]

    def journal_total(self, bucket: str, *, reason: str | None = None) -> int:
        """指定 bucket（と reason）の Journal delta 合計。"""
        return sum(
            int(entry["delta_units"])
            for entry in self.journal
            if entry["bucket"] == bucket
            and (reason is None or entry["reason"] == reason)
        )

    def to_amount_strings(self) -> dict[str, str]:
        return {
            "deposit": format_amount_units(self.deposit_units),
            "escrow": format_amount_units(self.escrow_units),
            "available": format_amount_units(self.available_units),
            "locked": format_amount_units(self.locked_units),
            "paid": format_amount_units(self.paid_units),
            "refunded": format_amount_units(self.refunded_units),
        }


# ---------------------------------------------------------------------------
# 不変条件チェッカ
# ---------------------------------------------------------------------------


def check_conservation(snapshot: Snapshot) -> list[str]:
    """D == E + paid + refunded と E == available + locked、全口座非負を検証する。

    違反の説明文一覧を返す（空なら成立）。ledger show 由来の値だけで判定し、
    アプリの conservation フラグに依存しない。
    """
    violations: list[str] = []
    expected = snapshot.escrow_units + snapshot.paid_units + snapshot.refunded_units
    if snapshot.deposit_units != expected:
        violations.append(
            f"D != E + paid + refunded: D={snapshot.deposit_units}"
            f" E={snapshot.escrow_units} paid={snapshot.paid_units}"
            f" refunded={snapshot.refunded_units}"
        )
    if snapshot.escrow_units != snapshot.available_units + snapshot.locked_units:
        violations.append(
            f"E != available + locked: E={snapshot.escrow_units}"
            f" available={snapshot.available_units} locked={snapshot.locked_units}"
        )
    for account in snapshot.accounts:
        units = parse_amount_string(account["amount"])
        if units < 0:
            violations.append(
                f"negative account: {account['account_id']} = {account['amount']}"
            )
    for participant_id, units in sorted(snapshot.wallet_units.items()):
        if units < 0:
            violations.append(f"negative wallet: {participant_id} = {units}")
    return violations


def negative_accounts(snapshot: Snapshot) -> list[str]:
    return [
        str(account["account_id"])
        for account in snapshot.accounts
        if parse_amount_string(account["amount"]) < 0
    ]


def check_payee_entitlement(snapshot: Snapshot) -> list[str]:
    """受取権者との一致を保存則とは独立に検証する。

    受取権者は計画書 第12節どおり、payout なら「当該 Job の有効 Submission を
    出した Lease の Worker」、refund なら「Root Job の Requester」として、
    payment_operations.payee_id / transfer_receipts.payee_id とは無関係に
    leases / submissions / jobs から導出する。導出結果と実際の予約・確定
    受取人が食い違えば違反として報告する（保存則が成立していても検出できる）。
    """
    violations: list[str] = []
    receipts_by_operation = {r["operation_id"]: r for r in snapshot.receipts}
    for operation in snapshot.operations:
        job_id = str(operation["job_id"])
        kind = str(operation["kind"])
        if kind == "payout":
            entitled = snapshot.submission_worker(job_id)
            source = f"lease worker of the valid submission for {job_id}"
        elif kind == "refund":
            entitled = snapshot.job_requesters.get(snapshot.root_id)
            source = f"root requester of {snapshot.root_id}"
        else:
            violations.append(f"unknown payment kind: {kind}")
            continue
        if entitled is None:
            violations.append(
                f"cannot derive entitled payee for operation"
                f" {operation['operation_id']} ({source})"
            )
            continue
        if operation["payee_id"] != entitled:
            violations.append(
                f"reserved payee mismatch for {operation['operation_id']}:"
                f" reserved={operation['payee_id']} entitled={entitled} ({source})"
            )
        receipt = receipts_by_operation.get(operation["operation_id"])
        if receipt is None:
            continue
        if receipt["payee_id"] != entitled:
            violations.append(
                f"receipt payee mismatch for {operation['operation_id']}:"
                f" receipt={receipt['payee_id']} entitled={entitled} ({source})"
            )
        if int(receipt["amount_units"]) != parse_amount_string(operation["amount"]):
            violations.append(
                f"receipt amount mismatch for {operation['operation_id']}:"
                f" receipt={receipt['amount_units']} reserved={operation['amount']}"
            )
    return violations


# ---------------------------------------------------------------------------
# 実プロセス E2E 環境
# ---------------------------------------------------------------------------


@dataclass
class E2EWorld:
    """1 シナリオ分の実プロセス E2E 環境。

    - `project_root`: `--root` に渡すプロジェクトルート（tmp_path 配下）
    - `db_path`: 実ファイル SQLite のパス
    - `report`: 観測結果と operation_id を蓄積するレポート
    """

    project_root: Path
    scenario_id: str
    report: ScenarioReport
    root_id: str | None = None
    child_ids: list[str] = field(default_factory=list)
    snapshots: list[Snapshot] = field(default_factory=list)
    _op_counter: int = 0

    @property
    def db_path(self) -> Path:
        return scheduler.resolve_db_path(self.project_root)

    def next_operation_id(self, kind: str, label: str) -> str:
        """`<kind>:<suffix>` 形式（第16節）でシナリオ内一意な operation_id を採番する。

        `kind` は service が要求する接頭辞（seed / create / fund / claim /
        heartbeat / submit / approve / dispute / abandon / refund など）。
        """
        self._op_counter += 1
        return f"{kind}:{self.scenario_id.lower()}-{self._op_counter:02d}-{label}"

    # -- CLI（実プロセス） -------------------------------------------------

    def _cli_argv(
        self,
        args: list[str],
        *,
        actor: str | None,
        operation_id: str | None,
    ) -> list[str]:
        argv = [
            sys.executable,
            "-m",
            "ojp.cli",
            "--root",
            str(self.project_root),
            "--clock-mode",
            ClockMode.TEST.value,
        ]
        if actor is not None:
            argv += ["--actor", actor]
        if operation_id is not None:
            argv += ["--operation-id", operation_id]
        argv += list(args)
        argv.append("--json")
        return argv

    @staticmethod
    def _parse_stdout_json(stdout: str) -> dict[str, Any] | None:
        text = stdout.strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def run_cli(
        self,
        args: list[str],
        *,
        actor: str | None = None,
        operation_id: str | None = None,
        expect_ok: bool = True,
        action: str | None = None,
    ) -> CliResult:
        """`python -m ojp.cli --root ... --clock-mode test [...] --json` を別プロセスで実行する。

        `cli.main()` の直呼びはしない。stdout の単一 JSON を共通封筒
        （`ok` / `data` / `error`）として構造化して返す。
        """
        argv = self._cli_argv(args, actor=actor, operation_id=operation_id)
        completed = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=PROCESS_TIMEOUT_SECONDS,
        )
        result = CliResult(
            argv=argv,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            payload=self._parse_stdout_json(completed.stdout),
        )
        self.report.add_operation(
            channel="cli",
            actor=actor or SYSTEM_ID,
            action=action or " ".join(args[:2]),
            operation_id=result.operation_id or operation_id,
            ok=result.ok,
        )
        if expect_ok and not result.ok:
            raise HarnessError(
                f"CLI command failed: {' '.join(argv)}\n"
                f"exit={completed.returncode}\n"
                f"stdout={completed.stdout}\nstderr={completed.stderr}"
            )
        return result

    # -- 独立 tick プロセス ------------------------------------------------

    def tick_once(self, *, label: str | None = None) -> dict[str, Any]:
        """`ojp tick --once` を別プロセスで実行し、終了を待って tick 封筒を返す。

        tick の stdout は共通封筒ではなく lifecycle の集計 JSON
        （`counts` / `error`）なので、run_cli とは別に解釈する。
        """
        argv = self._cli_argv(["tick", "--once"], actor=SYSTEM_ID, operation_id=None)
        completed = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=PROCESS_TIMEOUT_SECONDS,
        )
        payload = self._parse_stdout_json(completed.stdout) or {}
        ok = completed.returncode == 0 and payload.get("error") is None
        self.report.add_operation(
            channel="tick",
            actor=SYSTEM_ID,
            action=label or "tick --once",
            operation_id=None,
            ok=ok,
        )
        if not ok:
            raise HarnessError(
                f"tick --once failed: exit={completed.returncode}\n"
                f"stdout={completed.stdout}\nstderr={completed.stderr}"
            )
        return payload

    def pending_payment_count(self) -> int:
        """未確定（PENDING / RETRYABLE）の PaymentOperation 件数を DB から読む。"""
        with self.read_only_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM payment_operations"
                " WHERE root_id = ? AND status <> 'SUCCEEDED'",
                (self.root_id,),
            ).fetchone()
        return int(row["n"])

    def settle(self, *, max_rounds: int = 10) -> int:
        """決済が全て確定するまで独立 tick プロセスを回し、各ラウンド後に観測する。

        決済が複数 commit に分かれる場合に備え、tick の各ラウンド後にスナップショット
        を取って不変条件を検査する。実行したラウンド数を返す。
        """
        for round_no in range(1, max_rounds + 1):
            self.tick_once(label=f"tick --once (settle round {round_no})")
            self.observe(f"after tick round {round_no}")
            if self.pending_payment_count() == 0:
                return round_no
        raise HarnessError(
            f"payments did not settle within {max_rounds} tick rounds"
        )

    def run_tick_until(
        self,
        condition: Callable[[sqlite3.Connection], bool],
        *,
        timeout_seconds: float = 20.0,
        interval_seconds: float = 0.1,
        label: str = "tick --watch",
    ) -> float:
        """`ojp tick --watch` を別プロセスで起動し、DB 上の観測条件が成立するまで待つ。

        待機は OS の単調時計（`time.monotonic`）を基準にし、固定 Clock に依存しない。
        条件成立後に watch プロセスを停止させ、経過秒数を返す。条件が有限 timeout 内に
        成立しなければ HarnessError。
        """
        argv = [
            sys.executable,
            "-m",
            "ojp.cli",
            "--root",
            str(self.project_root),
            "--clock-mode",
            ClockMode.TEST.value,
            "--actor",
            SYSTEM_ID,
            "tick",
            "--watch",
            "--interval",
            str(interval_seconds),
            "--json",
        ]
        started = time.monotonic()
        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = started + timeout_seconds
            while True:
                with self.read_only_connection() as conn:
                    satisfied = condition(conn)
                if satisfied:
                    elapsed = time.monotonic() - started
                    self.report.add_operation(
                        channel="tick",
                        actor=SYSTEM_ID,
                        action=label,
                        operation_id=None,
                        ok=True,
                    )
                    return elapsed
                if process.poll() is not None:
                    out, err = process.communicate()
                    raise HarnessError(
                        "tick --watch exited before the observed condition held:"
                        f" exit={process.returncode}\nstdout={out}\nstderr={err}"
                    )
                if time.monotonic() >= deadline:
                    raise HarnessError(
                        "tick --watch did not reach the observed condition within"
                        f" {timeout_seconds}s"
                    )
                time.sleep(interval_seconds)
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=PROCESS_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=PROCESS_TIMEOUT_SECONDS)
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()

    # -- 共有 Clock --------------------------------------------------------

    def advance_clock_to(self, new_now_us: int) -> bool:
        """実ファイル DB への独立接続で共有 Clock を進め、commit してから戻る。

        時刻更新 transaction に Job 処理を混在させない（第7節）。呼び出し側は
        この関数の戻り後に tick プロセスを起動する。
        """
        conn = db.connect(self.db_path)
        try:
            moved = clock.set_test_now(conn, new_now_us)
        finally:
            conn.close()
        return moved

    def advance_clock_by(self, delta_us: int) -> bool:
        return self.advance_clock_to(self.current_clock_us() + delta_us)

    def current_clock_us(self) -> int:
        with self.read_only_connection() as conn:
            return int(clock.now_for_read_snapshot(conn))

    def lease_expires_at_us(self, lease_id: str) -> int:
        """Lease の expires_at を DB から読む（期限越えの時刻計算に使う）。"""
        with self.read_only_connection() as conn:
            row = conn.execute(
                "SELECT expires_at_us FROM leases WHERE id = ?", (lease_id,)
            ).fetchone()
        if row is None:
            raise HarnessError(f"lease not found: {lease_id}")
        return int(row["expires_at_us"])

    def job_deadline_us(self, job_id: str) -> int:
        """Job の公開 Version の deadline を DB から読む。"""
        with self.read_only_connection() as conn:
            row = conn.execute(
                "SELECT v.deadline_us FROM jobs j"
                " JOIN job_versions v ON v.id = j.version_id"
                " WHERE j.id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            raise HarnessError(f"job or published version not found: {job_id}")
        return int(row["deadline_us"])

    def expire_job(self, job_id: str, *, label: str | None = None) -> None:
        """共有 Clock を Job の Lease 期限・deadline より後へ進め、独立 tick で失効させる。

        時刻更新の commit 完了後に tick プロセスを起動する（実時間 sleep を使わない）。
        観測条件は「その Job が終端状態になること」で、待機は単調時計ベース。
        """
        deadline_us = self.job_deadline_us(job_id)
        target_us = deadline_us + 1_000_000
        with self.read_only_connection() as conn:
            row = conn.execute(
                "SELECT MAX(expires_at_us) AS latest FROM leases WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        if row is not None and row["latest"] is not None:
            target_us = max(target_us, int(row["latest"]) + 1_000_000)
        self.advance_clock_to(target_us)
        self.run_tick_until(
            lambda conn: conn.execute(
                "SELECT state FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()["state"]
            in TERMINAL_JOB_STATES,
            label=label or f"tick --watch (expire {job_id})",
        )

    # -- 読み取り専用スナップショット ---------------------------------------

    @contextlib.contextmanager
    def read_only_connection(self) -> Iterator[sqlite3.Connection]:
        """実ファイル DB への独立接続（harness の観測専用。書込は行わない）。"""
        conn = db.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def snapshot(self, label: str) -> Snapshot:
        """`ojp ledger show ROOT --json`（実プロセス）＋ 読取接続でスナップショットを取る。"""
        if self.root_id is None:
            raise HarnessError("root_id is not set yet; create the root job first")
        ledger_result = self.run_cli(
            ["ledger", "show", self.root_id],
            actor=REQUESTER_ID,
            action=f"ledger show ({label})",
        )
        ledger_data = ledger_result.data
        totals = ledger_data["totals"]

        with self.read_only_connection() as conn:
            job_rows = conn.execute(
                "SELECT id, state, requester_id FROM jobs WHERE root_id = ? ORDER BY id",
                (self.root_id,),
            ).fetchall()
            lease_rows = conn.execute(
                "SELECT l.id, l.job_id, l.worker_id, l.generation, l.expires_at_us,"
                " l.closed_reason FROM leases l JOIN jobs j ON j.id = l.job_id"
                " WHERE j.root_id = ? ORDER BY l.id",
                (self.root_id,),
            ).fetchall()
            submission_rows = conn.execute(
                "SELECT s.id, s.job_id, s.lease_id, s.version_id, s.artifact_json,"
                " s.artifact_hash, s.verification_result, s.valid_at_us,"
                " s.review_due_at_us FROM submissions s JOIN jobs j ON j.id = s.job_id"
                " WHERE j.root_id = ? ORDER BY s.id",
                (self.root_id,),
            ).fetchall()
            acceptance_rows = conn.execute(
                "SELECT a.job_id, a.submission_id, a.decision, a.decided_by,"
                " a.decided_at_us FROM acceptances a JOIN jobs j ON j.id = a.job_id"
                " WHERE j.root_id = ? ORDER BY a.job_id",
                (self.root_id,),
            ).fetchall()
            receipt_rows = conn.execute(
                "SELECT r.receipt_id, r.operation_id, r.amount_units, r.payee_id,"
                " p.kind, p.job_id FROM transfer_receipts r"
                " JOIN payment_operations p ON p.operation_id = r.operation_id"
                " WHERE p.root_id = ? ORDER BY r.receipt_id",
                (self.root_id,),
            ).fetchall()
            wallet_rows = conn.execute(
                "SELECT participant_id, balance_units FROM mock_wallets"
                " ORDER BY participant_id"
            ).fetchall()
            journal_rows = conn.execute(
                "SELECT t.operation_id, t.reason, t.created_at_us, e.entry_no,"
                " e.delta_units, a.bucket, a.owner_job_id, a.beneficiary_id"
                " FROM journal_entries e"
                " JOIN journal_transactions t ON t.operation_id = e.operation_id"
                " JOIN budget_accounts a ON a.id = e.account_id"
                " WHERE a.root_id = ?"
                " ORDER BY t.created_at_us ASC, t.operation_id ASC, e.entry_no ASC",
                (self.root_id,),
            ).fetchall()

        return Snapshot(
            label=label,
            root_id=self.root_id,
            deposit_units=parse_amount_string(totals["deposit"]),
            escrow_units=parse_amount_string(totals["escrow"]),
            available_units=parse_amount_string(totals["available"]),
            locked_units=parse_amount_string(totals["locked"]),
            locked_breakdown_units={
                bucket: parse_amount_string(value)
                for bucket, value in ledger_data["locked_breakdown"].items()
            },
            paid_units=parse_amount_string(totals["paid"]),
            refunded_units=parse_amount_string(totals["refunded"]),
            accounts=[dict(a) for a in ledger_data["accounts"]],
            operations=[dict(o) for o in ledger_data["operations"]],
            conservation_ok_reported=bool(ledger_data["conservation"]["ok"]),
            job_states={str(r["id"]): str(r["state"]) for r in job_rows},
            job_requesters={str(r["id"]): str(r["requester_id"]) for r in job_rows},
            leases=[dict(r) for r in lease_rows],
            submissions=[dict(r) for r in submission_rows],
            acceptances=[dict(r) for r in acceptance_rows],
            receipts=[dict(r) for r in receipt_rows],
            wallet_units={
                str(r["participant_id"]): int(r["balance_units"]) for r in wallet_rows
            },
            journal=[dict(r) for r in journal_rows],
        )

    # -- commit ごとの観測 -------------------------------------------------

    def observe(self, label: str) -> Snapshot:
        """スナップショットを取り、不変条件を検査してレポートへ履歴として蓄積する。

        各書込操作の完了後（および tick の各ラウンド後）に呼ぶ。保存則・非負・
        受取権者一致のいずれかが破れていれば即座に失敗させる。
        """
        snap = self.snapshot(label)
        conservation_violations = check_conservation(snap)
        entitlement_violations = check_payee_entitlement(snap)
        negatives = negative_accounts(snap)
        self.report.add_observation(
            ObservationRecord(
                label=label,
                totals=snap.to_amount_strings(),
                locked_breakdown={
                    bucket: format_amount_units(units)
                    for bucket, units in sorted(snap.locked_breakdown_units.items())
                },
                conservation_ok=not conservation_violations,
                accounts_non_negative=not negatives,
                negative_accounts=negatives,
                payee_entitlement_ok=not entitlement_violations,
                payee_entitlement_violations=entitlement_violations,
                job_states=dict(snap.job_states),
                paid_by_payee={
                    payee: format_amount_units(units)
                    for payee, units in sorted(snap.paid_by_payee_units.items())
                },
                refunded_by_payee={
                    payee: format_amount_units(units)
                    for payee, units in sorted(snap.refunded_by_payee_units.items())
                },
            )
        )
        self.snapshots.append(snap)
        if conservation_violations:
            raise HarnessError(
                f"conservation violated at {label!r}: {conservation_violations}"
            )
        if entitlement_violations:
            raise HarnessError(
                f"payee entitlement violated at {label!r}: {entitlement_violations}"
            )
        return snap


# ---------------------------------------------------------------------------
# MCP stdio クライアント（A 役・B 役）
# ---------------------------------------------------------------------------


class AgentSession:
    """A/B 役の MCP stdio セッション。8 tools だけを呼ぶ。

    stdout は JSON-RPC 専用なので、サーバープロセスの stderr は別ファイルへ
    捕捉し、失敗時の診断に使う（`stderr_text()`）。
    """

    def __init__(
        self,
        world: E2EWorld,
        actor: str,
        session: ClientSession,
        stderr_path: Path,
    ) -> None:
        self.world = world
        self.actor = actor
        self._session = session
        self._stderr_path = stderr_path

    def stderr_text(self) -> str:
        if not self._stderr_path.exists():
            return ""
        return self._stderr_path.read_text(encoding="utf-8", errors="replace")

    async def list_tool_names(self) -> list[str]:
        tools = await self._session.list_tools()
        return sorted(tool.name for tool in tools.tools)

    async def call(
        self,
        tool: str,
        arguments: dict[str, Any],
        *,
        expect_ok: bool = True,
    ) -> ToolResult:
        """MCP tool を呼び、`ok` / `data` / `error` の封筒を取り出して返す。"""
        raw = await self._session.call_tool(tool, arguments)
        payload = _parse_tool_envelope(raw, tool=tool, stderr=self.stderr_text())
        result = ToolResult(
            tool=tool,
            actor=self.actor,
            is_error=bool(getattr(raw, "isError", False)),
            payload=payload,
        )
        self.world.report.add_operation(
            channel="mcp",
            actor=self.actor,
            action=tool,
            operation_id=result.operation_id or arguments.get("operation_id"),
            ok=result.ok,
        )
        if expect_ok and not result.ok:
            raise HarnessError(
                f"MCP tool {tool} (actor={self.actor}) failed: {payload}\n"
                f"server stderr:\n{self.stderr_text()}"
            )
        return result


def _parse_tool_envelope(raw: Any, *, tool: str, stderr: str) -> dict[str, Any]:
    """ToolResult の content から共通封筒（`ok` / `data` / `error`）を取り出す。"""
    if not raw.content:
        raise HarnessError(
            f"MCP tool {tool} returned empty content\nserver stderr:\n{stderr}"
        )
    text = raw.content[0].text
    # SDK 2.x の ToolError ラッパー 'Error executing tool <name>: <payload>'
    if ": " in text and not text.strip().startswith("{"):
        _, text = text.split(": ", 1)
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise HarnessError(
            f"MCP tool {tool} returned non-JSON content: {text!r}\n"
            f"server stderr:\n{stderr}"
        ) from exc


@contextlib.asynccontextmanager
async def agent_session(world: E2EWorld, actor: str) -> AsyncIterator[AgentSession]:
    """`ojp mcp --actor <ACTOR>` を stdio サーバーとして実プロセス起動して接続する。

    A 役・B 役はそれぞれ別のサーバープロセスになる（この関数を役ごとに呼ぶ）。
    """
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "ojp.cli",
            "--root",
            str(world.project_root),
            "--clock-mode",
            ClockMode.TEST.value,
            "mcp",
            "--actor",
            actor,
        ],
    )
    stderr_path = world.project_root / f"mcp-{actor}.stderr.log"
    with stderr_path.open("w", encoding="utf-8") as errlog:
        async with stdio_client(params, errlog=errlog) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield AgentSession(world, actor, session, stderr_path)


# ---------------------------------------------------------------------------
# 環境構築
# ---------------------------------------------------------------------------


def create_world(tmp_path: Path, scenario_id: str) -> E2EWorld:
    """tmp_path 配下に project root を作り、test mode の実ファイル DB を初期化する。

    `demo init` は実プロセスの CLI で実行する。
    """
    project_root = tmp_path / f"ojp-{scenario_id.lower()}"
    project_root.mkdir(parents=True, exist_ok=True)
    world = E2EWorld(
        project_root=project_root,
        scenario_id=scenario_id,
        report=ScenarioReport(scenario_id=scenario_id),
    )
    world.run_cli(
        ["demo", "init"],
        actor=SYSTEM_ID,
        operation_id=world.next_operation_id("seed", "demo-init"),
        action="demo init",
    )
    if not world.db_path.exists():
        raise HarnessError(f"database file was not created: {world.db_path}")
    return world


def create_and_fund_root(
    world: E2EWorld,
    *,
    amount: str = "100.000000",
    card_path: Path = ROOT_CARD_PATH,
) -> tuple[str, str]:
    """Requester が CLI で Root を作成し、全額入金して OPEN にする。

    戻り値は (root_id, published_version_id)。
    """
    create = world.run_cli(
        ["job", "create", "--card", str(card_path)],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("create", "root"),
        action="job create",
    )
    root_id = str(create.data["job_id"])
    version_id = str(create.data["version_id"])
    world.root_id = root_id

    world.run_cli(
        ["job", "fund", root_id, "--amount", amount],
        actor=REQUESTER_ID,
        operation_id=world.next_operation_id("fund", "root"),
        action="job fund",
    )
    return root_id, version_id


# ---------------------------------------------------------------------------
# 4結果シナリオ共通の開始条件（第18節「各シナリオは Root 入金 100、A の
# part-1 への Child 発注 10 から始める」）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScenarioStart:
    """共通の開始条件を確立した後の識別子。"""

    root_id: str
    root_version_id: str
    root_lease_id: str
    child_id: str
    child_version_id: str
    child_lease_id: str


async def start_root_child_part1(
    world: E2EWorld,
    agent_a: AgentSession,
    agent_b: AgentSession,
    *,
    child_budget: str = "10.000000",
    child_deadline: str = "2027-01-16T00:00:00.000000Z",
    observe: bool = True,
) -> ScenarioStart:
    """Root 入金 100 →（A が MCP で）Root Claim・part-1 Child 10 作成 →（B が MCP で）
    Child Claim までを実行する。シナリオ固有の期待値は含まない。

    Root の作成・入金は呼出側が済ませている前提（`create_and_fund_root`）。
    """
    if world.root_id is None:
        raise HarnessError("root job must be created and funded first")
    root_id = world.root_id
    with world.read_only_connection() as conn:
        row = conn.execute(
            "SELECT version_id FROM jobs WHERE id = ?", (root_id,)
        ).fetchone()
    root_version_id = str(row["version_id"])

    claim_root = await agent_a.call(
        "ojp_claim_job",
        {
            "job_id": root_id,
            "expected_version_id": root_version_id,
            "operation_id": world.next_operation_id("claim", "a-root"),
        },
    )
    root_lease_id = str(claim_root.data["lease_id"])
    if observe:
        world.observe("after root claim by A")

    create_child = await agent_a.call(
        "ojp_create_child_job",
        {
            "parent_job_id": root_id,
            "lease_id": root_lease_id,
            "task_key": "part-1",
            "budget": child_budget,
            "deadline": child_deadline,
            "operation_id": world.next_operation_id("create", "a-child"),
        },
    )
    child_id = str(create_child.data["child_id"])
    child_version_id = str(create_child.data["version_id"])
    world.child_ids.append(child_id)
    if observe:
        world.observe("after child create")

    claim_child = await agent_b.call(
        "ojp_claim_job",
        {
            "job_id": child_id,
            "expected_version_id": child_version_id,
            "operation_id": world.next_operation_id("claim", "b-child"),
        },
    )
    child_lease_id = str(claim_child.data["lease_id"])
    if observe:
        world.observe("after child claim by B")

    return ScenarioStart(
        root_id=root_id,
        root_version_id=root_version_id,
        root_lease_id=root_lease_id,
        child_id=child_id,
        child_version_id=child_version_id,
        child_lease_id=child_lease_id,
    )


def write_artifact(world: E2EWorld, name: str, payload: dict[str, Any]) -> Path:
    """CLI の `--artifact` に渡す成果物 JSON ファイルを書き出す。"""
    path = world.project_root / f"artifact-{name}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path
