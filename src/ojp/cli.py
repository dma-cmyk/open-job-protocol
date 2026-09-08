"""Open Job Protocol (OJP) CLI.

PoC 計画書 第14節（CLI設計）・第5節（Actorと成果物への権限）。
--actor オプションによる Actor 指定は信頼済みローカル操作者用（開発・テスト・検証用）
であり、本番環境の暗号学的認証や安全な権限分離を意味するものではありません。
本番認証・実資金・外部Facilitator連携はPoCの境界外です。
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pydantic

from . import clock, db, domain, ledger, response, scheduler, service, verification
from .domain import ErrorCode, OjpError

_ISO_TIMESTAMP_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?Z$"
)


DEFAULT_TEST_NOW_US = 1_800_000_000_000_000  # 2027-01-15T08:00:00Z (test mode 初期時刻)

def parse_timestamp_iso(value: str) -> int:
    """ISO-8601 UTC 文字列（末尾 Z）を整数マイクロ秒へ変換する。

    float を経由せず、service.format_timestamp_us の完全な逆変換となる。
    形式不正や無効な暦日・負のタイムスタンプは INVALID_ARGUMENT で拒否する。
    """
    if isinstance(value, bool) or not isinstance(value, str):
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"timestamp must be a string, got {type(value).__name__}",
        )
    m = _ISO_TIMESTAMP_RE.match(value)
    if not m:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"timestamp must be ISO-8601 UTC string like 'YYYY-MM-DDTHH:MM:SSZ' or 'YYYY-MM-DDTHH:MM:SS.ffffffZ': {value!r}",
        )
    year, month, day, hour, minute, second, frac_str = m.groups()
    try:
        d = dt.datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            int(second),
            tzinfo=dt.timezone.utc,
        )
    except ValueError as exc:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"invalid calendar date/time in timestamp: {value!r}",
        ) from exc
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    delta = d - epoch
    total_seconds = delta.days * 86400 + delta.seconds
    if total_seconds < 0:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"timestamp must be on or after 1970-01-01T00:00:00Z: {value!r}",
        )
    frac_us = int(frac_str.ljust(6, "0")) if frac_str else 0
    return total_seconds * 1_000_000 + frac_us


def load_job_card(card_path: Path) -> dict[str, Any]:
    """Root Job 作成用のカードファイルを読み込み、検証済みのパラメータ dict を返す。"""
    try:
        raw_text = card_path.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError) as exc:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"card file not found or inaccessible: {card_path}",
        ) from exc
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"invalid JSON in card file: {exc}",
        ) from exc

    if not isinstance(data, dict):
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            "card JSON must be an object",
        )

    try:
        title = data["title"]
        budget = data["budget"]
        deadline_str = data["deadline"]
        deadline_us = parse_timestamp_iso(deadline_str)

        raw_sub = data["subcontract_policy"]
        max_amount_str = raw_sub.get("max_amount")
        max_amount_units = domain.parse_amount_units(max_amount_str)
        subcontract_policy = domain.SubcontractPolicy(
            enabled=raw_sub["enabled"],
            max_amount_units=max_amount_units,
            max_ratio_bps=raw_sub["max_ratio_bps"],
            max_children=raw_sub["max_children"],
            max_depth=raw_sub["max_depth"],
        )

        timing_policy = (
            domain.TimingPolicy.model_validate(data["timing_policy"])
            if data.get("timing_policy") is not None
            else None
        )
        artifact_access_policy = (
            domain.ArtifactAccessPolicy.model_validate(data["artifact_access_policy"])
            if data.get("artifact_access_policy") is not None
            else None
        )

        root_def = domain.RootTaskDefinition.model_validate(data["root"])

        raw_catalog = data.get("catalog", [])
        task_catalog: list[domain.TaskCatalogEntry] = []
        for e in raw_catalog:
            cap_units = domain.parse_amount_units(e["budget_cap"])
            tp = (
                domain.TimingPolicy.model_validate(e["timing_policy"])
                if e.get("timing_policy") is not None
                else None
            )
            task_catalog.append(
                domain.TaskCatalogEntry(
                    task_key=e["task_key"],
                    input_values=e["input_values"],
                    expected=e["expected"],
                    budget_cap_units=cap_units,
                    timing_policy=tp,
                )
            )

        input_json = ledger.canonical_json_dumps(root_def.input_values)
        conditions_json = ledger.canonical_json_dumps(root_def.expected)
        verifier_id = verification.VERIFIER_ID
        verifier_hash = verification.verifier_hash()

        return {
            "title": title,
            "budget": budget,
            "deadline_us": deadline_us,
            "subcontract_policy": subcontract_policy,
            "task_catalog": task_catalog,
            "timing_policy": timing_policy,
            "artifact_access_policy": artifact_access_policy,
            "input_json": input_json,
            "conditions_json": conditions_json,
            "verifier_id": verifier_id,
            "verifier_hash": verifier_hash,
        }
    except (KeyError, TypeError, ValueError, pydantic.ValidationError) as exc:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"invalid card definition: {exc}",
        ) from exc


def _build_parser() -> argparse.ArgumentParser:
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "--root",
        default=argparse.SUPPRESS,
        help="プロジェクトルートのパス（必須。data/ojp.sqlite3 を解決する）",
    )
    common_parser.add_argument(
        "--actor",
        default=argparse.SUPPRESS,
        help="信頼済みローカル操作者の Participant ID（本番認証ではありません。既定: pt-system）",
    )
    common_parser.add_argument(
        "--json",
        action="store_true",
        default=argparse.SUPPRESS,
        help="結果を単一の JSON で stdout へ出力する",
    )
    common_parser.add_argument(
        "--clock-mode",
        choices=[m.value for m in domain.ClockMode],
        default=argparse.SUPPRESS,
        help="起動時に期待する DB の Clock mode（既定: realtime）。不一致なら MODE_MISMATCH",
    )
    common_parser.add_argument(
        "--operation-id",
        default=argparse.SUPPRESS,
        help="操作の冪等性キー（省略時は service が自動採番して結果へ返す）",
    )

    write_parser = argparse.ArgumentParser(add_help=False)

    parser = argparse.ArgumentParser(
        prog="ojp",
        description="Open Job Protocol (OJP) PoC CLI",
        parents=[common_parser],
    )
    subparsers = parser.add_subparsers(dest="group", required=True)

    # 1. demo init
    demo_parser = subparsers.add_parser("demo", parents=[common_parser])
    demo_subs = demo_parser.add_subparsers(dest="subcommand", required=True)
    demo_init = demo_subs.add_parser("init", parents=[common_parser, write_parser])
    demo_init.add_argument(
        "--seed",
        default="5000.000000",
        help="Requester の seed 額（6桁小数文字列。既定: 5000.000000）",
    )

    # 2. job
    job_parser = subparsers.add_parser("job", parents=[common_parser])
    job_subs = job_parser.add_subparsers(dest="subcommand", required=True)

    # job create
    job_create = job_subs.add_parser("create", parents=[common_parser, write_parser])
    job_create.add_argument(
        "--card",
        required=True,
        help="Root Job 定義 JSON ファイルのパス",
    )

    # job fund
    job_fund = job_subs.add_parser("fund", parents=[common_parser, write_parser])
    job_fund.add_argument("job", help="入金対象の Root Job ID")
    job_fund.add_argument(
        "--amount",
        required=True,
        help="入金額（6桁小数文字列）",
    )

    # job list
    job_list = job_subs.add_parser("list", parents=[common_parser])
    job_list.add_argument(
        "--state",
        choices=[s.value for s in domain.JobState],
        help="絞り込み対象の Job 状態",
    )
    job_list.add_argument("--parent", help="親 Job ID による絞り込み")
    job_list.add_argument("--limit", type=int, default=20, help="取得件数上限（1..100。既定: 20）")
    job_list.add_argument("--cursor", help="ページネーションカーソル")

    # job get
    job_get = job_subs.add_parser("get", parents=[common_parser])
    job_get.add_argument("job", help="取得対象の Job ID")

    # job claim
    job_claim = job_subs.add_parser("claim", parents=[common_parser, write_parser])
    job_claim.add_argument("job", help="Claim 対象の Job ID")
    job_claim.add_argument(
        "--version",
        required=True,
        help="期待する公開 JobVersion ID",
    )

    # job heartbeat
    job_heartbeat = job_subs.add_parser("heartbeat", parents=[common_parser, write_parser])
    job_heartbeat.add_argument("job", help="heartbeat 対象の Job ID")
    job_heartbeat.add_argument(
        "--lease",
        required=True,
        help="有効 Lease ID",
    )
    job_heartbeat.add_argument(
        "--generation",
        type=int,
        default=1,
        help="Lease の generation（既定: 1）",
    )

    # job submit
    job_submit = job_subs.add_parser("submit", parents=[common_parser, write_parser])
    job_submit.add_argument("job", help="提出対象の Job ID")
    job_submit.add_argument(
        "--lease",
        required=True,
        help="有効 Lease ID",
    )
    job_submit.add_argument(
        "--version",
        required=True,
        help="対象 JobVersion ID",
    )
    job_submit.add_argument(
        "--artifact",
        required=True,
        help="成果物 JSON ファイルのパス",
    )

    # job approve
    job_approve = job_subs.add_parser("approve", parents=[common_parser, write_parser])
    job_approve.add_argument("job", help="承認対象の Job ID")
    job_approve.add_argument(
        "--submission",
        required=True,
        help="承認対象の Submission ID",
    )

    # job dispute
    job_dispute = job_subs.add_parser("dispute", parents=[common_parser, write_parser])
    job_dispute.add_argument("job", help="異議対象の Job ID")
    job_dispute.add_argument(
        "--submission",
        required=True,
        help="異議対象の Submission ID",
    )
    job_dispute.add_argument(
        "--condition",
        required=True,
        help="不適合の対象条件 ID（例: sum）",
    )
    job_dispute.add_argument(
        "--reason",
        required=True,
        help="異議理由コード（例: CONDITION_MISMATCH）",
    )
    job_dispute.add_argument(
        "--evidence",
        help="証跡ファイルのパス（任意）",
    )

    # job abandon
    job_abandon = job_subs.add_parser("abandon", parents=[common_parser, write_parser])
    job_abandon.add_argument("job", help="放棄対象の Job ID")
    job_abandon.add_argument(
        "--lease",
        required=True,
        help="放棄する有効 Lease ID",
    )

    # job refund
    job_refund = job_subs.add_parser("refund", parents=[common_parser, write_parser])
    job_refund.add_argument("job", help="返金対象の Root Job ID")

    # 3. child create
    child_parser = subparsers.add_parser("child", parents=[common_parser])
    child_subs = child_parser.add_subparsers(dest="subcommand", required=True)
    child_create = child_subs.add_parser("create", parents=[common_parser, write_parser])
    child_create.add_argument("parent", help="親 Job ID（Root Job ID）")
    child_create.add_argument(
        "--lease",
        required=True,
        help="親 Job の有効 Lease ID",
    )
    child_create.add_argument(
        "--task",
        required=True,
        help="カタログ内のタスクキー（例: part-1）",
    )
    child_create.add_argument(
        "--budget",
        required=True,
        help="Child 予算（6桁小数文字列。例: 10.000000）",
    )
    child_create.add_argument(
        "--deadline",
        required=True,
        help="Child 期限（ISO-8601 UTC 文字列。例: 2027-01-16T00:00:00.000000Z）",
    )

    # 4. ledger show
    ledger_parser = subparsers.add_parser("ledger", parents=[common_parser])
    ledger_subs = ledger_parser.add_subparsers(dest="subcommand", required=True)
    ledger_show = ledger_subs.add_parser("show", parents=[common_parser])
    ledger_show.add_argument("job", help="台帳を表示する Root Job ID")

    # 5. payment retry
    payment_parser = subparsers.add_parser("payment", parents=[common_parser])
    payment_subs = payment_parser.add_subparsers(dest="subcommand", required=True)
    payment_retry = payment_subs.add_parser("retry", parents=[common_parser])
    payment_retry.add_argument("operation", help="再試行する Payment Operation ID")

    # 6. tick
    tick_parser = subparsers.add_parser("tick", parents=[common_parser])
    tick_mode = tick_parser.add_mutually_exclusive_group(required=True)
    tick_mode.add_argument("--once", action="store_true", help="tick を 1 回だけ実行する")
    tick_mode.add_argument("--watch", action="store_true", help="tick を一定間隔で繰り返す")
    tick_parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="--watch の tick 間隔（秒。既定: 1.0）",
    )

    # 7. mcp
    subparsers.add_parser("mcp", parents=[common_parser])

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point。終了コード: 成功 0 / 違反 2 / 一時障害 3。"""
    parser = _build_parser()
    args = parser.parse_args(argv)

    # 共通フラグのデフォルト適用と必須検査
    if not hasattr(args, "root"):
        parser.error("the following arguments are required: --root")
    if not hasattr(args, "actor"):
        args.actor = scheduler.DEFAULT_ACTOR_ID
    if not hasattr(args, "json"):
        args.json = False
    if not hasattr(args, "clock_mode"):
        args.clock_mode = domain.ClockMode.REALTIME.value
    if not hasattr(args, "operation_id"):
        args.operation_id = None

    root = Path(args.root).expanduser()
    db_path = scheduler.resolve_db_path(root)

    # tick サブコマンドは scheduler.main へ委譲する（第14節）
    if args.group == "tick":
        scheduler_argv = [
            "--root",
            str(root),
            "--actor",
            args.actor,
            "--clock-mode",
            args.clock_mode,
            "--interval",
            str(args.interval),
        ]
        if getattr(args, "once", False):
            scheduler_argv.append("--once")
        elif getattr(args, "watch", False):
            scheduler_argv.append("--watch")
        if args.json:
            scheduler_argv.append("--json")
        return scheduler.main(scheduler_argv)

    # mcp サブコマンド: MCP stdio サーバーを起動する（第13節・第14節）
    if args.group == "mcp":
        from . import mcp_server

        return mcp_server.main(
            actor_id=args.actor,
            root=root,
            clock_mode=args.clock_mode,
        )
    conn: sqlite3.Connection | None = None
    try:
        # DB ファイルの存在確認と接続
        if args.group == "demo" and args.subcommand == "init":
            if not db_path.exists():
                db_path.parent.mkdir(parents=True, exist_ok=True)
                mode = domain.ClockMode(args.clock_mode)
                test_now = DEFAULT_TEST_NOW_US if mode == domain.ClockMode.TEST else None
                conn = clock.initialize_database(db_path, mode, test_now_us=test_now)
            else:
                conn = db.connect(db_path)
        else:
            # それ以外のコマンドは DB を新規作成せず、未存在なら INVALID_STATE で exit 2
            if not db_path.exists():
                raise OjpError(
                    ErrorCode.INVALID_STATE,
                    f"database file not found: {db_path}. run 'ojp demo init' first",
                )
            conn = db.connect(db_path)

        # 起動設定と DB の Clock mode を照合
        clock.assert_mode(conn, domain.ClockMode(args.clock_mode))

        # サブコマンドのディスパッチ
        result: Any = None
        is_query = False

        if args.group == "demo" and args.subcommand == "init":
            seed_units = domain.parse_amount_units(args.seed)
            result = service.demo_init(
                conn,
                actor_id=args.actor,
                seed_units=seed_units,
                operation_id=args.operation_id,
            )

        elif args.group == "job" and args.subcommand == "create":
            card_data = load_job_card(Path(args.card))
            result = service.create_root(
                conn,
                actor_id=args.actor,
                title=card_data["title"],
                budget=card_data["budget"],
                deadline_us=card_data["deadline_us"],
                subcontract_policy=card_data["subcontract_policy"],
                task_catalog=card_data["task_catalog"],
                timing_policy=card_data["timing_policy"],
                artifact_access_policy=card_data["artifact_access_policy"],
                input_json=card_data["input_json"],
                verifier_id=card_data["verifier_id"],
                verifier_hash=card_data["verifier_hash"],
                conditions_json=card_data["conditions_json"],
                operation_id=args.operation_id,
            )

        elif args.group == "job" and args.subcommand == "fund":
            amount_units = domain.parse_amount_units(args.amount)
            result = service.fund_root(
                conn,
                actor_id=args.actor,
                root_id=args.job,
                requester_id=args.actor,
                expected_amount_units=amount_units,
                amount_units=amount_units,
                operation_id=args.operation_id,
            )

        elif args.group == "job" and args.subcommand == "list":
            is_query = True
            st = domain.JobState(args.state) if args.state else None
            result = service.list_jobs(
                conn,
                actor_id=args.actor,
                state=st,
                parent_id=args.parent,
                limit=args.limit,
                cursor=args.cursor,
            )

        elif args.group == "job" and args.subcommand == "get":
            is_query = True
            result = service.get_job(conn, actor_id=args.actor, job_id=args.job)

        elif args.group == "job" and args.subcommand == "claim":
            result = service.claim(
                conn,
                actor_id=args.actor,
                job_id=args.job,
                expected_version_id=args.version,
                operation_id=args.operation_id,
            )

        elif args.group == "job" and args.subcommand == "heartbeat":
            result = service.heartbeat(
                conn,
                actor_id=args.actor,
                job_id=args.job,
                lease_id=args.lease,
                generation=args.generation,
                operation_id=args.operation_id,
            )

        elif args.group == "child" and args.subcommand == "create":
            budget_units = domain.parse_amount_units(args.budget)
            deadline_us = parse_timestamp_iso(args.deadline)
            result = service.create_child(
                conn,
                actor_id=args.actor,
                parent_job_id=args.parent,
                lease_id=args.lease,
                task_key=args.task,
                budget_units=budget_units,
                deadline_us=deadline_us,
                operation_id=args.operation_id,
            )

        elif args.group == "job" and args.subcommand == "submit":
            try:
                artifact_json = Path(args.artifact).read_text(encoding="utf-8")
            except (FileNotFoundError, OSError) as exc:
                raise OjpError(
                    ErrorCode.INVALID_ARGUMENT,
                    f"artifact file not found: {args.artifact}",
                ) from exc
            result = service.submit(
                conn,
                actor_id=args.actor,
                job_id=args.job,
                lease_id=args.lease,
                version_id=args.version,
                artifact_json=artifact_json,
                operation_id=args.operation_id,
            )

        elif args.group == "job" and args.subcommand == "approve":
            result = service.approve(
                conn,
                actor_id=args.actor,
                job_id=args.job,
                submission_id=args.submission,
                operation_id=args.operation_id,
            )

        elif args.group == "job" and args.subcommand == "dispute":
            evidence = None
            if args.evidence:
                try:
                    evidence = Path(args.evidence).read_text(encoding="utf-8")
                except (FileNotFoundError, OSError) as exc:
                    raise OjpError(
                        ErrorCode.INVALID_ARGUMENT,
                        f"evidence file not found: {args.evidence}",
                    ) from exc
            result = service.dispute(
                conn,
                actor_id=args.actor,
                job_id=args.job,
                submission_id=args.submission,
                condition_id=args.condition,
                reason_code=args.reason,
                evidence=evidence,
                operation_id=args.operation_id,
            )

        elif args.group == "job" and args.subcommand == "abandon":
            result = service.abandon(
                conn,
                actor_id=args.actor,
                job_id=args.job,
                lease_id=args.lease,
                operation_id=args.operation_id,
            )

        elif args.group == "job" and args.subcommand == "refund":
            result = service.refund(
                conn,
                actor_id=args.actor,
                job_id=args.job,
                operation_id=args.operation_id,
            )

        elif args.group == "ledger" and args.subcommand == "show":
            is_query = True
            result = service.get_ledger(
                conn,
                actor_id=args.actor,
                root_id=args.job,
            )

        elif args.group == "payment" and args.subcommand == "retry":
            result = service.retry_payment(
                conn,
                actor_id=args.actor,
                operation_id=args.operation,
            )

        # 応答の構築
        if is_query:
            payload = response.success(result)
        else:
            # 書込コマンド（CommandResult）
            payload = response.success(
                result.data,
                operation_id=result.operation_id,
                replayed=result.replayed,
            )

        if args.json:
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        else:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        return response.EXIT_OK

    except OjpError as exc:
        if args.json:
            print(
                json.dumps(
                    response.failure(exc),
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
        else:
            print(f"Error ({exc.code}): {exc.message}", file=sys.stderr)
        return response.exit_code(exc.code)

    except sqlite3.Error as exc:
        is_busy = db.is_db_busy(exc)
        payload = response.db_error_payload(exc, busy=is_busy)
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        else:
            print(f"Database error ({payload['error']['code']}): {exc}", file=sys.stderr)
        return response.exit_code(payload["error"]["code"])

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
