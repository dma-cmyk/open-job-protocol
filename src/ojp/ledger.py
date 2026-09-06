"""BudgetAccount・Journal・MockWallet の台帳操作（計画書 第9節・第16節）。

口座が正本。集計表示（D / E / available / locked 内訳 / paid / refunded）は
口座から導出し、親集計と子口座を足し直さない。全口座は非負で、負にする移動は
SQLite の CHECK 制約違反をドメインエラーへ変換して拒否する。残高更新と
Journal（JournalTransaction / JournalEntry）は同一 transaction で確定する。

MockWallet との出入り（Root funding、将来の送金）も Journal に載せ、
wallet_ledger_accounts（migration 002）で Wallet の台帳上の相手口座を表す。
これにより MockWallet の入金前残高を含めた全口座間の Journal 合計が 0 になる。

このモジュールの関数は transaction を所有しない。service 層が BEGIN IMMEDIATE で
開始した transaction（接続）と、clock.now_for_write_transaction で1回だけ採取した
now を受け取る。下位関数は SQLite の制約違反を OjpError へ変換して返す。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from . import clock
from .domain import (
    ASSET_MOCK_USDC,
    LOCKED_BUCKETS,
    Bucket,
    BudgetAccount,
    ClockMode,
    ErrorCode,
    OjpError,
    PaymentKind,
    PaymentOperation,
    PaymentStatus,
    TransferReceipt,
)

# wallet_ledger_accounts に対応する budget_accounts 行を識別する source_key。
# Job 資金の bucket 値・business_key と衝突しない専用の名前空間を使う。
WALLET_LEDGER_SOURCE_KEY = "wallet-ledger"

_OPERATION_ID_KINDS = ("fund", "allocate", "return", "reserve", "seed", "payout", "refund")


def new_operation_id(kind: str) -> str:
    """CLI 未指定時と同じ形の operation_id を生成する（`<kind>:<uuid4>`）。"""
    if kind not in _OPERATION_ID_KINDS:
        raise OjpError(ErrorCode.INVALID_ARGUMENT, f"unknown operation kind: {kind!r}")
    return f"{kind}:{uuid.uuid4()}"


def canonical_json_dumps(value: Any) -> str:
    """payload_hash 用の canonical JSON。

    規則を固定する: キーは Unicode コードポイント順（sort_keys=True）、
    区切りは `,` / `:`（空白なし）、非 ASCII はエスケープしない（ensure_ascii=False）、
    UTF-8 エンコード前提の文字列として扱う。dict/list/str/int/float/bool/None
    以外の値は拒否し、同じ payload は常に同じ文字列になる。
    """
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT, f"payload must be canonical JSON: {exc}"
        ) from exc


def account_ref(
    root_id: str,
    owner_job_id: str,
    bucket: Bucket,
    beneficiary_id: str | None = None,
    source_key: str | None = None,
) -> str:
    """口座の自然キーから口座 id を導出する。

    冪等再試行が「同じ口座」を確定的に指せるよう、id は自然キーから生成する
    （ランダム id では再試行のたびに別口座になり得る）。
    """
    natural = [root_id, owner_job_id, bucket.value, beneficiary_id, source_key]
    return "acct:" + canonical_json_dumps(natural)


def _get_required_account(conn: sqlite3.Connection, account_id: str) -> BudgetAccount:
    row = conn.execute(
        "SELECT * FROM budget_accounts WHERE id = ?", (account_id,)
    ).fetchone()
    if row is None:
        raise OjpError(ErrorCode.INVALID_STATE, f"budget account not found: {account_id}")
    return BudgetAccount.model_validate(dict(row))


def get_or_create_account(
    conn: sqlite3.Connection,
    *,
    root_id: str,
    owner_job_id: str,
    bucket: Bucket,
    beneficiary_id: str | None = None,
    source_key: str | None = None,
    purpose: str | None = None,
) -> BudgetAccount:
    """口座の自然キーで取得し、無ければ残高 0 で作成する（冪等）。

    一意性は migration 002 の COALESCE 一意 INDEX が NULL 列込みで実効化する。
    同時実行では先に commit した側が作り、後続は既存行を読む。
    """
    account_id = account_ref(root_id, owner_job_id, bucket, beneficiary_id, source_key)
    row = conn.execute(
        "SELECT * FROM budget_accounts WHERE id = ?", (account_id,)
    ).fetchone()
    if row is not None:
        return BudgetAccount.model_validate(dict(row))
    conn.execute(
        "INSERT OR IGNORE INTO budget_accounts"
        " (id, root_id, owner_job_id, bucket, purpose, amount_units,"
        "  beneficiary_id, source_key)"
        " VALUES (?, ?, ?, ?, ?, 0, ?, ?)",
        (account_id, root_id, owner_job_id, bucket.value, purpose, beneficiary_id, source_key),
    )
    return _get_required_account(conn, account_id)


def _get_wallet_ledger_account(conn: sqlite3.Connection, participant_id: str) -> BudgetAccount:
    """MockWallet の台帳上の相手口座を取得する。無ければ Wallet 残高で初期化する。

    Wallet 側を Journal に載せるための表現（migration 002）。口座の初期残高は
    Wallet の入金前残高であり、「入金前残高を含めた全口座間の Journal 合計が 0」
    の検証はこの口座を含めて行う。以後の Wallet 増減は apply_journal の
    _apply_deltas が口座残高へ反映する（Wallet 表との二重更新は行わない）。
    """
    row = conn.execute(
        "SELECT account_id FROM wallet_ledger_accounts WHERE participant_id = ?",
        (participant_id,),
    ).fetchone()
    if row is not None:
        return _get_required_account(conn, row["account_id"])
    account = get_or_create_account(
        conn,
        root_id=participant_id,
        owner_job_id=participant_id,
        bucket=Bucket.AVAILABLE,
        source_key=WALLET_LEDGER_SOURCE_KEY,
        purpose="mock wallet ledger counterpart",
    )
    conn.execute(
        "INSERT INTO wallet_ledger_accounts (account_id, participant_id) VALUES (?, ?)",
        (account.id, participant_id),
    )
    # 既存 Wallet 残高を口座へ一度だけ反映する（seed は Journal に載せないデモ準備）。
    conn.execute(
        "UPDATE budget_accounts SET amount_units ="
        " (SELECT balance_units FROM mock_wallets WHERE participant_id = ?)"
        " WHERE id = ?",
        (participant_id, account.id),
    )
    return _get_required_account(conn, account.id)


def _insert_entries(
    conn: sqlite3.Connection,
    operation_id: str,
    entries: list[tuple[str, int]],
) -> None:
    """1 操作の JournalEntry を operation_id + entry_no で一意に記録する。

    delta 合計が 0 でない操作は台帳の保存則を壊すため拒否する。
    """
    if not entries:
        raise OjpError(ErrorCode.INVALID_ARGUMENT, "journal entries must not be empty")
    total = sum(delta for _, delta in entries)
    if total != 0:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            f"journal entries of one operation must sum to zero: {total}",
        )
    for entry_no, (account_id, delta) in enumerate(entries):
        conn.execute(
            "INSERT INTO journal_entries (operation_id, entry_no, account_id, delta_units)"
            " VALUES (?, ?, ?, ?)",
            (operation_id, entry_no, account_id, delta),
        )


def _apply_deltas(
    conn: sqlite3.Connection,
    entries: list[tuple[str, int]],
) -> None:
    """口座残高を更新する。非負制約違反はドメイン競合として拒否する。"""
    for account_id, delta in entries:
        try:
            cursor = conn.execute(
                "UPDATE budget_accounts SET amount_units = amount_units + ? WHERE id = ?",
                (delta, account_id),
            )
        except sqlite3.IntegrityError as exc:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"account balance would become negative: {account_id}",
            ) from exc
        if cursor.rowcount != 1:
            raise OjpError(
                ErrorCode.INVALID_STATE, f"budget account not found: {account_id}"
            )


def apply_journal(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    reason: str,
    now_us: int,
    entries: list[tuple[str, int]],
) -> None:
    """JournalTransaction・Entry と残高更新を同一 transaction で確定する。

    Entry は (account_id, delta_units) の列で、合計は必ず 0。呼び出し側は
    service 層の transaction 内からのみ呼ぶ。
    """
    conn.execute(
        "INSERT INTO journal_transactions (operation_id, reason, created_at_us)"
        " VALUES (?, ?, ?)",
        (operation_id, reason, now_us),
    )
    _insert_entries(conn, operation_id, entries)
    _apply_deltas(conn, entries)


# ---------------------------------------------------------------------------
# 口座からの集計導出（計画書 第9節: 親集計と子口座を足し直さない）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RootLedgerView:
    """Root の資金集計。口座から導出した表示のみで、正本は budget_accounts。"""

    root_id: str
    deposit_units: int
    available_units: int
    locked_units: int
    locked_breakdown_units: dict[str, int]
    paid_units: int
    refunded_units: int

    @property
    def escrow_units(self) -> int:
        """E = available + locked"""
        return self.available_units + self.locked_units


def get_root_ledger_view(conn: sqlite3.Connection, root_id: str) -> RootLedgerView:
    """Root の D / E / available / locked 内訳 / paid / refunded を導出する。

    残高の正本は budget_accounts（available / locked）。paid / refunded の正本は
    transfer_receipts（Receipt がある時だけ増える。APPROVED や DONE になっただけ
    では増えない）。D（Root 入金総額）は reason='fund' の funding Journal
    （Escrow available への累積入金）から導出する。入金は 1 回だけなので
    fund の効果と一致し、child 失敗時の available への戻し（reason='return'
    等）は含めない。
    """
    breakdown: dict[str, int] = {b.value: 0 for b in sorted(LOCKED_BUCKETS, key=str)}
    available = 0
    rows = conn.execute(
        "SELECT bucket, COALESCE(SUM(amount_units), 0) AS total"
        " FROM budget_accounts"
        " WHERE root_id = ? AND (source_key IS NULL OR source_key != ?)"
        " GROUP BY bucket",
        (root_id, WALLET_LEDGER_SOURCE_KEY),
    ).fetchall()
    for row in rows:
        bucket = Bucket(row["bucket"])
        total = int(row["total"])
        if bucket == Bucket.AVAILABLE:
            available += total
        elif bucket in LOCKED_BUCKETS:
            breakdown[bucket.value] += total
        elif bucket in (Bucket.PAID, Bucket.REFUNDED):
            # paid/refunded は残高を持たない（正本は transfer_receipts）。
            # S1 以前の互換のため残っていても集計には使わない。
            continue
        else:  # pragma: no cover - bucket CHECK により到達しない
            raise OjpError(ErrorCode.INVALID_STATE, f"unexpected bucket: {bucket}")
    receipt_rows = conn.execute(
        "SELECT p.kind, COALESCE(SUM(r.amount_units), 0) AS total"
        " FROM transfer_receipts r"
        " JOIN payment_operations p ON p.operation_id = r.operation_id"
        " WHERE p.root_id = ?"
        " GROUP BY p.kind",
        (root_id,),
    ).fetchall()
    paid = 0
    refunded = 0
    for row in receipt_rows:
        if PaymentKind(row["kind"]) == PaymentKind.PAYOUT:
            paid += int(row["total"])
        else:
            refunded += int(row["total"])
    deposit_row = conn.execute(
        "SELECT COALESCE(SUM(e.delta_units), 0) AS deposit"
        " FROM journal_entries e"
        " JOIN journal_transactions t ON t.operation_id = e.operation_id"
        " JOIN budget_accounts a ON a.id = e.account_id"
        " WHERE t.reason = 'fund' AND a.root_id = ? AND a.bucket = ?"
        "   AND (a.source_key IS NULL OR a.source_key != ?) AND e.delta_units > 0",
        (root_id, Bucket.AVAILABLE.value, WALLET_LEDGER_SOURCE_KEY),
    ).fetchone()
    deposit = int(deposit_row["deposit"])
    return RootLedgerView(
        root_id=root_id,
        deposit_units=deposit,
        available_units=available,
        locked_units=sum(breakdown.values()),
        locked_breakdown_units=breakdown,
        paid_units=paid,
        refunded_units=refunded,
    )


# ---------------------------------------------------------------------------
# 台帳不変条件の検証（テストから使える公開関数）
# ---------------------------------------------------------------------------


def check_accounts_non_negative(conn: sqlite3.Connection) -> list[str]:
    """全口座非負を検証する。違反口座 id の一覧を返す（空なら成立）。"""
    rows = conn.execute(
        "SELECT id FROM budget_accounts WHERE amount_units < 0"
    ).fetchall()
    return [row["id"] for row in rows]


def check_journal_zero_sum(conn: sqlite3.Connection) -> list[str]:
    """1 操作の Entry の delta 合計が 0 であることを検証する。"""
    rows = conn.execute(
        "SELECT operation_id, SUM(delta_units) AS total"
        " FROM journal_entries GROUP BY operation_id HAVING total <> 0"
    ).fetchall()
    return [row["operation_id"] for row in rows]


def check_cumulative_journal_zero_sum(conn: sqlite3.Connection) -> int:
    """MockWallet の入金前残高を含めた全口座間の Journal 合計の累計を返す。

    wallet_ledger_accounts 口座の初期残高（Wallet の入金前残高）を含めた
    全口座の現在残高合計は初期残高合計に等しく、Journal の累計合計は 0 で
    なければならない。戻り値は累計合計（0 なら成立）。
    """
    row = conn.execute(
        "SELECT COALESCE(SUM(delta_units), 0) AS total FROM journal_entries"
    ).fetchone()
    return int(row["total"])


def check_conservation(conn: sqlite3.Connection, root_id: str) -> bool:
    """D = available + locked + paid + refunded の保存則を検証する。"""
    view = get_root_ledger_view(conn, root_id)
    return view.deposit_units == (
        view.available_units + view.locked_units + view.paid_units + view.refunded_units
    )


def assert_ledger_invariants(conn: sqlite3.Connection, root_id: str) -> None:
    """保存則・非負・Journal 合計 0 の全不変条件を検証し、違反なら OjpError。"""
    negative = check_accounts_non_negative(conn)
    if negative:
        raise OjpError(
            ErrorCode.INVALID_STATE, f"negative budget accounts: {negative}"
        )
    bad_ops = check_journal_zero_sum(conn)
    if bad_ops:
        raise OjpError(
            ErrorCode.INVALID_STATE, f"journal operations not zero-sum: {bad_ops}"
        )
    cumulative = check_cumulative_journal_zero_sum(conn)
    if cumulative != 0:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            f"cumulative journal sum must be zero: {cumulative}",
        )
    if not check_conservation(conn, root_id):
        view = get_root_ledger_view(conn, root_id)
        raise OjpError(
            ErrorCode.INVALID_STATE,
            f"conservation violated for root {root_id}: {view}",
        )


# ---------------------------------------------------------------------------
# Root funding の正本照合（計画書 第9節: Root fund は公開予算の全額入金）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RootFundingTarget:
    """Root funding の正本情報。jobs / job_versions から導出した確定値。

    引落し元の Requester・公開予算・asset は呼出側の申告ではなく DB から
    解決し、fund の payload にはこの確定値だけを固定する。
    """

    requester_id: str
    budget_units: int
    asset: str


def resolve_root_job(conn: sqlite3.Connection, root_id: str) -> str:
    """Root 性と Requester を jobs から解決する（公開状態は見ない）。

    - 対象 Job が Root（parent_id IS NULL かつ root_id = id）でなければ
      INVALID_TARGET
    - 戻り値は引落し元の Requester（jobs.requester_id）

    Actor 権限の検査（FORBIDDEN）が公開 Version の有無の検査（INVALID_STATE）
    より先に来るように、公開状態の解決とは分離する。権限のない Actor に
    Job の公開状態を漏らさないための順序。
    """
    row = conn.execute(
        "SELECT requester_id FROM jobs"
        " WHERE id = ? AND parent_id IS NULL AND root_id = id",
        (root_id,),
    ).fetchone()
    if row is None:
        raise OjpError(
            ErrorCode.INVALID_TARGET,
            f"fund target must be a root job (parent_id IS NULL, root_id = id):"
            f" {root_id}",
        )
    return str(row["requester_id"])


def resolve_root_published_budget(
    conn: sqlite3.Connection, root_id: str
) -> tuple[int, str]:
    """公開 Version の予算・asset を job_versions から解決する。

    Root 性・Requester の解決（resolve_root_job）と権限検査の後に呼ぶ。
    Version 行が無ければ INVALID_STATE。
    """
    row = conn.execute(
        "SELECT v.budget_units AS budget_units, v.asset AS asset"
        " FROM jobs j JOIN job_versions v ON v.id = j.version_id"
        " WHERE j.id = ?",
        (root_id,),
    ).fetchone()
    if row is None:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            f"root job has no published version (jobs.version_id is unset): {root_id}",
        )
    return int(row["budget_units"]), str(row["asset"])


def resolve_root_funding_target(
    conn: sqlite3.Connection, root_id: str
) -> RootFundingTarget:
    """Root funding の対象を DB から解決する（2 段解決の薄いラッパー）。

    - 対象 Job が Root であること（parent_id IS NULL かつ root_id = id）を
      確認する。Child への fund は INVALID_TARGET で拒否する
    - 引落し元の Requester は jobs.requester_id から導出する
    - 入金額・asset は job_versions の公開版（PoC は UNIQUE(job_id, version)
      の 1 版）から導出する。Version 行が無ければ INVALID_STATE

    fund_root のように権限検査を先に行う経路は、resolve_root_job /
    resolve_root_published_budget を直接呼んで順序を制御する。
    """
    requester_id = resolve_root_job(conn, root_id)
    budget_units, asset = resolve_root_published_budget(conn, root_id)
    return RootFundingTarget(
        requester_id=requester_id,
        budget_units=budget_units,
        asset=asset,
    )


# ---------------------------------------------------------------------------
# MockWallet（seed はデモ準備専用。Job 資金操作の経路からは呼べない）
# ---------------------------------------------------------------------------


def seed_mock_wallet_for_demo(
    conn: sqlite3.Connection,
    *,
    participant_id: str,
    asset: str,
    balance_units: int,
) -> None:
    """デモ準備専用: MockWallet を seed する（計画書 第9節・CLI `ojp demo init` 相当）。

    Job 資金操作の経路からは呼ばない。fund_root は「既に seed された Wallet」
    からの引落しだけを行い、Wallet が無い・残高不足なら拒否する。
    seed は Journal に載せないデモデータの投入であり、冪等（同額再実行は no-op、
    別額は拒否）。
    """
    if asset != ASSET_MOCK_USDC:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT, f"unsupported asset for seeding: {asset!r}"
        )
    if balance_units < 0:
        raise OjpError(ErrorCode.INVALID_ARGUMENT, "seed balance must be non-negative")
    row = conn.execute(
        "SELECT balance_units FROM mock_wallets WHERE participant_id = ?",
        (participant_id,),
    ).fetchone()
    if row is not None:
        if int(row["balance_units"]) != balance_units:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"mock wallet already seeded with a different balance: {participant_id}",
            )
        return
    conn.execute(
        "INSERT INTO mock_wallets (participant_id, asset, balance_units) VALUES (?, ?, ?)",
        (participant_id, asset, balance_units),
    )


def _require_seeded_wallet(conn: sqlite3.Connection, participant_id: str) -> None:
    """seed 済み Wallet の存在を要求する。無ければ拒否（seed 経路の悪用防止）。"""
    row = conn.execute(
        "SELECT 1 FROM mock_wallets WHERE participant_id = ?", (participant_id,)
    ).fetchone()
    if row is None:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            f"mock wallet not found (seed 済みでない): {participant_id}",
        )


def _sync_wallet_ledger_account(conn: sqlite3.Connection, participant_id: str) -> BudgetAccount:
    """Wallet 表の残高を、台帳口座の残高へ合わせる（台帳口座が正本）。

    apply_journal の _apply_deltas が台帳口座を更新した後に呼び、
    mock_wallets 表を台帳へ同期する。Wallet の増減は必ず Journal 経由で行い、
    台帳口座と Wallet 表が乖離しないようにする。
    """
    account = _get_wallet_ledger_account(conn, participant_id)
    conn.execute(
        "UPDATE mock_wallets SET balance_units ="
        " (SELECT amount_units FROM budget_accounts WHERE id = ?)"
        " WHERE participant_id = ?",
        (account.id, participant_id),
    )
    return _get_required_account(conn, account.id)


# ---------------------------------------------------------------------------
# 100→10 の操作一覧（計画書 第9節）を構成する台帳プリミティブ
# ---------------------------------------------------------------------------


def allocate_child_work_in_tx(
    conn: sqlite3.Connection,
    *,
    root_id: str,
    child_id: str,
    amount_units: int,
    operation_id: str,
    now_us: int,
) -> dict[str, Any]:
    """Child create: available -amount / child_work +amount（business_key allocate:{child_id}）。"""
    if amount_units <= 0:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT, "child budget must be positive (ゼロChild予算拒否)"
        )
    available = get_or_create_account(
        conn, root_id=root_id, owner_job_id=root_id, bucket=Bucket.AVAILABLE
    )
    child_work = get_or_create_account(
        conn,
        root_id=root_id,
        owner_job_id=child_id,
        bucket=Bucket.CHILD_WORK,
        source_key=child_id,
        purpose="child work reservation",
    )
    apply_journal(
        conn,
        operation_id=operation_id,
        reason="allocate",
        now_us=now_us,
        entries=[(available.id, -amount_units), (child_work.id, amount_units)],
    )
    return {
        "root_id": root_id,
        "child_id": child_id,
        "amount_units": amount_units,
        "child_work_account_id": child_work.id,
    }


def child_approval_in_tx(
    conn: sqlite3.Connection,
    *,
    root_id: str,
    child_id: str,
    amount_units: int,
    operation_id: str,
    now_us: int,
) -> dict[str, Any]:
    """Child approval: child_work -amount / child_payout +amount。

    残額 0 なら予約は作らず正常な no-op とする（計画書 第16節）。
    """
    if amount_units < 0:
        raise OjpError(ErrorCode.INVALID_ARGUMENT, "amount must be non-negative")
    if amount_units == 0:
        return {"root_id": root_id, "child_id": child_id, "amount_units": 0, "no_op": True}
    child_work = get_or_create_account(
        conn,
        root_id=root_id,
        owner_job_id=child_id,
        bucket=Bucket.CHILD_WORK,
        source_key=child_id,
    )
    child_payout = get_or_create_account(
        conn,
        root_id=root_id,
        owner_job_id=child_id,
        bucket=Bucket.CHILD_PAYOUT,
        source_key=child_id,
        purpose="child payout reservation",
    )
    apply_journal(
        conn,
        operation_id=operation_id,
        reason="reserve",
        now_us=now_us,
        entries=[(child_work.id, -amount_units), (child_payout.id, amount_units)],
    )
    return {
        "root_id": root_id,
        "child_id": child_id,
        "amount_units": amount_units,
        "child_payout_account_id": child_payout.id,
    }


def child_failure_return_in_tx(
    conn: sqlite3.Connection,
    *,
    root_id: str,
    child_id: str,
    amount_units: int,
    operation_id: str,
    now_us: int,
    parent_terminal_refund_reserved: bool = False,
    child_done_payment_pending: bool = False,
) -> dict[str, Any]:
    """Child 失敗時の原資返却。

    - 通常（Parent 生存）: child_work -amount / available +amount
      （business_key return:{child_id}）
    - Parent 終端（返金予約済み）の場合: child_work → available → refund の
      2 移動を一つの transaction（この関数 1 回の呼び出し）で記録し、
      business_key refund:{root_id}:child-return:{child_id} で結合する。
      途中の available を外から再利用させない（呼び出し側は
      parent_terminal_refund_reserved=True を渡す）。
    - Child が DONE だが送金障害中（child_done_payment_pending=True）の場合は
      失敗返却を禁止する（計画書 第9節）。
    """
    if child_done_payment_pending:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            "child is DONE with payment pending; failure return is forbidden",
        )
    if amount_units < 0:
        raise OjpError(ErrorCode.INVALID_ARGUMENT, "amount must be non-negative")
    if amount_units == 0:
        return {"root_id": root_id, "child_id": child_id, "amount_units": 0, "no_op": True}
    child_work = get_or_create_account(
        conn,
        root_id=root_id,
        owner_job_id=child_id,
        bucket=Bucket.CHILD_WORK,
        source_key=child_id,
    )
    available = get_or_create_account(
        conn, root_id=root_id, owner_job_id=root_id, bucket=Bucket.AVAILABLE
    )
    if parent_terminal_refund_reserved:
        refund = get_or_create_account(
            conn,
            root_id=root_id,
            owner_job_id=root_id,
            bucket=Bucket.REFUND,
            purpose="requester refund reservation",
        )
        apply_journal(
            conn,
            operation_id=operation_id,
            reason="child-return-after-parent-terminal",
            now_us=now_us,
            entries=[
                (child_work.id, -amount_units),
                (available.id, amount_units),
                (available.id, -amount_units),
                (refund.id, amount_units),
            ],
        )
        return {
            "root_id": root_id,
            "child_id": child_id,
            "amount_units": amount_units,
            "refund_account_id": refund.id,
            "combined": True,
        }
    apply_journal(
        conn,
        operation_id=operation_id,
        reason="return",
        now_us=now_us,
        entries=[(child_work.id, -amount_units), (available.id, amount_units)],
    )
    return {"root_id": root_id, "child_id": child_id, "amount_units": amount_units}


def parent_approval_in_tx(
    conn: sqlite3.Connection,
    *,
    root_id: str,
    amount_units: int,
    operation_id: str,
    now_us: int,
) -> dict[str, Any]:
    """Parent approval: available -amount / parent_payout +amount。

    残額 0 なら予約は作らず正常な no-op とする（計画書 第16節）。
    """
    if amount_units < 0:
        raise OjpError(ErrorCode.INVALID_ARGUMENT, "amount must be non-negative")
    if amount_units == 0:
        return {"root_id": root_id, "amount_units": 0, "no_op": True}
    available = get_or_create_account(
        conn, root_id=root_id, owner_job_id=root_id, bucket=Bucket.AVAILABLE
    )
    parent_payout = get_or_create_account(
        conn,
        root_id=root_id,
        owner_job_id=root_id,
        bucket=Bucket.PARENT_PAYOUT,
        purpose="parent payout reservation",
    )
    apply_journal(
        conn,
        operation_id=operation_id,
        reason="reserve",
        now_us=now_us,
        entries=[(available.id, -amount_units), (parent_payout.id, amount_units)],
    )
    return {
        "root_id": root_id,
        "amount_units": amount_units,
        "parent_payout_account_id": parent_payout.id,
    }


def parent_failure_refund_in_tx(
    conn: sqlite3.Connection,
    *,
    root_id: str,
    amount_units: int,
    operation_id: str,
    now_us: int,
) -> dict[str, Any]:
    """Parent failure: available -amount / refund +amount（business_key refund:{root_id}:terminal）。

    残額 0 なら予約は作らず正常な no-op とする（計画書 第16節）。
    """
    if amount_units < 0:
        raise OjpError(ErrorCode.INVALID_ARGUMENT, "amount must be non-negative")
    if amount_units == 0:
        return {"root_id": root_id, "amount_units": 0, "no_op": True}
    available = get_or_create_account(
        conn, root_id=root_id, owner_job_id=root_id, bucket=Bucket.AVAILABLE
    )
    refund = get_or_create_account(
        conn,
        root_id=root_id,
        owner_job_id=root_id,
        bucket=Bucket.REFUND,
        purpose="requester refund reservation",
    )
    apply_journal(
        conn,
        operation_id=operation_id,
        reason="reserve",
        now_us=now_us,
        entries=[(available.id, -amount_units), (refund.id, amount_units)],
    )
    return {
        "root_id": root_id,
        "amount_units": amount_units,
        "refund_account_id": refund.id,
    }


# ---------------------------------------------------------------------------
# PaymentOperation（計画書 第5節・第9節「Operationと送金失敗」）
# ---------------------------------------------------------------------------

# 上限付きバックオフ（計画書 第9節: 1 / 2 / 4 / 8 / 30 秒、以後 30 秒）。
# 有限回で打ち切って返金へ切り替えない。永続障害は last_error と予約金として可視化する。
RETRY_BACKOFF_SECONDS = (1, 2, 4, 8, 30)
RETRY_BACKOFF_MAX_SECONDS = 30


def compute_next_retry_at_us(now_us: int, attempt_count: int) -> int:
    """attempt_count 回目の失敗直後の next_retry_at_us を返す。

    attempt_count=1 → now+1s、2 → +2s、3 → +4s、4 → +8s、5 以降は +30s。
    now は clock.now_for_write_transaction で採取した transaction の時刻を使う。
    """
    if attempt_count < 1:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"attempt_count must be >= 1: {attempt_count}",
        )
    index = min(attempt_count - 1, len(RETRY_BACKOFF_SECONDS) - 1)
    delay_seconds = min(RETRY_BACKOFF_SECONDS[index], RETRY_BACKOFF_MAX_SECONDS)
    return now_us + delay_seconds * 1_000_000


def _get_payment_operation(
    conn: sqlite3.Connection, operation_id: str
) -> PaymentOperation | None:
    row = conn.execute(
        "SELECT * FROM payment_operations WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()
    if row is None:
        return None
    return PaymentOperation.model_validate(dict(row))


def get_payment_operation(
    conn: sqlite3.Connection, operation_id: str
) -> PaymentOperation | None:
    """PaymentOperation を operation_id で取得する（無ければ None）。"""
    return _get_payment_operation(conn, operation_id)


def get_payment_operation_by_business_key(
    conn: sqlite3.Connection, business_key: str
) -> PaymentOperation | None:
    """確定済みの業務効果（business_key）から既存 PaymentOperation を探す。

    異なる operation_id で同じ業務効果（同じ Child への payout、同じ原資の
    refund）を再確定しようとしたとき、既存の予約と PaymentOperation を返す
    ために使う（二重送金を防ぐ照合。operations.business_key の UNIQUE が
    先に競合する経路ではこの関数は呼ばれない）。
    """
    row = conn.execute(
        "SELECT * FROM payment_operations WHERE business_key = ?",
        (business_key,),
    ).fetchone()
    if row is None:
        return None
    return PaymentOperation.model_validate(dict(row))


def create_payment_operation_in_tx(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    business_key: str,
    root_id: str,
    job_id: str,
    source_account_id: str,
    amount_units: int,
    payee_id: str,
    kind: PaymentKind,
) -> PaymentOperation:
    """PaymentOperation を PENDING で作成する。原資と受取人は作成時に固定。

    operations 行（冪等性の正本）は service._run_idempotent が効果の記録に
    先立って同じ transaction 内に挿入済み（payment_operations が operations
    を FK で参照するため）。payment_operations.business_key の UNIQUE と
    operations.business_key の UNIQUE（migration 002）が二重確定を防ぐ。
    """
    if amount_units <= 0:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT, "payment amount must be positive"
        )
    try:
        conn.execute(
            "INSERT INTO payment_operations"
            " (operation_id, business_key, root_id, job_id, source_account_id,"
            "  amount_units, payee_id, kind, status, attempt_count)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
            (
                operation_id,
                business_key,
                root_id,
                job_id,
                source_account_id,
                amount_units,
                payee_id,
                kind.value,
                PaymentStatus.PENDING.value,
            ),
        )
    except sqlite3.IntegrityError as exc:
        if "payment_operations.business_key" in str(exc):
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"payment already reserved for business effect: {business_key}",
            ) from exc
        raise
    payment = _get_payment_operation(conn, operation_id)
    assert payment is not None
    return payment


def list_due_payment_operations(
    conn: sqlite3.Connection, *, now_us: int
) -> list[PaymentOperation]:
    """settlement 対象の PENDING / RETRYABLE を期限順で取得する。

    PENDING は即時対象、RETRYABLE は next_retry_at_us <= now_us のものだけ。
    """
    rows = conn.execute(
        "SELECT * FROM payment_operations"
        " WHERE status = ?"
        "    OR (status = ? AND next_retry_at_us IS NOT NULL"
        "        AND next_retry_at_us <= ?)"
        " ORDER BY operation_id",
        (PaymentStatus.PENDING.value, PaymentStatus.RETRYABLE.value, now_us),
    ).fetchall()
    return [PaymentOperation.model_validate(dict(row)) for row in rows]


def _update_payment_status(
    conn: sqlite3.Connection,
    operation_id: str,
    *,
    status: PaymentStatus,
    attempt_count: int | None = None,
    next_retry_at_us: int | None = None,
    last_error: str | None = None,
    receipt_id: str | None = None,
) -> None:
    cursor = conn.execute(
        "UPDATE payment_operations SET status = ?,"
        " attempt_count = COALESCE(?, attempt_count),"
        " next_retry_at_us = ?, last_error = ?,"
        " receipt_id = COALESCE(?, receipt_id)"
        " WHERE operation_id = ?",
        (
            status.value,
            attempt_count,
            next_retry_at_us,
            last_error,
            receipt_id,
            operation_id,
        ),
    )
    if cursor.rowcount != 1:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            f"payment operation not found: {operation_id}",
        )


def record_payment_error_in_tx(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    error_message: str,
) -> PaymentOperation:
    """失敗理由を last_error へ記録するだけ（status・attempt_count は据え置き）。

    Receipt 正本との整合違反（SUCCEEDED だが Receipt が無い・3 属性が
    食い違う）の可視化に使う。既に SUCCEEDED へ確定した整合状態は後から
    書き換えないため、attempt_count・next_retry_at_us は変えず、運用者が
    内容を確認して是正できるよう理由だけを残す。
    """
    payment = _get_payment_operation(conn, operation_id)
    if payment is None:
        raise OjpError(
            ErrorCode.INVALID_STATE, f"payment operation not found: {operation_id}"
        )
    _update_payment_status(
        conn,
        operation_id,
        status=payment.status,
        next_retry_at_us=payment.next_retry_at_us,
        last_error=error_message,
    )
    updated = _get_payment_operation(conn, operation_id)
    assert updated is not None
    return updated


def mark_payment_retryable_in_tx(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    error_message: str,
    now_us: int,
) -> PaymentOperation:
    """送金失敗を記録する（RETRYABLE + バックオフ）。

    有限回で打ち切らない（計画書 第9節）。attempt_count は増え続け、
    next_retry_at_us は 30 秒上限で繰り返し再試行される。
    """
    payment = _get_payment_operation(conn, operation_id)
    if payment is None:
        raise OjpError(
            ErrorCode.INVALID_STATE, f"payment operation not found: {operation_id}"
        )
    if payment.status == PaymentStatus.SUCCEEDED:
        return payment
    new_count = payment.attempt_count + 1
    _update_payment_status(
        conn,
        operation_id,
        status=PaymentStatus.RETRYABLE,
        attempt_count=new_count,
        next_retry_at_us=compute_next_retry_at_us(now_us, new_count),
        last_error=error_message,
    )
    updated = _get_payment_operation(conn, operation_id)
    assert updated is not None
    return updated


@dataclass(frozen=True)
class PaymentFailureView:
    """送金失敗の可視化ビュー（Phase 5 の `ojp ledger show` / `ojp payment retry`
    が使う土台）。失敗理由・再試行状況と、未送金のまま予約に残っている
    原資口座・金額（locked 予約金）を一緒に返す。"""

    operation_id: str
    status: PaymentStatus
    attempt_count: int
    last_error: str | None
    next_retry_at_us: int | None
    source_account_id: str
    source_amount_units: int
    amount_units: int
    payee_id: str
    kind: PaymentKind


def get_payment_failure_view(
    conn: sqlite3.Connection, operation_id: str
) -> PaymentFailureView | None:
    """PaymentOperation の失敗理由・再試行状況・locked 予約金を読み取る。

    予約口座が settlement で消費されていても PaymentOperation の原資・金額は
    作成時の固定値として返る（PaymentOperation が正本）。
    """
    payment = _get_payment_operation(conn, operation_id)
    if payment is None:
        return None
    source_row = conn.execute(
        "SELECT amount_units FROM budget_accounts WHERE id = ?",
        (payment.source_account_id,),
    ).fetchone()
    return PaymentFailureView(
        operation_id=payment.operation_id,
        status=payment.status,
        attempt_count=payment.attempt_count,
        last_error=payment.last_error,
        next_retry_at_us=payment.next_retry_at_us,
        source_account_id=payment.source_account_id,
        source_amount_units=int(source_row["amount_units"]) if source_row else 0,
        amount_units=payment.amount_units,
        payee_id=payment.payee_id,
        kind=payment.kind,
    )


def mark_payment_succeeded_in_tx(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    receipt_id: str,
) -> PaymentOperation:
    """Receipt 確定後に SUCCEEDED へ収束させる（冪等）。

    Receipt が正本。既に SUCCEEDED なら receipt_id の一致だけを確認する。
    """
    payment = _get_payment_operation(conn, operation_id)
    if payment is None:
        raise OjpError(
            ErrorCode.INVALID_STATE, f"payment operation not found: {operation_id}"
        )
    if payment.status == PaymentStatus.SUCCEEDED:
        if payment.receipt_id != receipt_id:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"payment {operation_id} succeeded with a different receipt:"
                f" {payment.receipt_id} != {receipt_id}",
            )
        return payment
    _update_payment_status(
        conn,
        operation_id,
        status=PaymentStatus.SUCCEEDED,
        receipt_id=receipt_id,
    )
    updated = _get_payment_operation(conn, operation_id)
    assert updated is not None
    return updated


# ---------------------------------------------------------------------------
# Escrow port と Mock 実装（計画書 第9節・第15節）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TransferPayload:
    """Escrow port の transfer へ渡す immutable payload。

    ドメインが「誰へ、いくら、何を根拠に」を決め、port が実行する。
    原資口座・受取人・金額は PaymentOperation 作成時に固定され、ここで
    変更できない。
    """

    operation_id: str
    amount_units: int
    payee_id: str
    source_account_id: str
    business_key: str


@dataclass(frozen=True)
class FundPayload:
    """Escrow port の fund へ渡す immutable payload。

    transfer と同じく、ドメインが「誰の Wallet から、どの Root の Escrow
    available へ、いくら、何を根拠に」を決め、port が実行する。金額の一致・
    原資の固定はドメイン判断であり、payload に確定値として固定される。
    本番 Escrow 用の chain_id・wallet 署名・x402 ヘッダー・外部 HTTP API の
    空実装は持たない。
    """

    operation_id: str
    root_id: str
    requester_id: str
    expected_amount_units: int
    amount_units: int
    asset: str
    business_key: str


class PaymentFailedError(Exception):
    """Escrow port からの送金失敗。失敗は種類にかかわらず PaymentOperation に
    記録され（attempt_count・last_error・RETRYABLE・バックオフ）、上限付き
    バックオフで再試行される。永続障害は last_error と予約金として可視化
    され、返金への切替・Job FAILED へは落ちない。"""


class EscrowPort:
    """Mock Escrow の port（計画書 第15節）。

    fund / transfer / lookup。本番アダプターは PoC では実装しない。
    """

    def fund(
        self,
        conn: sqlite3.Connection,
        operation_id: str,
        payload: FundPayload,
    ) -> dict[str, Any]:  # pragma: no cover - protocol 宣言
        raise NotImplementedError

    def transfer(
        self,
        conn: sqlite3.Connection,
        operation_id: str,
        payload: TransferPayload,
    ) -> TransferReceipt:  # pragma: no cover - protocol 宣言
        raise NotImplementedError

    def lookup(
        self, conn: sqlite3.Connection, operation_id: str
    ) -> TransferReceipt | None:  # pragma: no cover - protocol 宣言
        raise NotImplementedError


def lookup_transfer_receipt(
    conn: sqlite3.Connection, operation_id: str
) -> TransferReceipt | None:
    """operation_id に対応する Receipt を照会する（Receipt が正本）。"""
    row = conn.execute(
        "SELECT * FROM transfer_receipts WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()
    if row is None:
        return None
    return TransferReceipt.model_validate(dict(row))


# failpoint（障害注入）の seam。Phase 1 の db._pre_write_lock_hook と同じ
# 「テストが属性へ関数を代入する」方式。本番コードに環境変数・ファイル I/O・
# 待機を持ち込まず、test mode の DB でのみ発火する（realtime では代入済みでも
# 拒否する。第14節「failpoint は専用 test mode のみで有効」）。
failpoint_before_commit: Callable[[str], None] | None = None
failpoint_after_commit: Callable[[str], None] | None = None
failpoint_after_receipt: Callable[[str], None] | None = None


def _fire_failpoint(
    conn: sqlite3.Connection, hook: Callable[[str], None] | None, name: str
) -> None:
    if hook is None:
        return
    if clock.read_mode(conn) != ClockMode.TEST:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            f"failpoint {name} is only available in test mode",
        )
    hook(name)


def assert_failpoints_allowed(conn: sqlite3.Connection) -> None:
    """failpoint が代入済みなら、test mode の DB でなければ拒否する。

    settlement（process_single_payment）の先頭・台帳・Wallet・Receipt・
    PaymentOperation を一切変更しない位置で呼ぶ前置検査。「failpoint は
    test mode の DB でのみ有効で、通常経路（realtime）から触れない」という
    契約は、何も動かさずに拒否することを意味する。発火点（_fire_failpoint）
    側のモード検査は二重の防御として残す。
    """
    names = [
        name
        for name, hook in (
            ("before_commit", failpoint_before_commit),
            ("after_commit", failpoint_after_commit),
            ("after_receipt", failpoint_after_receipt),
        )
        if hook is not None
    ]
    if not names:
        return
    if clock.read_mode(conn) != ClockMode.TEST:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            f"failpoint {names[0]} is only available in test mode",
        )


class MockEscrow(EscrowPort):
    """同一 DB 内で MockWallet・Receipt・台帳口座を原子的に更新する Mock。

    fund は seed 済み Requester Wallet の減額と Escrow available の増額を
    呼出側の transaction 内で Journal 確定する（port を通らない funding の
    確定経路は存在しない）。

    transfer は Receipt を先に照会し、既存 Receipt の金額・受取人・原資が
    予約（payload）と一致することを確認する。未処理の場合のみ、locked 減額・
    MockWallet 増額・Journal・Receipt を 1 つの DB
    transaction（呼出側の transaction）で確定する（paid/refunded は Receipt の
    集計として導出され、Receipt がある時だけ増える）。
    transfer_receipts.operation_id は UNIQUE（001）で二重送金を防ぐ。
    """

    def fund(
        self,
        conn: sqlite3.Connection,
        operation_id: str,
        payload: FundPayload,
    ) -> dict[str, Any]:
        # Root funding の実行: seed 済み Requester Wallet の減額と Escrow
        # available の増額を、呼出側の transaction 内で Journal 確定する
        # （transfer と同じく port が実行し、ドメインは payload で確定値だけを
        # 渡す）。正確な全額入金のみ受け付け、ゼロ・不足・超過は拒否する。
        # Root 間の資金移動は計画書 第5節で禁止されており、payload には
        # 「別 Root の資金を原資にする」入力経路が存在しない（原資は当該
        # Root の Requester の Wallet に固定される）。
        if payload.operation_id != operation_id:
            raise OjpError(
                ErrorCode.INVALID_ARGUMENT,
                "fund operation_id must match the immutable payload",
            )
        if payload.asset != ASSET_MOCK_USDC:
            raise OjpError(
                ErrorCode.INVALID_ARGUMENT,
                f"unsupported asset for mock fund: {payload.asset!r}",
            )
        if payload.expected_amount_units <= 0:
            raise OjpError(
                ErrorCode.INVALID_ARGUMENT, "expected amount must be positive"
            )
        if payload.amount_units <= 0:
            raise OjpError(
                ErrorCode.INVALID_ARGUMENT,
                "fund amount must be positive (ゼロ入金拒否)",
            )
        if payload.amount_units != payload.expected_amount_units:
            raise OjpError(
                ErrorCode.INVALID_ARGUMENT,
                "fund amount must equal the full budget exactly:"
                f" expected={payload.expected_amount_units},"
                f" got={payload.amount_units}",
            )
        available = get_or_create_account(
            conn,
            root_id=payload.root_id,
            owner_job_id=payload.root_id,
            bucket=Bucket.AVAILABLE,
        )
        _require_seeded_wallet(conn, payload.requester_id)
        wallet_account = _get_wallet_ledger_account(conn, payload.requester_id)
        apply_journal(
            conn,
            operation_id=operation_id,
            reason="fund",
            now_us=clock.now_for_write_transaction(conn),
            entries=[
                (wallet_account.id, -payload.amount_units),
                (available.id, payload.amount_units),
            ],
        )
        # 台帳口座を正本として Wallet 表へ反映する（二重更新しない）
        _sync_wallet_ledger_account(conn, payload.requester_id)
        return {
            "root_id": payload.root_id,
            "amount_units": payload.amount_units,
            "available_account_id": available.id,
        }

    def lookup(
        self, conn: sqlite3.Connection, operation_id: str
    ) -> TransferReceipt | None:
        return lookup_transfer_receipt(conn, operation_id)

    def transfer(
        self,
        conn: sqlite3.Connection,
        operation_id: str,
        payload: TransferPayload,
    ) -> TransferReceipt:
        # (a) 決済 commit 前の失敗の注入点。発火するとこの後の台帳・Receipt
        # 記録は一切行われず、呼出側の transaction 全体が rollback される。
        _fire_failpoint(conn, failpoint_before_commit, "before_commit")
        if payload.operation_id != operation_id:
            raise OjpError(
                ErrorCode.INVALID_ARGUMENT,
                "transfer operation_id must match the immutable payload",
            )
        # 1. Receipt を先に照会する（Receipt が正本）。既存なら照合だけ。
        existing = self.lookup(conn, operation_id)
        if existing is not None:
            if (
                existing.amount_units != payload.amount_units
                or existing.payee_id != payload.payee_id
                or existing.source_account_id != payload.source_account_id
            ):
                raise OjpError(
                    ErrorCode.INVALID_STATE,
                    "existing receipt does not match the reservation"
                    f" (operation_id={operation_id})",
                )
            return existing
        # 2. 未処理: 原資（locked）の口座を確認し、locked 減額・MockWallet 増額・
        #    Journal・Receipt を同一 transaction で確定する（paid/refunded は
        #    Receipt から導出するため残高口座は持たない）。
        source = _get_required_account(conn, payload.source_account_id)
        if source.bucket not in LOCKED_BUCKETS:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"payment source must be a locked bucket: {source.bucket}",
            )
        if source.amount_units < payload.amount_units:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "locked reservation is smaller than the payment amount",
            )
        _require_seeded_wallet(conn, payload.payee_id)
        wallet_account = _get_wallet_ledger_account(conn, payload.payee_id)
        # 保存則: locked 減額と Wallet 増額が貸借一致する。paid/refunded は
        # 残高口座を持たず、Receipt の集計として導出される（Receipt が正本で、
        # Receipt がある時だけ増える。Journal の delta 合計は 0 を維持する）。
        apply_journal(
            conn,
            operation_id=operation_id,
            reason="pay" if payload.business_key.startswith("payout:") else "refund",
            now_us=clock.now_for_write_transaction(conn),
            entries=[
                (source.id, -payload.amount_units),
                (wallet_account.id, payload.amount_units),
            ],
        )
        # 台帳口座を正本として Wallet 表へ反映する（mock_wallets の直接
        # UPDATE による二重更新はしない）
        _sync_wallet_ledger_account(conn, payload.payee_id)
        receipt_id = f"receipt:{operation_id}"
        try:
            conn.execute(
                "INSERT INTO transfer_receipts"
                " (receipt_id, operation_id, amount_units, payee_id, asset,"
                "  source_account_id)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (
                    receipt_id,
                    operation_id,
                    payload.amount_units,
                    payload.payee_id,
                    ASSET_MOCK_USDC,
                    payload.source_account_id,
                ),
            )
        except sqlite3.IntegrityError as exc:
            # operation_id UNIQUE の競合は、照会と挿入の間に別経路で確定した
            # 場合にのみ起きる。Receipt を正本として再照会して照合する。
            raced = self.lookup(conn, operation_id)
            if raced is None:
                raise OjpError(
                    ErrorCode.INVALID_STATE,
                    f"receipt insertion failed without a receipt: {exc}",
                ) from exc
            if (
                raced.amount_units != payload.amount_units
                or raced.payee_id != payload.payee_id
                or raced.source_account_id != payload.source_account_id
            ):
                raise OjpError(
                    ErrorCode.INVALID_STATE,
                    "existing receipt does not match the reservation"
                    f" (operation_id={operation_id})",
                ) from exc
            return raced
        return TransferReceipt(
            receipt_id=receipt_id,
            operation_id=operation_id,
            amount_units=payload.amount_units,
            payee_id=payload.payee_id,
            asset=ASSET_MOCK_USDC,
            source_account_id=payload.source_account_id,
        )
