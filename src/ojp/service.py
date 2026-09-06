"""Application service 層（計画書 第15節・第16節）。

service 層が transaction を所有し、下位の ledger 関数は開始済みの
transaction（接続）を受け取る。全書込は BEGIN IMMEDIATE で開始し、
now は clock.now_for_write_transaction で transaction 内 1 回だけ採取する。

冪等性は二段階（計画書 第16節）:
1. operation_id UNIQUE + actor/kind/payload_hash 照合。同一 ID・同一 actor・
   同一 payload の再送は保存済み結果をそのまま返す（replayed=True）。
   同一 ID の別 payload・別 Actor は IDEMPOTENCY_CONFLICT。
2. business_key の UNIQUE（migration 002）により、異なる operation_id でも
   同じ業務効果は 1 回だけ。

DB_BUSY は同じ operation_id で transaction 全体を再試行する。SQLite の
制約違反は正常なドメイン競合（OjpError + 適切な ErrorCode）へ変換する。
"""

from __future__ import annotations

import hashlib
import json
import random
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from . import clock, db, ledger
from .domain import (
    Bucket,
    ErrorCode,
    OjpError,
    OperationStatus,
    PaymentKind,
    PaymentStatus,
)

DB_BUSY_MAX_ATTEMPTS = 20
DB_BUSY_BASE_DELAY_SECONDS = 0.01
DB_BUSY_MAX_DELAY_SECONDS = 0.5

_KIND_PREFIXES = {
    "fund": "fund",
    "allocate": "allocate",
    "return": "return",
    "reserve": "reserve",
    "payout": "payout",
    "refund": "refund",
}


def _payload_hash(payload: dict[str, Any]) -> str:
    canonical = ledger.canonical_json_dumps(payload)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_operation_id(operation_id: str, kind: str) -> None:
    """operation_id を `<kind>:<suffix>` 形式に限定する。

    S2 が PaymentOperation の operation_id を operations へ FK で結ぶため、
    形式を揃えておく（S2 の送金 operation_id もこの規則に従う）。
    """
    prefix = _KIND_PREFIXES[kind]
    if (
        not isinstance(operation_id, str)
        or not operation_id.startswith(prefix + ":")
        or len(operation_id) <= len(prefix) + 1
    ):
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"operation_id must be '{prefix}:<suffix>': {operation_id!r}",
        )


def _insert_operation(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    actor_id: str,
    kind: str,
    payload_hash: str,
    business_key: str | None,
    status: OperationStatus,
    result: str | None,
    now_us: int,
) -> None:
    try:
        conn.execute(
            "INSERT INTO operations"
            " (operation_id, actor_id, kind, payload_hash, business_key, status, result,"
            "  created_at_us)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (operation_id, actor_id, kind, payload_hash, business_key, status.value, result, now_us),
        )
    except sqlite3.IntegrityError as exc:
        raise _map_integrity_error(exc) from exc


def _map_integrity_error(exc: sqlite3.IntegrityError) -> OjpError:
    message = str(exc)
    if "operations_business_key_unique" in message or (
        "operations.business_key" in message
    ):
        return OjpError(
            ErrorCode.INVALID_STATE,
            f"duplicate business effect (business_key already used): {message}",
        )
    if "journal_transactions" in message or "journal_entries" in message:
        return OjpError(
            ErrorCode.INVALID_STATE, f"journal already recorded for operation: {message}"
        )
    return OjpError(ErrorCode.INVALID_STATE, f"constraint violation: {message}")


def _existing_payment_result(
    conn: sqlite3.Connection, business_key: str
) -> CommandResult | None:
    """確定済みの業務効果（business_key）に既存の予約・PaymentOperation があれば
    その結果を返す。無ければ None。

    異なる operation_id で同じ業務効果（同じ Child への payout、同じ原資の
    refund）を確定しようとしたとき、新しい送金を作らず既存の結果を返す
    （計画書 第16節「同じ Child を別 ID で二重 approve しても既存
    Acceptance と既存 PaymentOperation を返す」）。Journal（資金移動）を
    持たない 0 残 no-op の予約記録は送金の効果を持たないため、ここでは
    payment_operations だけを見る。
    """
    payment = ledger.get_payment_operation_by_business_key(conn, business_key)
    if payment is None:
        return None
    operation_row = conn.execute(
        "SELECT result FROM operations WHERE operation_id = ?",
        (payment.operation_id,),
    ).fetchone()
    data = (
        json.loads(operation_row["result"])
        if operation_row is not None and operation_row["result"]
        else {}
    )
    data = {**data, "payment_operation_id": payment.operation_id}
    return CommandResult(
        data=data, operation_id=payment.operation_id, replayed=True
    )


@dataclass(frozen=True)
class CommandResult:
    """コマンドの結果。replayed は同一 operation_id 再送で保存済み結果を返したか。"""

    data: dict[str, Any]
    operation_id: str
    replayed: bool = False


def _run_idempotent(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    kind: str,
    operation_id: str | None,
    business_key: str | None,
    payload: dict[str, Any],
    apply_effects,
    reuse_existing_payment: bool = False,
    payment_kind: PaymentKind | None = None,
) -> CommandResult:
    """Operation の冪等性を管理しながら transaction を所有して実行する。

    - operation_id 未指定なら `<kind>:<uuid4>` を生成して結果へ返す
    - 同一 ID・同一 actor・同一 payload の再送は保存済み結果を replay する
    - 同一 ID の別 payload / 別 Actor は IDEMPOTENCY_CONFLICT
    - business_key の UNIQUE 競合（異なる ID で同じ業務効果）はドメイン競合へ変換。
      reuse_existing_payment=True の送金予約系では、競合前に確定済みの既存
      PaymentOperation の結果を返す（同じ業務効果は 1 回だけ送金される）
    - DB_BUSY は同じ operation_id で transaction 全体を有限回再試行する
    - payment_kind を渡すと、apply_effects の直前に送金用の派生 Operation
      （`<operation_id>:payment`、business_key=NULL）も同じ transaction で
      記録する。payment_operations が operations を FK で参照するため、
      送金予約を作るコマンドはこの派生行を親にする（計画書 第16節の派生 ID）
    """
    if operation_id is None:
        operation_id = ledger.new_operation_id(kind)
    _validate_operation_id(operation_id, kind)
    payload_hash = _payload_hash(payload)

    for attempt in range(DB_BUSY_MAX_ATTEMPTS):
        try:
            with db.transaction(conn, immediate=True):
                existing = conn.execute(
                    "SELECT actor_id, kind, payload_hash, status, result"
                    " FROM operations WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
                if existing is None and reuse_existing_payment and business_key is not None:
                    # 別 operation_id での再確定: 確定済みの同じ業務効果があれば
                    # 新しい送金を作らず既存の結果を返す（二重送金しない）。
                    # 同一 operation_id の再送は下の通常 replay 経路で処理する。
                    reused = _existing_payment_result(conn, business_key)
                    if reused is not None:
                        return reused
                if existing is not None:
                    if (
                        existing["actor_id"] != actor_id
                        or existing["kind"] != kind
                        or existing["payload_hash"] != payload_hash
                    ):
                        raise OjpError(
                            ErrorCode.IDEMPOTENCY_CONFLICT,
                            "operation_id was already used with a different"
                            " actor/kind/payload",
                        )
                    if existing["status"] != OperationStatus.SUCCEEDED.value:
                        raise OjpError(
                            ErrorCode.INVALID_STATE,
                            f"operation {operation_id} is stored with status"
                            f" {existing['status']} and cannot be replayed",
                        )
                    data = json.loads(existing["result"]) if existing["result"] else {}
                    return CommandResult(data=data, operation_id=operation_id, replayed=True)

                # now は効果を記録する直前に transaction 内 1 回だけ採取する
                now_us = clock.now_for_write_transaction(conn)
                # operations 行は効果の記録より先に挿入する。payment_operations
                # が operations を FK で参照するため、子（送金予約）を作る効果は
                # 親（冪等性の正本）の存在を必要とする。result は効果の記録後に
                # UPDATE で確定する。効果の記録が失敗すれば transaction ごと
                # rollback され、この行も残らない。
                _insert_operation(
                    conn,
                    operation_id=operation_id,
                    actor_id=actor_id,
                    kind=kind,
                    payload_hash=payload_hash,
                    business_key=business_key,
                    status=OperationStatus.SUCCEEDED,
                    result=None,
                    now_us=now_us,
                )
                if payment_kind is not None:
                    payment_payload_hash = _payload_hash(
                        {**payload, "derived": "payment"}
                    )
                    _insert_operation(
                        conn,
                        operation_id=f"{operation_id}:payment",
                        actor_id=actor_id,
                        kind=payment_kind.value,
                        payload_hash=payment_payload_hash,
                        business_key=None,
                        status=OperationStatus.SUCCEEDED,
                        result=None,
                        now_us=now_us,
                    )
                data = apply_effects(conn, operation_id, now_us)
                conn.execute(
                    "UPDATE operations SET result = ? WHERE operation_id = ?",
                    (
                        json.dumps(data, ensure_ascii=False, sort_keys=True),
                        operation_id,
                    ),
                )
                return CommandResult(data=data, operation_id=operation_id, replayed=False)
        except sqlite3.OperationalError as exc:
            if db.is_db_busy(exc) and attempt < DB_BUSY_MAX_ATTEMPTS - 1:
                delay = min(
                    DB_BUSY_BASE_DELAY_SECONDS * (2**attempt),
                    DB_BUSY_MAX_DELAY_SECONDS,
                ) * (0.5 + random.random())
                time.sleep(delay)
                continue
            raise OjpError(
                ErrorCode.DB_BUSY, f"database is busy after {attempt + 1} attempts"
            ) from exc
    raise OjpError(ErrorCode.DB_BUSY, "database is busy")  # pragma: no cover


# ---------------------------------------------------------------------------
# Root funding
# ---------------------------------------------------------------------------


def fund_root(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    root_id: str,
    requester_id: str,
    expected_amount_units: int,
    amount_units: int,
    operation_id: str | None = None,
    escrow: ledger.EscrowPort | None = None,
) -> CommandResult:
    """Root funding（計画書 第9節: Root fund 100）。

    ドメイン側が決めるのは、正確な全額入金であること・seed 済み Requester
    Wallet が原資であること・business_key fund:{root_id} と operation_id の
    冪等性。Requester・公開予算・asset は呼出側の申告を信用せず DB
    （jobs / job_versions）から解決する:

    - 対象 Job が Root（parent_id IS NULL かつ root_id = id）でなければ
      INVALID_TARGET
    - requester_id / expected_amount_units が DB の正本と違えば FORBIDDEN
      （呼出側の値を黙って採用しない）
    - asset は job_versions.asset が mock-USDC であることを要求する

    実行（Wallet 減額と Escrow available 増額の Journal 確定）は
    EscrowPort.fund が呼出側の transaction 内で行う（計画書 第15節:
    「ドメインが『誰へ、いくら、何を根拠に』を決め、port が実行する」）。
    escrow を省略した場合は MockEscrow を使う（process_payments 等と同じ形）。
    """
    escrow_port = escrow if escrow is not None else ledger.MockEscrow()
    payload = {
        "root_id": root_id,
        "requester_id": requester_id,
        "expected_amount_units": expected_amount_units,
        "amount_units": amount_units,
    }

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        target = ledger.resolve_root_funding_target(c, root_id)
        if requester_id != target.requester_id:
            raise OjpError(
                ErrorCode.FORBIDDEN,
                "requester_id does not match the root job's requester:"
                f" got={requester_id}, expected={target.requester_id}"
                " (原資は DB の jobs.requester_id から解決する)",
            )
        if expected_amount_units != target.budget_units:
            raise OjpError(
                ErrorCode.FORBIDDEN,
                "expected_amount_units does not match the published budget:"
                f" got={expected_amount_units}, expected={target.budget_units}"
                " (公開予算は DB の job_versions.budget_units が正本)",
            )
        if target.asset != ledger.ASSET_MOCK_USDC:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"unsupported asset in the published version: {target.asset!r}",
            )
        return escrow_port.fund(
            c,
            op_id,
            ledger.FundPayload(
                operation_id=op_id,
                root_id=root_id,
                requester_id=target.requester_id,
                expected_amount_units=target.budget_units,
                amount_units=amount_units,
                asset=target.asset,
                business_key=f"fund:{root_id}",
            ),
        )

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="fund",
        operation_id=operation_id,
        business_key=f"fund:{root_id}",
        payload=payload,
        apply_effects=_apply,
    )


# ---------------------------------------------------------------------------
# 100→10 の操作一覧（計画書 第9節）の予約系サービス
# ---------------------------------------------------------------------------


def allocate_child_work(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    root_id: str,
    child_id: str,
    amount_units: int,
    operation_id: str | None = None,
) -> CommandResult:
    """Child create 10（available -10 / child_work +10、business_key allocate:{child_id}）。"""
    payload = {"root_id": root_id, "child_id": child_id, "amount_units": amount_units}
    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="allocate",
        operation_id=operation_id,
        business_key=f"allocate:{child_id}",
        payload=payload,
        apply_effects=lambda c, op_id, now: ledger.allocate_child_work_in_tx(
            c,
            root_id=root_id,
            child_id=child_id,
            amount_units=amount_units,
            operation_id=op_id,
            now_us=now,
        ),
    )


def reserve_child_payout(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    root_id: str,
    child_id: str,
    amount_units: int,
    payee_id: str,
    operation_id: str | None = None,
) -> CommandResult:
    """Child approval（child_work -10 / child_payout +10）と送金予約の確定。

    承認の transaction で資金予約（locked への移動）と PaymentOperation
    （PENDING、原資と受取人は作成時に固定）をまとめて確定する
    （計画書 第9節 1）。残額 0 なら支払い Operation を作らず正常な no-op。
    business_key は payout:{child_id}。異なる operation_id で同じ Child の
    支払いを再確定しようとしても既存の結果を返す（二重送金しない）。
    """
    payload = {
        "root_id": root_id,
        "child_id": child_id,
        "amount_units": amount_units,
        "payee_id": payee_id,
    }

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        data = ledger.child_approval_in_tx(
            c,
            root_id=root_id,
            child_id=child_id,
            amount_units=amount_units,
            operation_id=op_id,
            now_us=now,
        )
        return _attach_payment_operation(
            c,
            data=data,
            operation_id=op_id,
            business_key=f"payout:{child_id}",
            root_id=root_id,
            job_id=child_id,
            payee_id=payee_id,
            kind=PaymentKind.PAYOUT,
        )

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="payout",
        operation_id=operation_id,
        business_key=f"payout:{child_id}",
        payload=payload,
        apply_effects=_apply,
        reuse_existing_payment=True,
        payment_kind=PaymentKind.PAYOUT,
    )


def _attach_payment_operation(
    conn: sqlite3.Connection,
    *,
    data: dict[str, Any],
    operation_id: str,
    business_key: str,
    root_id: str,
    job_id: str,
    payee_id: str,
    kind: PaymentKind,
) -> dict[str, Any]:
    """資金予約の結果に PaymentOperation を確定して結果へ添える。

    予約が no-op（残額 0）なら支払い Operation は作らない（計画書 第16節）。
    PaymentOperation の operation_id はコマンドの operation_id に `:payment`
    を付けた派生 ID（計画書 第16節「一連の処理から生じる各資金移動には
    派生 ID を割り当てる」）。予約の Journal と送金の PaymentOperation で
    同じ operation_id を共有すると journal_transactions.operation_id の
    PRIMARY KEY に衝突するため。
    """
    if data.get("no_op"):
        return data
    source_key = {
        Bucket.CHILD_PAYOUT: "child_payout_account_id",
        Bucket.PARENT_PAYOUT: "parent_payout_account_id",
        Bucket.REFUND: "refund_account_id",
    }
    account_id = next(
        data[key] for key in source_key.values() if key in data
    )
    payment_operation_id = f"{operation_id}:payment"
    payment = ledger.create_payment_operation_in_tx(
        conn,
        operation_id=payment_operation_id,
        business_key=business_key,
        root_id=root_id,
        job_id=job_id,
        source_account_id=account_id,
        amount_units=int(data["amount_units"]),
        payee_id=payee_id,
        kind=kind,
    )
    return {
        **data,
        "payment_operation_id": payment.operation_id,
        "payment_status": payment.status.value,
    }


def return_child_work(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    root_id: str,
    child_id: str,
    amount_units: int,
    operation_id: str | None = None,
    parent_terminal_refund_reserved: bool = False,
    child_done_payment_pending: bool = False,
) -> CommandResult:
    """Child failure（child_work -10 / available +10、business_key return:{child_id}）。

    Parent 失敗後（parent_terminal_refund_reserved=True）なら
    child_work → available → refund を 1 transaction で記録し、
    business_key refund:{root_id}:child-return:{child_id} で結合する。
    Child DONE かつ送金障害中（child_done_payment_pending=True）は禁止。
    """
    payload = {
        "root_id": root_id,
        "child_id": child_id,
        "amount_units": amount_units,
        "parent_terminal_refund_reserved": parent_terminal_refund_reserved,
        "child_done_payment_pending": child_done_payment_pending,
    }
    business_key = (
        f"refund:{root_id}:child-return:{child_id}"
        if parent_terminal_refund_reserved
        else f"return:{child_id}"
    )
    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="return",
        operation_id=operation_id,
        business_key=business_key,
        payload=payload,
        apply_effects=lambda c, op_id, now: ledger.child_failure_return_in_tx(
            c,
            root_id=root_id,
            child_id=child_id,
            amount_units=amount_units,
            operation_id=op_id,
            now_us=now,
            parent_terminal_refund_reserved=parent_terminal_refund_reserved,
            child_done_payment_pending=child_done_payment_pending,
        ),
    )


def reserve_parent_payout(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    root_id: str,
    amount_units: int,
    payee_id: str,
    operation_id: str | None = None,
) -> CommandResult:
    """Parent approval（available -90 / parent_payout +90）と送金予約の確定。

    business_key は payout:{root_id}。残額 0 なら支払い Operation を作らず
    正常な no-op。異なる operation_id の再確定は既存の結果を返す。
    """
    payload = {"root_id": root_id, "amount_units": amount_units, "payee_id": payee_id}

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        data = ledger.parent_approval_in_tx(
            c,
            root_id=root_id,
            amount_units=amount_units,
            operation_id=op_id,
            now_us=now,
        )
        return _attach_payment_operation(
            c,
            data=data,
            operation_id=op_id,
            business_key=f"payout:{root_id}",
            root_id=root_id,
            job_id=root_id,
            payee_id=payee_id,
            kind=PaymentKind.PAYOUT,
        )

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="payout",
        operation_id=operation_id,
        business_key=f"payout:{root_id}",
        payload=payload,
        apply_effects=_apply,
        reuse_existing_payment=True,
        payment_kind=PaymentKind.PAYOUT,
    )


def reserve_parent_refund(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    root_id: str,
    amount_units: int,
    operation_id: str | None = None,
    payee_id: str | None = None,
) -> CommandResult:
    """Parent failure（available -90 / refund +90）と返金予約の確定。

    business_key は refund:{root_id}:terminal。payee_id（Root Requester）を
    渡すと返金の PaymentOperation を同じ transaction で確定する（S2 の
    返金経路。送金処理は process_payments）。残額 0 なら支払い Operation を
    作らず正常な no-op。異なる operation_id の再確定は既存の結果を返す。
    """
    payload = {
        "root_id": root_id,
        "amount_units": amount_units,
        "payee_id": payee_id,
    }

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        data = ledger.parent_failure_refund_in_tx(
            c,
            root_id=root_id,
            amount_units=amount_units,
            operation_id=op_id,
            now_us=now,
        )
        if payee_id is None:
            return data
        return _attach_payment_operation(
            c,
            data=data,
            operation_id=op_id,
            business_key=f"refund:{root_id}:terminal",
            root_id=root_id,
            job_id=root_id,
            payee_id=payee_id,
            kind=PaymentKind.REFUND,
        )

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="refund" if payee_id is not None else "reserve",
        operation_id=operation_id,
        business_key=f"refund:{root_id}:terminal",
        payload=payload,
        apply_effects=_apply,
        reuse_existing_payment=payee_id is not None,
        payment_kind=PaymentKind.REFUND if payee_id is not None else None,
    )


# ---------------------------------------------------------------------------
# 共通 settlement 処理（計画書 第9節 1〜5）
# ---------------------------------------------------------------------------

# 送金失敗として記録する例外。OjpError（DB 制約のドメイン変換・Receipt 正本
# との照合違反を含む）と ledger.PaymentFailedError（Escrow port の失敗）の
# 両方を記録対象として扱う。MemoryError や KeyboardInterrupt 等は捕捉しない。
# 失敗は種類にかかわらず記録される（計画書 第9節: 有限回で打ち切らず、
# 失敗理由と予約金を可視化する）。
_PAYMENT_FAILURE_ERRORS = (OjpError, ledger.PaymentFailedError)


def _check_receipt_matches_reservation(
    conn: sqlite3.Connection,
    payment: Any,
    receipt: ledger.TransferReceipt | None,
) -> str | None:
    """Receipt の存在と金額・受取人・原資の一致を照合する。

    Receipt が正本という原則に反する状態（SUCCEEDED なのに Receipt が無い、
    Receipt の 3 属性が予約と食い違う）を成功として返さないための検査。
    違反があれば理由の文言を返し、無ければ None。
    """
    if receipt is None:
        return (
            f"payment {payment.operation_id} is {payment.status.value}"
            " but the receipt (正本) is missing"
        )
    mismatches: list[str] = []
    if receipt.amount_units != payment.amount_units:
        mismatches.append(
            f"amount_units: receipt={receipt.amount_units}"
            f" reservation={payment.amount_units}"
        )
    if receipt.payee_id != payment.payee_id:
        mismatches.append(
            f"payee_id: receipt={receipt.payee_id} reservation={payment.payee_id}"
        )
    if receipt.source_account_id != payment.source_account_id:
        mismatches.append(
            "source_account_id: receipt="
            f"{receipt.source_account_id} reservation={payment.source_account_id}"
        )
    if mismatches:
        return (
            "existing receipt does not match the reservation"
            f" (operation_id={payment.operation_id}): " + "; ".join(mismatches)
        )
    return None


def _payload_for_payment(payment: Any) -> ledger.TransferPayload:
    """PaymentOperation 作成時に固定した原資・受取人・金額から immutable
    payload を再構成する。settlement が金額・受取人を変えられないことを
    型で示す（payload の属性はすべて PaymentOperation 由来）。"""
    return ledger.TransferPayload(
        operation_id=payment.operation_id,
        amount_units=payment.amount_units,
        payee_id=payment.payee_id,
        source_account_id=payment.source_account_id,
        business_key=payment.business_key,
    )


def process_single_payment(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    escrow: ledger.EscrowPort | None = None,
) -> CommandResult:
    """1 件の PaymentOperation を settlement する（共通処理の単位）。

    - PENDING / RETRYABLE を取得し、同じ operation_id で Escrow port の
      transfer を呼ぶ（計画書 第9節 2）
    - Receipt が無いのに paid/refunded を増やさない（Mock が Receipt 正本）
    - 結果を記録する前に停止しても、再起動後に lookup で Receipt を照会して
      SUCCEEDED へ収束させる（第9節 4）
    - DB commit 前の障害は全体 rollback。commit 後の応答消失は Receipt が正本
    - 再試行で Job を FAILED にしない（有限回で打ち切らない。本モジュールは
      Job 状態を一切触らない）
    - SUCCEEDED 済みの再実行は Receipt の存在と金額・受取人・原資の一致を
      必ず照合する（Receipt が正本）。欠落・不一致は成功として返さず、
      理由を last_error に記録する（既に SUCCEEDED へ確定した整合状態は
      後から書き換えず、運用者が内容を確認して是正する）
    - 未確定の送金の失敗は種類にかかわらず attempt_count・last_error を
      記録し、status=RETRYABLE・next_retry_at_us をバックオフで設定する
      （返金への切替は行わない）
    """
    escrow_port = escrow if escrow is not None else ledger.MockEscrow()

    def _record_failure(c: sqlite3.Connection, reason: str) -> CommandResult:
        now = clock.now_for_write_transaction(c)
        updated = ledger.mark_payment_retryable_in_tx(
            c,
            operation_id=operation_id,
            error_message=reason,
            now_us=now,
        )
        return CommandResult(
            data={
                "operation_id": operation_id,
                "payment_status": updated.status.value,
                "attempt_count": updated.attempt_count,
                "next_retry_at_us": updated.next_retry_at_us,
                "last_error": updated.last_error,
            },
            operation_id=operation_id,
        )

    def _record_integrity_violation(
        c: sqlite3.Connection, payment: Any, reason: str
    ) -> CommandResult:
        """Receipt 正本との整合違反を last_error に記録して可視化する。

        既に SUCCEEDED へ確定した整合状態は後から書き換えないため、
        status は据え置いたまま、違反理由の記録（last_error）と件ごとの
        結果への報告（consistency_error）だけを行う。成功としては返さない。
        """
        now = clock.now_for_write_transaction(c)
        if payment.status == PaymentStatus.SUCCEEDED:
            updated = ledger.record_payment_error_in_tx(
                c, operation_id=operation_id, error_message=reason
            )
        else:
            updated = ledger.mark_payment_retryable_in_tx(
                c, operation_id=operation_id, error_message=reason, now_us=now
            )
        return CommandResult(
            data={
                "operation_id": operation_id,
                "payment_status": updated.status.value,
                "attempt_count": updated.attempt_count,
                "next_retry_at_us": updated.next_retry_at_us,
                "last_error": updated.last_error,
                "consistency_error": True,
            },
            operation_id=operation_id,
        )

    for attempt in range(DB_BUSY_MAX_ATTEMPTS):
        try:
            with db.transaction(conn, immediate=True):
                payment = ledger.get_payment_operation(conn, operation_id)
                if payment is None:
                    raise OjpError(
                        ErrorCode.INVALID_STATE,
                        f"payment operation not found: {operation_id}",
                    )
                # (b) commit 後の応答消失の注入点。send 系を別 transaction で
                # 確定させた直後・この status 更新 transaction の commit 前に
                # 発火させると、「Receipt は commit 済み・PaymentOperation は
                # 未更新」の中断状態になる（再起動後の収束の検証用）。
                ledger._fire_failpoint(
                    conn, ledger.failpoint_after_commit, "after_commit"
                )
                if payment.status == PaymentStatus.SUCCEEDED:
                    # 冪等: Receipt の存在と金額・受取人・原資の一致を必ず
                    # 照合する（Receipt が正本。Wallet・Journal は増えない）。
                    # 欠落・不一致は成功として返さず、理由を last_error に
                    # 記録して可視化する（整合性エラーとして扱う）。
                    receipt = escrow_port.lookup(conn, operation_id)
                    violation = _check_receipt_matches_reservation(
                        conn, payment, receipt
                    )
                    if violation is not None:
                        return _record_integrity_violation(
                            conn, payment, violation
                        )
                    assert receipt is not None
                    return CommandResult(
                        data={
                            "operation_id": operation_id,
                            "payment_status": PaymentStatus.SUCCEEDED.value,
                            "receipt_id": receipt.receipt_id,
                            "already_succeeded": True,
                        },
                        operation_id=operation_id,
                        replayed=True,
                    )
                try:
                    receipt = escrow_port.transfer(
                        conn, operation_id, _payload_for_payment(payment)
                    )
                except _PAYMENT_FAILURE_ERRORS as exc:
                    # 失敗は種類にかかわらず記録する（Receipt 正本との照合
                    # 違反を含む）。返金への切替・Job の FAILED 化は行わず、
                    # 失敗理由と予約金を可視化する。
                    return _record_failure(conn, str(exc))
                updated = ledger.mark_payment_succeeded_in_tx(
                    conn, operation_id=operation_id, receipt_id=receipt.receipt_id
                )
                return CommandResult(
                    data={
                        "operation_id": operation_id,
                        "payment_status": updated.status.value,
                        "receipt_id": receipt.receipt_id,
                        "amount_units": receipt.amount_units,
                        "payee_id": receipt.payee_id,
                    },
                    operation_id=operation_id,
                )
        except sqlite3.OperationalError as exc:
            if db.is_db_busy(exc) and attempt < DB_BUSY_MAX_ATTEMPTS - 1:
                delay = min(
                    DB_BUSY_BASE_DELAY_SECONDS * (2**attempt),
                    DB_BUSY_MAX_DELAY_SECONDS,
                ) * (0.5 + random.random())
                time.sleep(delay)
                continue
            raise OjpError(
                ErrorCode.DB_BUSY, f"database is busy after {attempt + 1} attempts"
            ) from exc
    raise OjpError(ErrorCode.DB_BUSY, "database is busy")  # pragma: no cover


def process_payments(
    conn: sqlite3.Connection,
    *,
    escrow: ledger.EscrowPort | None = None,
    limit: int | None = None,
) -> list[CommandResult]:
    """共通 settlement 処理（計画書 第15節 Lifecycle: process_payments）。

    PENDING / RETRYABLE（期限到来分）を取得し、同じ operation_id で Escrow
    port の transfer を呼ぶ。1 件ずつ独立した transaction で処理し、1 件の
    失敗で他の PaymentOperation の処理を止めない。送金失敗は
    process_single_payment 内で RETRYABLE として記録されて件ごとの結果へ
    反映される。記録不能な予期せぬ失敗（RuntimeError 等）だけが
    呼出側へ伝播する。
    """
    escrow_port = escrow if escrow is not None else ledger.MockEscrow()
    now_us = clock.now_for_read_snapshot(conn)
    due = ledger.list_due_payment_operations(conn, now_us=now_us)
    if limit is not None:
        due = due[:limit]
    results: list[CommandResult] = []
    for payment in due:
        results.append(
            process_single_payment(
                conn, operation_id=payment.operation_id, escrow=escrow_port
            )
        )
    return results


def retry_payment(
    conn: sqlite3.Connection,
    *,
    operation_id: str,
    escrow: ledger.EscrowPort | None = None,
) -> CommandResult:
    """CLI `ojp payment retry OPERATION` 相当: 同じ支払い ID を再処理する。

    金額・受取人・原資は PaymentOperation 作成時の固定値からしか取らない
    （payload を外部から受け取らない）。next_retry_at_us を無視して即時
    再試行する手動経路であり、バックオフ自体は変えない。SUCCEEDED 済み
    なら Receipt の存在と金額・受取人・原資の一致を照合する
    （Receipt が正本。欠落・不一致は成功として返さない）。
    """
    return process_single_payment(conn, operation_id=operation_id, escrow=escrow)
