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

from . import clock, db, domain, ledger, verification
from .domain import (
    Bucket,
    ErrorCode,
    JobState,
    LeaseClosedReason,
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
    "create": "create",
    "claim": "claim",
    "heartbeat": "heartbeat",
    "expiry": "expiry",
    "abandon": "abandon",
    "submit": "submit",
    "approve": "approve",
    "dispute": "dispute",
    "auto-approve": "auto-approve",
    "resolve": "resolve",
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
    """SQLite の制約違反を正常なドメイン競合へ変換する（計画書 第16節）。

    生の sqlite3.IntegrityError を呼出側へ漏らさない。leases の
    「有効 Lease は Job 当たり最大 1」「UNIQUE(job_id, generation)」の
    制約違反は、同時 Claim・再 Claim 競合として CLAIM_CONFLICT へ変換する。
    """
    message = str(exc)
    if "leases_one_active_per_job" in message or "leases.job_id" in message:
        return OjpError(
            ErrorCode.CLAIM_CONFLICT,
            f"lease conflict (another active lease won the job): {message}",
        )
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


def _reject_operation_id_reuse(
    conn: sqlite3.Connection,
    *,
    operation_id: str | None,
    actor_id: str,
    kind: str,
    payload: dict[str, Any],
) -> None:
    """既存 operations 行との照合を前置検査より先に確定する（計画書 第16節
    「operation_id UNIQUE＋actor/kind/payload_hash照合。同ID別payloadや別
    Actorでの再利用はIDEMPOTENCY_CONFLICT」）。

    approve のような業務キー再利用（reuse_existing_payment）の前置検査を
    持つコマンドでは、前置検査が _run_idempotent の照合より先に動くと、
    同じ operation_id を別 Actor / 別 payload で再利用したとき所定の
    IDEMPOTENCY_CONFLICT ではなく FORBIDDEN / INVALID_TARGET に化ける。
    それを防ぐため、前置検査の先頭（ドメイン検査より前）で呼び、
    operation_id 照合を常に最優先にする。

    - operation_id が None（呼出側で未指定）なら何もしない
      （_run_idempotent が新規採番するため照合対象が存在しない）
    - operations に行が無ければ何もしない（新規の operation_id）
    - actor_id / kind / payload_hash のいずれかが不一致なら
      OjpError(IDEMPOTENCY_CONFLICT)。比較は _run_idempotent と同じ
      （payload は _run_idempotent へ渡すものと同一の dict を渡し、
      同じ _payload_hash(payload) で照合する）
    - 一致する場合は何もしない（_run_idempotent の replay 経路に任せる）
    """
    if operation_id is None:
        return
    existing = conn.execute(
        "SELECT actor_id, kind, payload_hash FROM operations"
        " WHERE operation_id = ?",
        (operation_id,),
    ).fetchone()
    if existing is None:
        return
    if (
        existing["actor_id"] != actor_id
        or existing["kind"] != kind
        or existing["payload_hash"] != _payload_hash(payload)
    ):
        raise OjpError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "operation_id was already used with a different actor/kind/payload",
        )


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
# Root 作成（計画書 第5節 Job / JobVersion）
# ---------------------------------------------------------------------------


def create_root(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    title: str,
    budget: str,
    deadline_us: int,
    subcontract_policy: domain.SubcontractPolicy,
    task_catalog: list[domain.TaskCatalogEntry],
    timing_policy: domain.TimingPolicy | None = None,
    artifact_access_policy: domain.ArtifactAccessPolicy | None = None,
    input_json: str | None = None,
    verifier_id: str | None = None,
    verifier_hash: str | None = None,
    conditions_json: str | None = None,
    conditions_hash: str | None = None,
    operation_id: str | None = None,
) -> CommandResult:
    """Root Job（DRAFT）と公開 JobVersion（version=1）を 1 つの書込
    transaction で作成する（計画書 第5節 Job / JobVersion）。

    - budget は6桁小数文字列を domain.parse_amount_units で units 化する
      （正の整数。ゼロ・負・文字列以外は拒否）。asset は mock-USDC のみ
    - deadline_us は作成時点の now より未来であることを要求する
      （now >= deadline は拒否。第7節: 期限ちょうどは失効側）
    - subcontract_policy の値域は第10節（max_ratio_bps 0〜10000、
      max_amount_units 非負、max_children 非負整数、max_depth 0 または 1）
      を domain.SubcontractPolicy の型で強制し、**金額制約
      （max_amount_units）と比率制約（max_ratio_bps）の両方の指定を必須**
      にする。どちらもこの payload では省略不可なので、型検証済みの値が
      そのまま「両方指定あり」を意味する。値域違反・欠落は
      INVALID_ARGUMENT（pydantic ValidationError を変換）
    - timing_policy は domain.TimingPolicy の実値（既定: Lease 60秒 /
      heartbeat 目安20秒 / 検収待ち30秒 / Dispute 判定待ち30秒）を JSON で
      保存する。以後 Lease 長・検収期限はこの保存値から読み、コード内の
      定数を直接使わない（第7節: JobVersion に実際の値を保存する）
    - JobVersion は公開後 immutable（migration 004 のトリガーが UPDATE /
      DELETE を DB レベルで拒否する）
    - task_catalog 内の task_key 重複は INVALID_ARGUMENT で拒否する。
      カタログは task_key で引く表（第11節）であり、重複があると Child
      作成時のカタログ導出が一意に決まらない
    - Root 作成は資金移動の業務効果を持たないので business_key を持たない
      （business_key は二重拘束・二重払い・二重返金の防止のためのもので、
      第16節の表は fund / allocate / return / payout / refund にだけ
      定義する）。再送の冪等性は operation_id が担う（第16節の二段階
      一意性のうち再送 ID 側だけが該当する）
    """
    try:
        budget_units = domain.parse_amount_units(budget)
    except domain.MoneyError:
        raise
    # subcontract_policy の値域検証（計画書 第10節）。呼出側が検証を
    # バイパスして構築したポリシー（model_construct 等）でもサービス境界で
    # 必ず再検証する。金額制約（max_amount_units）と比率制約
    # （max_ratio_bps）の両方の指定を必須にする（この payload ではどちらも
    # 省略不可のため、再検証済みの値がそのまま「両方指定あり」を意味する）。
    # 値域違反・欠落は INVALID_ARGUMENT で拒否する。
    try:
        subcontract_policy = domain.SubcontractPolicy.model_validate(
            subcontract_policy.model_dump(),
            strict=True,
        )
    except Exception as exc:  # pydantic ValidationError（欠落・値域違反）
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"invalid subcontract_policy (第10節): {exc}",
        ) from exc
    try:
        task_catalog = [
            domain.TaskCatalogEntry.model_validate(entry.model_dump(), strict=True)
            for entry in task_catalog
        ]
    except Exception as exc:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT, f"invalid task_catalog: {exc}"
        ) from exc
    # task_key の重複は拒否する。カタログは task_key で引く表（第11節）であり、
    # 重複があると Child 作成時のカタログ導出が一意に決まらない。
    task_keys = [entry.task_key for entry in task_catalog]
    if len(task_keys) != len(set(task_keys)):
        duplicates = sorted({key for key in task_keys if task_keys.count(key) > 1})
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"task_catalog contains duplicate task_key: {duplicates}"
            " (カタログは task_key で引く表. 第11節)",
        )
    if not isinstance(deadline_us, int) or isinstance(deadline_us, bool):
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"deadline_us must be int, got {type(deadline_us).__name__}",
        )
    if deadline_us < 0:
        raise OjpError(ErrorCode.INVALID_ARGUMENT, "deadline_us must be non-negative")
    timing = timing_policy if timing_policy is not None else domain.TimingPolicy()
    access = (
        artifact_access_policy
        if artifact_access_policy is not None
        else domain.ArtifactAccessPolicy()
    )
    payload = {
        "title": title,
        "budget": budget,
        "deadline_us": deadline_us,
        "subcontract_policy": subcontract_policy.model_dump(),
        "task_catalog": [entry.model_dump() for entry in task_catalog],
        "timing_policy": timing.model_dump(),
        "artifact_access_policy": access.model_dump(),
        "input_json": input_json,
        "verifier_id": verifier_id,
        "verifier_hash": verifier_hash,
        "conditions_json": conditions_json,
        "conditions_hash": conditions_hash,
    }

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        # now は _run_idempotent が BEGIN IMMEDIATE 後に 1 回だけ採取した値。
        # 期限ちょうど（now == deadline）は失効側なので作成を拒否する（第7節）
        if now >= deadline_us:
            raise OjpError(
                ErrorCode.INVALID_ARGUMENT,
                f"deadline_us must be in the future: now={now} >= deadline={deadline_us}",
            )
        job_id = f"job:{op_id}"
        version_id = f"version:{job_id}:1"
        c.execute(
            "INSERT INTO jobs (id, root_id, parent_id, requester_id, state,"
            " row_version, created_at_us)"
            " VALUES (?, ?, NULL, ?, 'DRAFT', 0, ?)",
            (job_id, job_id, actor_id, now),
        )
        c.execute(
            "INSERT INTO job_versions (id, job_id, version, title, budget_units,"
            " asset, input_json, verifier_id, verifier_hash, conditions_json,"
            " conditions_hash, subcontract_policy, task_catalog, timing_policy,"
            " artifact_access_policy, deadline_us)"
            " VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                version_id,
                job_id,
                title,
                budget_units,
                domain.ASSET_MOCK_USDC,
                input_json,
                verifier_id,
                verifier_hash,
                conditions_json,
                conditions_hash,
                ledger.canonical_json_dumps(subcontract_policy.model_dump()),
                ledger.canonical_json_dumps(
                    [entry.model_dump() for entry in task_catalog]
                ),
                ledger.canonical_json_dumps(timing.model_dump()),
                ledger.canonical_json_dumps(access.model_dump()),
                deadline_us,
            ),
        )
        c.execute(
            "UPDATE jobs SET version_id = ? WHERE id = ?",
            (version_id, job_id),
        )
        return {
            "job_id": job_id,
            "version_id": version_id,
            "state": domain.JobState.DRAFT.value,
            "budget_units": budget_units,
            "deadline_us": deadline_us,
        }

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="create",
        operation_id=operation_id,
        business_key=None,
        payload=payload,
        apply_effects=_apply,
    )


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

    検査の順序は Root 性 → Actor 権限 → 申告 requester_id → 公開予算 →
    asset → Job 状態（DRAFT 必須）。権限のない Actor には Job の公開状態
    （Version の有無・公開予算）を返さないため、公開状態の解決は権限検査の
    後に行う。状態検査は「権限のない Actor に DRAFT/OPEN 等の状態を
    漏らさない位置」に挿すため、権限系の検査（Actor・申告・公開予算）が
    すべて通った後・asset 検査の後に置く（状態は DRAFT だけが合法であり、
    fund 成功のたびに OPEN へ 1 回だけ進む。第6節の表「DRAFT | Root の
    正確な全額入金が確定 | OPEN」）:

    - 対象 Job が Root（parent_id IS NULL かつ root_id = id）でなければ
      INVALID_TARGET
    - 権限: Root作成・入金・Root承認は Root Requester（第5節）。
      actor_id が DB の jobs.requester_id と一致しなければ FORBIDDEN
    - requester_id / expected_amount_units が DB の正本と違えば FORBIDDEN
      （呼出側の値を黙って採用しない）
    - asset は job_versions.asset が mock-USDC であることを要求する
    - jobs.state が DRAFT でなければ INVALID_STATE（未入金 Root は OPEN に
      ならず、OPEN 以降への二重 fund は business_key fund:{root_id} と
      operation 冪等性で既に 1 回だけ。この状態検査はその上で Claim 等の
      前提となる DRAFT→OPEN 遷移を記録する）

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
        # 1. Root 性（INVALID_TARGET）と Requester の解決。公開状態はまだ見ない
        root_requester_id = ledger.resolve_root_job(c, root_id)
        # 2. Actor 権限（FORBIDDEN）: 公開状態の解決より先に行う
        if actor_id != root_requester_id:
            raise OjpError(
                ErrorCode.FORBIDDEN,
                "actor is not the root requester"
                " (Root作成・入金・Root承認はRoot Requester. 第5節)",
            )
        # 3. 申告 requester_id の一致（FORBIDDEN）
        if requester_id != root_requester_id:
            raise OjpError(
                ErrorCode.FORBIDDEN,
                "requester_id does not match the root job's requester"
                " (原資は DB の jobs.requester_id から解決する)",
            )
        # 4. 公開予算の一致（FORBIDDEN）。権限検査後に公開状態を解決する
        budget_units, asset = ledger.resolve_root_published_budget(c, root_id)
        if expected_amount_units != budget_units:
            raise OjpError(
                ErrorCode.FORBIDDEN,
                "expected_amount_units does not match the published budget"
                " (公開予算は DB の job_versions.budget_units が正本)",
            )
        # 5. asset が mock-USDC（INVALID_STATE）
        if asset != ledger.ASSET_MOCK_USDC:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"unsupported asset in the published version: {asset!r}",
            )
        # 6. Job 状態（INVALID_STATE）: DRAFT のみ。権限系の検査をすべて
        #    通過した後に置く（権限のない Actor に状態を漏らさない）。
        #    読み取りと遷移（条件付き UPDATE）をこの transaction で直列化
        #    する（BEGIN IMMEDIATE の書込ロック内）。
        row = c.execute(
            "SELECT state, row_version FROM jobs WHERE id = ?", (root_id,)
        ).fetchone()
        assert row is not None  # resolve_root_job で存在確認済み
        if row["state"] != JobState.DRAFT.value:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "root job must be DRAFT to fund"
                f" (current state: {row['state']})."
                " 未入金 Root は OPEN にならず、二重 fund は"
                " fund:{root_id} で 1 回だけ（第6節・第16節）",
            )
        cursor = c.execute(
            "UPDATE jobs SET state = ?, row_version = row_version + 1"
            " WHERE id = ? AND state = ? AND row_version = ?",
            (
                JobState.OPEN.value,
                root_id,
                JobState.DRAFT.value,
                int(row["row_version"]),
            ),
        )
        if cursor.rowcount != 1:  # pragma: no cover - 書込ロック内では到達しない
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "root job state changed concurrently during fund",
            )
        return escrow_port.fund(
            c,
            op_id,
            ledger.FundPayload(
                operation_id=op_id,
                root_id=root_id,
                requester_id=root_requester_id,
                expected_amount_units=budget_units,
                amount_units=amount_units,
                asset=asset,
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
# Lease（計画書 第7節）: 共通判定と Claim / heartbeat
# ---------------------------------------------------------------------------


def _get_job_row(conn: sqlite3.Connection, job_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        raise OjpError(ErrorCode.INVALID_TARGET, f"job not found: {job_id}")
    return row


def _get_job_version_timing(
    conn: sqlite3.Connection, version_id: str
) -> tuple[domain.TimingPolicy, int]:
    """公開 JobVersion から timing_policy（実値）と deadline を読む。

    Lease 長・検収期限は JobVersion に保存した実値から読み、コード内の
    定数を直接使わない（計画書 第7節: JobVersion に実際の値を保存する）。
    """
    row = conn.execute(
        "SELECT timing_policy, deadline_us FROM job_versions WHERE id = ?",
        (version_id,),
    ).fetchone()
    if row is None:
        raise OjpError(
            ErrorCode.INVALID_TARGET, f"job version not found: {version_id}"
        )
    timing = domain.TimingPolicy.model_validate_json(row["timing_policy"])
    return timing, int(row["deadline_us"])


def _get_lease_row(conn: sqlite3.Connection, lease_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM leases WHERE id = ?", (lease_id,)).fetchone()
    if row is None:
        raise OjpError(ErrorCode.INVALID_TARGET, f"lease not found: {lease_id}")
    return row


def require_active_lease(
    conn: sqlite3.Connection,
    job_id: str,
    lease_id: str,
    generation: int,
    actor_id: str,
    now_us: int,
) -> domain.Lease:
    """「Lease を必要とする操作」から再利用する共通の Lease 有効性判定
    （計画書 第7節。Claim / heartbeat / abandon に加え、Phase 4 の submit
    もこの関数を使う前提）。

    検査の順序と対応コード（この順序のまま docstring にも固定する）:

    1. lease_id がその Job の Lease でなければ INVALID_TARGET
    2. closed_reason IS NOT NULL（閉じた Lease）なら LEASE_EXPIRED。
       **閉じた Lease を復活させない**（第7節）
    3. generation 不一致なら LEASE_EXPIRED（古い世代の Lease は失効扱い）
    4. worker_id != actor_id なら FORBIDDEN（Lease の Worker 本人のみ）
    5. now >= lease.expires_at_us または now >= job deadline なら
       LEASE_EXPIRED。**期限ちょうどは失効側を優先する**（第7節:
       now < deadline を有効、now >= deadline を失効とする）

    now は必ず「書込ロック取得後」に clock.now_for_write_transaction(conn)
    で 1 回だけ採取した値を呼出側から受け取る。クライアント時刻・
    time.time() を信用しない。
    """
    row = _get_lease_row(conn, lease_id)
    if row["job_id"] != job_id:
        raise OjpError(
            ErrorCode.INVALID_TARGET,
            f"lease {lease_id} does not belong to job {job_id}",
        )
    if row["closed_reason"] is not None:
        raise OjpError(
            ErrorCode.LEASE_EXPIRED,
            f"lease {lease_id} is already closed"
            f" (closed_reason={row['closed_reason']}). 閉じた Lease を復活させない",
        )
    if int(row["generation"]) != generation:
        raise OjpError(
            ErrorCode.LEASE_EXPIRED,
            f"lease generation mismatch: lease has {row['generation']},"
            f" got {generation}. 古い世代の Lease は失効扱い（第7節）",
        )
    if row["worker_id"] != actor_id:
        raise OjpError(
            ErrorCode.FORBIDDEN,
            f"actor {actor_id} is not the worker of lease {lease_id}",
        )
    job = _get_job_row(conn, job_id)
    if job["version_id"] is None:
        raise OjpError(
            ErrorCode.INVALID_STATE, f"job has no published version: {job_id}"
        )
    _, deadline_us = _get_job_version_timing(conn, job["version_id"])
    if now_us >= int(row["expires_at_us"]):
        raise OjpError(
            ErrorCode.LEASE_EXPIRED,
            f"lease expired: now={now_us} >= expires_at_us={row['expires_at_us']}",
        )
    if now_us >= deadline_us:
        raise OjpError(
            ErrorCode.LEASE_EXPIRED,
            f"job deadline passed: now={now_us} >= deadline_us={deadline_us}",
        )
    return domain.Lease.model_validate(dict(row))


def claim(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    job_id: str,
    expected_version_id: str,
    operation_id: str | None = None,
) -> CommandResult:
    """Claim（計画書 第7節）。Job を LEASED にし、有効 Lease を 1 件作る。

    BEGIN IMMEDIATE の transaction 内で、次を**再確認してから** Lease 作成と
    Job 更新を一緒に commit する（第7節。検査順序は状態 → 期限 → 有効 Lease
    → 公開版一致 → Requester 本人でないこと）:

    1. Job が OPEN（DRAFT / LEASED / 終端は INVALID_STATE。未入金 Root は
       OPEN にならないので DRAFT の Claim はここで拒否される。計画書 X01）
    2. now < deadline（now >= deadline は失効側。ここでは Job を EXPIRED に
       せず INVALID_STATE で拒否し、EXPIRED 化は expire_due_leases に任せる。
       Claim 側で EXPIRED へ書き換えると、claim と失効処理の両方が同じ
       遷移を持ち二重経路になるため、Lifecycle（expire_due_leases）に
       一本化する。第6節「OPEN + deadline 到来 → EXPIRED」）
    3. 有効 Lease（closed_reason IS NULL）が無い
    4. expected_version_id が jobs.version_id（公開版）と一致する。
       不一致は INVALID_TARGET
    5. actor_id が jobs.requester_id と一致するなら FORBIDDEN
       （Requester 本人は Claim できない。第7節の表「Requester本人でない
       Worker が Claim」）

    同時 Claim は DB の直列化で 1 件だけ成立する。敗者は条件付き UPDATE の
    更新行数 0（既に LEASED）または UNIQUE index
    leases_one_active_per_job / UNIQUE(job_id, generation) の制約違反から
    CLAIM_CONFLICT を受け取る。プロセス内 Lock には依存しない（第16節）。
    """
    payload = {"job_id": job_id, "expected_version_id": expected_version_id}

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        job = _get_job_row(c, job_id)
        # 1. Job が OPEN。LEASED（別の Claim が先に成立）はドメイン競合として
        #    CLAIM_CONFLICT にする（同時 Claim の敗者。計画書 第16節「同時
        #    Claim: 1件だけLease作成、他はCLAIM_CONFLICT」）。DRAFT（未入金）
        #    や終端状態は競合ではなく状態違反なので INVALID_STATE。
        if job["state"] == JobState.LEASED.value:
            raise OjpError(
                ErrorCode.CLAIM_CONFLICT,
                "job is already leased to another worker (同時 Claim の敗者)",
            )
        if job["state"] != JobState.OPEN.value:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"job must be OPEN to claim (current state: {job['state']})",
            )
        if job["version_id"] is None:
            raise OjpError(
                ErrorCode.INVALID_STATE, f"job has no published version: {job_id}"
            )
        # 2. now < deadline（期限ちょうどは失効側。EXPIRED 化は失効処理に任せる）
        timing, deadline_us = _get_job_version_timing(c, job["version_id"])
        if now >= deadline_us:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"job deadline passed: now={now} >= deadline_us={deadline_us}."
                " Job の EXPIRED 化は expire_due_leases に任せる（第6節）",
            )
        # 3. 有効 Lease が無い（同時実行の競合は下の UNIQUE index でも直列化）
        active = c.execute(
            "SELECT id FROM leases WHERE job_id = ? AND closed_reason IS NULL",
            (job_id,),
        ).fetchone()
        if active is not None:
            raise OjpError(
                ErrorCode.CLAIM_CONFLICT,
                f"job already has an active lease: {active['id']}",
            )
        # 4. 公開版の一致
        if expected_version_id != job["version_id"]:
            raise OjpError(
                ErrorCode.INVALID_TARGET,
                "expected_version_id does not match the published version"
                f" (published: {job['version_id']}, got: {expected_version_id})",
            )
        # 5. Requester 本人は Claim できない
        if actor_id == job["requester_id"]:
            raise OjpError(
                ErrorCode.FORBIDDEN,
                "the job requester cannot claim their own job"
                " (第7節: Requester本人でないWorkerがClaim)",
            )
        # generation はその Job の既存 Lease の最大 generation + 1（初回 1）。
        # 過去 Lease（閉じた Lease）も含めた最大値から採番するため、世代は
        # 単調に増え、UNIQUE(job_id, generation) と leases_one_active_per_job
        # の両方が競合の最後の砦になる。
        max_generation = c.execute(
            "SELECT COALESCE(MAX(generation), 0) AS g FROM leases WHERE job_id = ?",
            (job_id,),
        ).fetchone()["g"]
        generation = int(max_generation) + 1
        lease_id = f"lease:{op_id}"
        # expires_at = min(now + lease_seconds, job deadline)。heartbeat でも
        # この上限を越えない（第7節）
        expires_at_us = min(now + timing.lease_seconds * 1_000_000, deadline_us)
        try:
            c.execute(
                "INSERT INTO leases (id, job_id, worker_id, version_id, generation,"
                " claimed_at_us, heartbeat_at_us, expires_at_us, closed_reason)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                (
                    lease_id,
                    job_id,
                    actor_id,
                    job["version_id"],
                    generation,
                    now,
                    now,
                    expires_at_us,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise _map_integrity_error(exc) from exc
        # 条件付き UPDATE: OPEN かつ読み取った row_version の行だけを LEASED へ。
        # 更新行数 0 ならドメイン競合（別の Claim が先に成立）として扱う。
        cursor = c.execute(
            "UPDATE jobs SET state = ?, active_lease_id = ?,"
            " row_version = row_version + 1"
            " WHERE id = ? AND state = ? AND row_version = ?",
            (
                JobState.LEASED.value,
                lease_id,
                job_id,
                JobState.OPEN.value,
                int(job["row_version"]),
            ),
        )
        if cursor.rowcount != 1:
            raise OjpError(
                ErrorCode.CLAIM_CONFLICT,
                "job state changed concurrently during claim"
                " (another claim won the job)",
            )
        return {
            "job_id": job_id,
            "lease_id": lease_id,
            "generation": generation,
            "version_id": job["version_id"],
            "expires_at": expires_at_us,
            "state": JobState.LEASED.value,
        }

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="claim",
        operation_id=operation_id,
        business_key=None,
        payload=payload,
        apply_effects=_apply,
    )


def heartbeat(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    job_id: str,
    lease_id: str,
    generation: int,
    operation_id: str | None = None,
) -> CommandResult:
    """heartbeat（計画書 第7節）。Lease の expires_at を更新する。

    検査順序は require_active_lease の共通判定そのもの（1. lease_id がその
    Job の Lease → INVALID_TARGET、2. 閉じた Lease → LEASE_EXPIRED（復活
    させない）、3. generation 不一致 → LEASE_EXPIRED、4. Worker 本人でない
    → FORBIDDEN、5. Lease 期限・Job deadline 到来 → LEASE_EXPIRED）。
    heartbeat でも tick 未実行でも期限を検査する（第7節）。

    成功時: heartbeat_at_us = now、expires_at_us = min(now + lease_seconds,
    job deadline)。**heartbeat で Job deadline を越えない。**lease_seconds は
    JobVersion の timing_policy（保存された実値）から読む。
    """
    payload = {"job_id": job_id, "lease_id": lease_id, "generation": generation}

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        require_active_lease(c, job_id, lease_id, generation, actor_id, now)
        job = _get_job_row(c, job_id)
        assert job["version_id"] is not None
        timing, deadline_us = _get_job_version_timing(c, job["version_id"])
        new_expires_at_us = min(now + timing.lease_seconds * 1_000_000, deadline_us)
        cursor = c.execute(
            "UPDATE leases SET heartbeat_at_us = ?, expires_at_us = ?"
            " WHERE id = ? AND closed_reason IS NULL",
            (now, new_expires_at_us, lease_id),
        )
        if cursor.rowcount != 1:  # pragma: no cover - 共通判定済みなので到達しない
            raise OjpError(
                ErrorCode.LEASE_EXPIRED, f"lease was closed concurrently: {lease_id}"
            )
        return {
            "job_id": job_id,
            "lease_id": lease_id,
            "generation": generation,
            "expires_at": new_expires_at_us,
        }

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="heartbeat",
        operation_id=operation_id,
        business_key=None,
        payload=payload,
        apply_effects=_apply,
    )


# ---------------------------------------------------------------------------
# 失効処理と abandon（計画書 第6節・第7節・第14節・第15節 Lifecycle）
# ---------------------------------------------------------------------------

# 失効処理・abandon は system が tick から呼ぶ Lifecycle であり、通常の
# Actor 操作ではない。operations 行の actor_id は participants へ FK がある
# ため、Lifecycle の記録は system participant で行う（計画書 第14節
# 「tick／seedはsystem」）。system Actor の ID を決めるのは起動設定であり
# （第5節: サーバー起動設定から ActorContext を作る）、サービス層は
# 呼出側から受け取る。


def _is_system_actor(conn: sqlite3.Connection, actor_id: str) -> bool:
    """Actor が DB に登録された system Participant かどうか（True/False）。"""
    row = conn.execute(
        "SELECT kind FROM participants WHERE id = ?", (actor_id,)
    ).fetchone()
    return row is not None and row["kind"] == domain.ParticipantKind.SYSTEM.value


def _require_system_actor(conn: sqlite3.Connection, actor_id: str) -> None:
    """Lifecycle（expire_due_leases / reserve_refundable_balance）の呼出側が
    DB に登録された system Participant であることを検査する（計画書 第14節
    「tick／seedはsystem」。呼出側 scheduler／CLI だけに認可を任せない）。

    participants を actor_id で検索し、行が無い場合または kind != system の
    場合は FORBIDDEN。検査は呼出直後（対象の収集・既存 Operation の replay
    より前）と、各書込 transaction 内（状態変更や Operation 確定の前）の
    両方で行う。対象収集後に Participant の kind が変更される可能性を
    残さないため、transaction 内でもう一度検証する。
    """
    if not _is_system_actor(conn, actor_id):
        raise OjpError(
            ErrorCode.FORBIDDEN,
            f"actor {actor_id!r} is not a registered system participant"
            " (Lifecycle は system 専用. 第14節「tick／seedはsystem」)",
        )


def _active_lease_has_valid_submission(conn: sqlite3.Connection, job_id: str) -> bool:
    """有効提出（submissions 行）が存在するか。失効処理は有効提出が無い
    場合にだけ Job を EXPIRED にする（第6節「Lease失効またはJob deadline
    到来、有効提出なし」）。"""
    row = conn.execute(
        "SELECT 1 FROM submissions WHERE job_id = ?", (job_id,)
    ).fetchone()
    return row is not None


def _read_available_units(conn: sqlite3.Connection, root_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(SUM(amount_units), 0) AS total FROM budget_accounts"
        " WHERE root_id = ? AND bucket = ?"
        " AND (source_key IS NULL OR source_key != ?)",
        (root_id, Bucket.AVAILABLE.value, ledger.WALLET_LEDGER_SOURCE_KEY),
    ).fetchone()
    return int(row["total"])


def _read_child_work_units(conn: sqlite3.Connection, child_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(SUM(amount_units), 0) AS total FROM budget_accounts"
        " WHERE owner_job_id = ? AND bucket = ?",
        (child_id, Bucket.CHILD_WORK.value),
    ).fetchone()
    return int(row["total"])


def _close_lease_and_fail_job_effects(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    lease_row: sqlite3.Row | None,
    closed_reason: LeaseClosedReason,
    terminal_state: JobState,
    now: int,
) -> None:
    """Job 終端化の共通効果: Lease を閉じ、Job を終端状態にする。

    - Lease は closed_reason を設定して閉じる（閉じた Lease は復活しない）
    - Job は終端状態（FAILED / EXPIRED）へ進め、active_lease_id を外す。
      条件付き UPDATE（読み取った row_version に一致）で、同時実行による
      先の確定（有効提出等）があれば INVALID_STATE のドメイン競合にする
      （第7節: Submitとexpiryが競合した時は同じDB transaction境界で
      直列化する）
    """
    if lease_row is not None and lease_row["closed_reason"] is None:
        conn.execute(
            "UPDATE leases SET closed_reason = ? WHERE id = ?",
            (closed_reason.value, lease_row["id"]),
        )
    cursor = conn.execute(
        "UPDATE jobs SET state = ?, active_lease_id = NULL,"
        " row_version = row_version + 1"
        " WHERE id = ? AND row_version = ?",
        (terminal_state.value, job["id"], int(job["row_version"])),
    )
    if cursor.rowcount != 1:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            "job state changed concurrently during terminal transition"
            f" (job={job['id']}, target={terminal_state.value})",
        )


def _insert_refund_payment_operation(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    base_operation_id: str,
    business_key: str,
    root_id: str,
    job_id: str,
    source_account_id: str,
    amount_units: int,
    payee_id: str,
    now_us: int,
) -> domain.PaymentOperation:
    """返金予約に結合する PaymentOperation（PENDING）を同じ transaction で確定する。

    payment_operations が operations を FK で参照するため、派生 ID
    `<base_operation_id>:payment` の operations 行を先に挿入する
    （_run_idempotent の payment_kind 派生行と同じ規則。計画書 第16節
    「一連の処理から生じる各資金移動には派生 ID を割り当てる」）。
    """
    payment_operation_id = f"{base_operation_id}:payment"
    _insert_operation(
        conn,
        operation_id=payment_operation_id,
        actor_id=actor_id,
        kind=PaymentKind.REFUND.value,
        payload_hash=_payload_hash(
            {"derived_from": base_operation_id, "business_key": business_key}
        ),
        business_key=None,
        status=OperationStatus.SUCCEEDED,
        result=None,
        now_us=now_us,
    )
    return ledger.create_payment_operation_in_tx(
        conn,
        operation_id=payment_operation_id,
        business_key=business_key,
        root_id=root_id,
        job_id=job_id,
        source_account_id=source_account_id,
        amount_units=amount_units,
        payee_id=payee_id,
        kind=PaymentKind.REFUND,
    )


def _reserve_terminal_refund_with_payment_in_tx(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    actor_id: str,
    operation_id: str,
    now_us: int,
) -> dict[str, Any]:
    """Root 終端時の返金予約: 未拘束 available だけを refund:{root_id}:terminal
    の返金予約へ移し、返金の PaymentOperation を同じ transaction で確定する。

    計画書 第8節「Parent 終了時」の表の FAILED / EXPIRED 行。locked
    （child_work / child_payout / parent_payout）には触らない。available が
    0 のときは返金予約を作らない（正常な no-op。第16節）。同一の業務効果
    （business_key）の PaymentOperation が既にあれば新しい予約を作らず既存を
    返す（二重返金しない）。
    """
    root_id = str(job["id"])
    business_key = f"refund:{root_id}:terminal"
    existing = ledger.get_payment_operation_by_business_key(conn, business_key)
    if existing is not None:
        return {
            "root_id": root_id,
            "amount_units": existing.amount_units,
            "payment_operation_id": existing.operation_id,
            "payment_status": existing.status.value,
            "reused": True,
        }
    available_units = _read_available_units(conn, root_id)
    if available_units == 0:
        # 返金予約を作らない正常な no-op（計画書 第16節: 残額0なら支払い
        # Operation は作らない）
        return {"root_id": root_id, "amount_units": 0, "no_op": True}
    data = ledger.parent_failure_refund_in_tx(
        conn,
        root_id=root_id,
        amount_units=available_units,
        operation_id=operation_id,
        now_us=now_us,
    )
    payment = _insert_refund_payment_operation(
        conn,
        actor_id=actor_id,
        base_operation_id=operation_id,
        business_key=business_key,
        root_id=root_id,
        job_id=root_id,
        source_account_id=data["refund_account_id"],
        amount_units=available_units,
        payee_id=str(job["requester_id"]),
        now_us=now_us,
    )
    return {
        **data,
        "payment_operation_id": payment.operation_id,
        "payment_status": payment.status.value,
    }


def _child_terminal_fund_effects(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    actor_id: str,
    operation_id: str,
    now_us: int,
) -> dict[str, Any]:
    """Child 終端時の資金の後始末（計画書 第8節・第9節・第16節）。

    - Parent 生存中: child_work → available へ戻す
      （ledger.child_failure_return_in_tx の通常経路）。資金移動と同じ
      書込 transaction 内で、派生資金 operation_id に対応する operations 行
      （business_key return:{child_id}）を service 層から記録する。
      判定未確定の原資だけを戻す
    - Parent が FAILED / EXPIRED（終端）: child_work → available → refund の
      2 移動をこの 1 transaction（1 操作）で記録し、途中の available を外から
      再利用させない（第9節）。追加返金は business_key
      refund:{root_id}:child-return:{child_id} の PaymentOperation として
      確定する（refund:{root_id}:terminal とは別の原資キー）。
      **返金済み Parent の Job 状態は復活させない**（この関数は Parent の
      jobs 行を一切更新しない）
    - Parent DONE はここに含めない。Parent 提出は全 Child 判定終端が前提
      （第8節 CHILDREN_UNRESOLVED）なので、DONE の Parent に生存 Child は
      存在し得ず、戻し先は available でよい
    - child_work が 0 の no-op では新しい return:{child_id} を消費しない
      （operations 行を作らない。第16節の残額 0 no-op と同じ規則）
    """
    child_id = str(job["id"])
    root_id = str(job["root_id"])
    child_work_units = _read_child_work_units(conn, child_id)
    if child_work_units == 0:
        return {"root_id": root_id, "child_id": child_id, "amount_units": 0}
    parent = _get_job_row(conn, str(job["parent_id"]))
    parent_terminal = parent["state"] in {
        JobState.FAILED.value,
        JobState.EXPIRED.value,
    }
    if not parent_terminal:
        # Parent 生存中: child_work → available。派生資金 operation_id
        # （expiry:...:funds / abandon:...:funds）と同じ operation_id で
        # business_key return:{child_id} の operations 行を資金移動より先に
        # 挿入する（第16節の表「Child原資返却: return:{child_id}」。既存の
        # _insert_operation と migration 002 の operations_business_key_unique
        # を利用）。異なる operation_id で同じ return:{child_id} が使われて
        # いれば INVALID_STATE となり、transaction 全体が rollback されて
        # Journal・残高変更を残さない
        _insert_operation(
            conn,
            operation_id=operation_id,
            actor_id=actor_id,
            kind="return",
            payload_hash=_payload_hash(
                {
                    "root_id": root_id,
                    "child_id": child_id,
                    "amount_units": child_work_units,
                    "parent_terminal_refund_reserved": False,
                }
            ),
            business_key=f"return:{child_id}",
            status=OperationStatus.SUCCEEDED,
            result=None,
            now_us=now_us,
        )
        return ledger.child_failure_return_in_tx(
            conn,
            root_id=root_id,
            child_id=child_id,
            amount_units=child_work_units,
            operation_id=operation_id,
            now_us=now_us,
            parent_terminal_refund_reserved=False,
            child_done_payment_pending=False,
        )
    business_key = f"refund:{root_id}:child-return:{child_id}"
    existing = ledger.get_payment_operation_by_business_key(conn, business_key)
    if existing is not None:
        # 同一の追加返金は 1 回だけ（第16節）
        return {
            "root_id": root_id,
            "child_id": child_id,
            "amount_units": existing.amount_units,
            "payment_operation_id": existing.operation_id,
            "payment_status": existing.status.value,
            "reused": True,
        }
    data = ledger.child_failure_return_in_tx(
        conn,
        root_id=root_id,
        child_id=child_id,
        amount_units=child_work_units,
        operation_id=operation_id,
        now_us=now_us,
        parent_terminal_refund_reserved=True,
        child_done_payment_pending=False,
    )
    root = _get_job_row(conn, root_id)
    payment = _insert_refund_payment_operation(
        conn,
        actor_id=actor_id,
        base_operation_id=operation_id,
        business_key=business_key,
        root_id=root_id,
        job_id=child_id,
        source_account_id=data["refund_account_id"],
        amount_units=child_work_units,
        payee_id=str(root["requester_id"]),
        now_us=now_us,
    )
    return {
        **data,
        "payment_operation_id": payment.operation_id,
        "payment_status": payment.status.value,
    }


def _job_terminal_fund_effects(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    actor_id: str,
    operation_id: str,
    now: int,
) -> dict[str, Any]:
    """Job 終端時の資金の後始末（計画書 第6節・第8節の表）。

    - Child（parent_id IS NOT NULL）: _child_terminal_fund_effects。
      Parent 生存中なら child_work → available、Parent 終端後なら
      child_work → available → refund の追加返金を 1 transaction で確定する。
      Child が DONE で送金障害中（PENDING / RETRYABLE）なら失敗返却を
      禁止する（第9節。_child_done_with_pending_payment による DB 導出を
      含む。dispute FAIL 等の Phase 4 経路がこの禁止をすり抜けないように
      この関数内で判定する）
    - Root: _reserve_terminal_refund_with_payment_in_tx（= 計画書 第15節
      Lifecycle reserve_refundable_balance の終端行）。未拘束 available だけを
      refund:{root_id}:terminal の返金予約へ移し PaymentOperation を確定する。
      locked（child_work / child_payout / parent_payout）には触らない。
      **Parent 終端を Child へ伝播しない**: この関数は Child の jobs 行・
      child_work / child_payout を一切変更しない
    """
    if job["parent_id"] is not None:
        # Child が DONE で送金障害中は失敗返却を禁止する（第9節）。
        # _child_terminal_fund_effects は同じ transaction 内の Parent 状態
        # で Parent 生存中（return:{child_id}）と Parent 終端後
        # （refund:{root_id}:child-return:{child_id}）の両方の原資を動かす
        # ため、送金待ちの child_payout を誤って失敗側へ流さないように
        # ここで拒否する
        if _child_done_with_pending_payment(conn, str(job["id"])):
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "child is DONE with payment pending; failure return is"
                " forbidden (計画書 第9節)",
            )
        return _child_terminal_fund_effects(
            conn, job=job, actor_id=actor_id, operation_id=operation_id, now_us=now
        )
    return _reserve_terminal_refund_with_payment_in_tx(
        conn, job=job, actor_id=actor_id, operation_id=operation_id, now_us=now
    )


def _collect_expiry_targets(
    conn: sqlite3.Connection, now: int
) -> list[tuple[sqlite3.Row, sqlite3.Row | None, str]]:
    """失効対象を収集する（now はこの transaction で採取した 1 回の値）。

    対象（計画書 第6節・第7節）:

    - 対象 1: 有効 Lease で now >= expires_at_us。→ closed_reason='expired'
      にし、有効提出が無ければ Job を EXPIRED にする
    - 対象 2: OPEN で now >= deadline（Lease が無いまま期限到来）。
      → Job を EXPIRED
    - 対象 3: LEASED で now >= job deadline。→ Lease を閉じ Job を EXPIRED

    戻り値は (job 行, lease 行または None, 対象種別) の列。自動 OPEN 復帰・
    Worker 交代・新 Lease 作成はしない（対象の列挙にそもそも含めない）。
    """
    targets: list[tuple[sqlite3.Row, sqlite3.Row | None, str]] = []
    seen_jobs: set[str] = set()
    # 対象 1: 有効 Lease の期限切れ（LEASED の Job に紐づく）
    lease_rows = conn.execute(
        "SELECT l.* FROM leases l JOIN jobs j ON j.id = l.job_id"
        " WHERE l.closed_reason IS NULL AND l.expires_at_us <= ?"
        " ORDER BY l.id",
        (now,),
    ).fetchall()
    for lease_row in lease_rows:
        job = _get_job_row(conn, lease_row["job_id"])
        if job["state"] in {s.value for s in domain.TERMINAL_JOB_STATES}:
            continue
        targets.append((job, lease_row, "lease"))
        seen_jobs.add(job["id"])
    # 対象 2・3: Job deadline 到来（OPEN / LEASED）
    job_rows = conn.execute(
        "SELECT j.* FROM jobs j JOIN job_versions v ON v.id = j.version_id"
        " WHERE j.state IN (?, ?) AND v.deadline_us <= ?"
        " ORDER BY j.id",
        (JobState.OPEN.value, JobState.LEASED.value, now),
    ).fetchall()
    for job in job_rows:
        if job["id"] in seen_jobs:
            continue
        lease_row = None
        if job["active_lease_id"] is not None:
            lease_row = _get_lease_row(conn, job["active_lease_id"])
        targets.append((job, lease_row, "deadline"))
    return targets


def expire_due_leases(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    escrow: ledger.EscrowPort | None = None,
) -> list[CommandResult]:
    """期限到来分の失効処理（計画書 第15節 Lifecycle: expire_due_leases）。

    tick から呼ばれる想定。**tick 未実行でも Claim / heartbeat が期限を
    検査する**ため、失効の確定はこの Lifecycle が担う（第7節）。

    actor_id は起動設定が決める system Actor（第5節: サーバー起動設定から
    ActorContext を作る。第14節: tick／seedはsystem）。operations 行の
    actor_id は participants への FK があるため、呼出側（tick）が system
    participant の ID を渡す。service 層でも DB に登録された
    ParticipantKind.SYSTEM の Actor であることを検証する
    （_require_system_actor。呼出側 scheduler／CLI だけには任せない）。

    - 対象 1: 有効 Lease で now >= expires_at_us。→ closed_reason='expired'
      にし、有効提出が無ければ Job を EXPIRED にする
    - 対象 2: OPEN で now >= deadline（Lease が無いまま期限到来）。
      → Job を EXPIRED
    - 対象 3: LEASED で now >= job deadline。→ Lease を閉じ Job を EXPIRED
    - **自動 OPEN 復帰・Worker 交代・新 Lease 作成はしない**（第6節）
    - 資金の後始末（計画書 第8節の表。_job_terminal_fund_effects）:
      Root は未拘束 available のみ refund:{root_id}:terminal の返金予約へ移し
      PaymentOperation を確定する（locked には触らない）。Child は Parent
      生存中なら child_work → available、Parent 終端後なら
      child_work → available → refund の追加返金を 1 transaction で確定する
    - **Parent 終端を Child へ伝播しない**: Parent を EXPIRED にしても
      Child を自動 EXPIRED にしない。Child の Lease・期限処理は独立に継続する
      （Child は独自の deadline を持ち、Parent deadline を越えてもよい。第8節）
    - 自動処理の operation_id は第16節の規約に従い `expiry:{lease_id}`
      （Lease を伴わない Job 失効は `expiry:job:{job_id}`）。資金移動には
      派生 ID を割り当て、**同じ期限を二重処理しない**（再実行は冪等）
    - **認可は service 層で行う**: 呼出 Actor が DB 登録済みの system
      Participant でなければ FORBIDDEN（_require_system_actor）。期限対象の
      収集前に検査するため、対象が 0 件でも非system呼出しは空配列ではなく
      FORBIDDEN。各対象の書込 transaction 内でも、状態変更や Operation の
      確定前に再検証する（収集後に kind が変更される可能性を残さない）
    - 1 対象ずつ独立した _run_idempotent（BEGIN IMMEDIATE）で処理し、
      now は各 transaction 内で 1 回だけ採取する。DB_BUSY は
      _run_idempotent の再試行に任せる
    """
    del escrow  # 資金移動は同一 DB の内部移動のみ（Escrow port は使わない）
    # 認可は対象の収集より先（対象が 0 件でも非systemは FORBIDDEN）
    with db.transaction(conn, immediate=False):
        _require_system_actor(conn, actor_id)
    # 対象の収集は読み取りだけ（各対象の確定はそれぞれ独立した書込
    # transaction で行い、書込ロック内で再確認する）
    with db.transaction(conn, immediate=False):
        preview_now = clock.now_for_read_snapshot(conn)
        targets = _collect_expiry_targets(conn, preview_now)
    results: list[CommandResult] = []
    for job, lease_row, kind in targets:
        job_id = str(job["id"])
        if lease_row is not None:
            expiry_op_id = f"expiry:{lease_row['id']}"
        else:
            # Lease を伴わない Job 失効は衝突しない派生 ID にする（第16節）
            expiry_op_id = f"expiry:job:{job_id}"
        payload = {"job_id": job_id, "kind": kind}

        def _apply(
            c: sqlite3.Connection,
            op_id: str,
            now: int,
            _job_id: str = job_id,
            _lease_id: str | None = (
                str(lease_row["id"]) if lease_row is not None else None
            ),
        ) -> dict[str, Any]:
            # 書込 transaction 内で認可を再検証する（収集後に Participant の
            # kind が変更される可能性を残さない。状態変更や Operation の
            # 確定より前）
            _require_system_actor(c, actor_id)
            current = _get_job_row(c, _job_id)
            if current["state"] in {s.value for s in domain.TERMINAL_JOB_STATES}:
                # 既に終端（先に有効提出や別の失効が確定した）。何もしない
                return {"job_id": _job_id, "state": current["state"], "skipped": True}
            current_lease = (
                _get_lease_row(c, _lease_id) if _lease_id is not None else None
            )
            if current["state"] == JobState.OPEN.value:
                # 対象 2: OPEN + deadline 到来
                if current["version_id"] is None:
                    raise OjpError(
                        ErrorCode.INVALID_STATE,
                        f"job has no published version: {_job_id}",
                    )
                _, deadline_us = _get_job_version_timing(c, current["version_id"])
                if now < deadline_us:
                    return {
                        "job_id": _job_id,
                        "state": current["state"],
                        "skipped": True,
                    }
                _close_lease_and_fail_job_effects(
                    c,
                    job=current,
                    lease_row=current_lease,
                    closed_reason=LeaseClosedReason.EXPIRED,
                    terminal_state=JobState.EXPIRED,
                    now=now,
                )
            elif current["state"] == JobState.LEASED.value:
                if current_lease is None:
                    raise OjpError(
                        ErrorCode.INVALID_STATE,
                        f"leased job has no lease row: {_job_id}",
                    )
                # 対象 1: 有効 Lease の期限切れ / 対象 3: LEASED + deadline 到来。
                # どちらか一方でも到来していれば失効側（期限ちょうどは失効側）
                if current["version_id"] is None:
                    raise OjpError(
                        ErrorCode.INVALID_STATE,
                        f"job has no published version: {_job_id}",
                    )
                _, deadline_us = _get_job_version_timing(c, current["version_id"])
                lease_due = now >= int(current_lease["expires_at_us"])
                deadline_due = now >= deadline_us
                if not (lease_due or deadline_due):
                    return {
                        "job_id": _job_id,
                        "state": current["state"],
                        "skipped": True,
                    }
                if current_lease["closed_reason"] is not None:
                    return {
                        "job_id": _job_id,
                        "state": current["state"],
                        "skipped": True,
                    }
                # 有効提出があれば Job は EXPIRED にしない（Lease だけ閉じる
                # こともしない。提出済みの Lease は submit 側が閉じる）
                if _active_lease_has_valid_submission(c, _job_id):
                    return {
                        "job_id": _job_id,
                        "state": current["state"],
                        "skipped": True,
                    }
                _close_lease_and_fail_job_effects(
                    c,
                    job=current,
                    lease_row=current_lease,
                    closed_reason=LeaseClosedReason.EXPIRED,
                    terminal_state=JobState.EXPIRED,
                    now=now,
                )
            else:
                # OPEN / LEASED 以外（SUBMITTED / DISPUTED 等）はこの Lifecycle の
                # 対象外（提出後の進行は review / dispute 期限が担う）
                return {
                    "job_id": _job_id,
                    "state": current["state"],
                    "skipped": True,
                }
            # 資金の後始末は Job 終端化と同じ transaction で確定する。
            # 派生 ID（<expiry_op_id>:funds）を資金移動に割り当てる（第16節
            # 「一連の処理から生じる各資金移動には派生 ID を割り当てる」）
            funds = _job_terminal_fund_effects(
                c,
                job=current,
                actor_id=actor_id,
                operation_id=f"{op_id}:funds",
                now=now,
            )
            return {
                "job_id": _job_id,
                "state": JobState.EXPIRED.value,
                "funds": funds,
            }

        results.append(
            _run_idempotent(
                conn,
                actor_id=actor_id,
                kind="expiry",
                operation_id=expiry_op_id,
                business_key=None,
                payload=payload,
                apply_effects=_apply,
            )
        )
    return results


def abandon(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    job_id: str,
    lease_id: str,
    operation_id: str | None = None,
) -> CommandResult:
    """Worker の提出前放棄（計画書 第6節・第14節 `ojp job abandon`）。

    検査順序は第14節のとおり: **Lease に記録された Worker との Actor 一致
    → Job が LEASED → Lease の指定・有効性**。提出後も Lease 履歴を使って
    Actor を確認できるため、Child Requester A なら FORBIDDEN、提出済み
    Child の Worker B なら INVALID_STATE となる。いずれも Child を失敗へ
    変更できない。Requester の一方的 fail には使えない（FORBIDDEN）。

    **存在しない Lease は Actor 検査より先に INVALID_TARGET**: 第14節の
    「対象 Job の Lease に記録された Worker との Actor 一致」は Lease 行
    （履歴を含む）から Actor を確認する手続きであるため、Lease 行が無けれ
    ば「確認対象が存在しない」INVALID_TARGET が先になる（権限の有無を
    Leap せず確認できた事実だけに基づいて判定する。N04 の A による
    abandon CHILD --lease B_LEASE は Lease 行が存在するため FORBIDDEN）。

    成功時: Lease を closed_reason='abandoned' で閉じ、Job を FAILED にする。
    資金の後始末は失効処理と同じ規則（計画書 第8節の表。
    _job_terminal_fund_effects）: Root は未拘束 available を返金予約
    （PaymentOperation 付き）、Child は Parent 生存中なら
    child_work → available、Parent 終端後なら child_work → available →
    refund の追加返金。Parent 終端を Child へ伝播しない。
    """
    payload = {"job_id": job_id, "lease_id": lease_id}

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        # 0. Lease 行の存在（無ければ INVALID_TARGET。Actor 検査の材料に
        #    なる Lease 行が無い場合は先に INVALID_TARGET）
        lease_row = _get_lease_row(c, lease_id)
        # 1. Lease に記録された Worker との Actor 一致（FORBIDDEN）。
        #    提出後も Lease 履歴で Actor を確認する（第14節）
        if lease_row["job_id"] != job_id:
            raise OjpError(
                ErrorCode.INVALID_TARGET,
                f"lease {lease_id} does not belong to job {job_id}",
            )
        if lease_row["worker_id"] != actor_id:
            raise OjpError(
                ErrorCode.FORBIDDEN,
                f"actor {actor_id} is not the worker of lease {lease_id}"
                " (Child Requester A が Child を abandon → FORBIDDEN)",
            )
        # 2. Job が LEASED（有効提出後の SUBMITTED 等は INVALID_STATE）
        job = _get_job_row(c, job_id)
        if job["state"] != JobState.LEASED.value:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"job must be LEASED to abandon (current state: {job['state']})."
                " 有効提出後の Worker は abandon できない（第14節）",
            )
        # 3. Lease の指定・有効性（その Job の有効 Lease であること）
        if lease_row["closed_reason"] is not None:
            raise OjpError(
                ErrorCode.LEASE_EXPIRED,
                f"lease {lease_id} is already closed"
                f" (closed_reason={lease_row['closed_reason']})",
            )
        if job["active_lease_id"] != lease_id:
            raise OjpError(
                ErrorCode.INVALID_TARGET,
                f"lease {lease_id} is not the active lease of job {job_id}",
            )
        if job["version_id"] is None:
            raise OjpError(
                ErrorCode.INVALID_STATE, f"job has no published version: {job_id}"
            )
        _, deadline_us = _get_job_version_timing(c, job["version_id"])
        if now >= int(lease_row["expires_at_us"]) or now >= deadline_us:
            raise OjpError(
                ErrorCode.LEASE_EXPIRED,
                f"lease {lease_id} is already due"
                f" (now={now}, expires_at_us={lease_row['expires_at_us']},"
                f" deadline_us={deadline_us})",
            )
        _close_lease_and_fail_job_effects(
            c,
            job=job,
            lease_row=lease_row,
            closed_reason=LeaseClosedReason.ABANDONED,
            terminal_state=JobState.FAILED,
            now=now,
        )
        # 資金の後始末は Job 終端化と同じ transaction で確定する（派生 ID）
        funds = _job_terminal_fund_effects(
            c, job=job, actor_id=actor_id, operation_id=f"{op_id}:funds", now=now
        )
        return {"job_id": job_id, "state": JobState.FAILED.value, "funds": funds}

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="abandon",
        operation_id=operation_id,
        business_key=None,
        payload=payload,
        apply_effects=_apply,
    )


# ---------------------------------------------------------------------------
# Parent 終了時の返金予約（計画書 第8節の表・第15節 Lifecycle）
# ---------------------------------------------------------------------------


def reserve_refundable_balance(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    root_id: str,
    operation_id: str | None = None,
) -> CommandResult:
    """計画書 第8節「Parent 終了時」の表の Root 未拘束額の行を確定する
    Lifecycle（第15節 Lifecycle: reserve_refundable_balance）。

    表の各行の担当箇所: 「新 Child」列は create_child の Parent LEASED 要求が、
    「既存 Child」列は「Parent 終端を Child へ伝播しない」規則（失効・
    abandon が Child の jobs 行・child_work / child_payout を触らないこと）が
    担う。この関数は「Root の未拘束額」列を 1 箇所で判定する:

    - FAILED / EXPIRED: 未拘束 available だけを refund:{root_id}:terminal の
      返金予約へ移し、返金の PaymentOperation を同じ transaction で確定する
      （_reserve_terminal_refund_with_payment_in_tx。失効・abandon の終端化
      transaction でも同じ関数を使う）。既に確定済みなら既存の予約を返し、
      available が 0 なら予約を作らない正常な no-op（第16節）
    - SUBMITTED / DISPUTED: 返金予約を作らない（Parent 判定のため保持）
    - DONE: A 向け支払い予約は Phase 4 の approve 経路が reserve_parent_payout
      を使って確定する。この関数は DONE では資金を動かさない
    - DRAFT / OPEN / LEASED: available として保持（何もしない）

    actor_id は起動設定が決める system Actor（expire_due_leases と同じく
    tick からの呼び出しを想定。operations 行の actor_id は participants への
    FK を満たす必要がある）。**認可は service 層で行う**: 呼出 Actor が DB
    登録済みの system Participant でなければ FORBIDDEN
    （_require_system_actor）。_run_idempotent へ入る前に検査するため、
    既存 Operation の replay を返す前にも system であることを保証する。
    新規効果については _apply の書込 transaction 内でも再検証する。
    冪等性は operation_id の replay と、返金予約側の business_key
    （refund:{root_id}:terminal）の PaymentOperation が担うため、この
    operations 行自体は business_key を持たない（SUBMITTED / DONE での
    hold 判定や no-op が業務キーを消費しないようにする）。
    """
    # 認可は _run_idempotent より前（既存 Operation の replay を返す前に
    # system であることを保証する）。拒否なら外側 Operation も残さない
    with db.transaction(conn, immediate=False):
        _require_system_actor(conn, actor_id)
    payload = {"root_id": root_id}

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        # 書込 transaction 内で認可を再検証する（新規効果の確定より前）
        _require_system_actor(c, actor_id)
        job = _get_job_row(c, root_id)
        if job["parent_id"] is not None:
            raise OjpError(
                ErrorCode.INVALID_TARGET,
                "reserve_refundable_balance target must be a root job"
                f" (parent_id must be NULL): {root_id}",
            )
        state = str(job["state"])
        if state in (JobState.FAILED.value, JobState.EXPIRED.value):
            data = _reserve_terminal_refund_with_payment_in_tx(
                c, job=job, actor_id=actor_id, operation_id=op_id, now_us=now
            )
            action = "no_op" if data.get("no_op") else "refund_reserved"
            return {"root_id": root_id, "state": state, "action": action, **data}
        # SUBMITTED / DISPUTED / DONE / DRAFT / OPEN / LEASED は資金を動かさない
        return {"root_id": root_id, "state": state, "action": "hold"}

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="reserve",
        operation_id=operation_id,
        business_key=None,
        payload=payload,
        apply_effects=_apply,
    )


# ---------------------------------------------------------------------------
# Child 作成（計画書 第8節・第10節・第11節）
# ---------------------------------------------------------------------------


def _policy_limit_error(reason: domain.PolicyLimitReason, message: str) -> OjpError:
    """POLICY_LIMIT に details.reason を載せる（計画書 第10節・第11節）。"""
    return OjpError(
        ErrorCode.POLICY_LIMIT, message, details={"reason": reason.value}
    )


def _get_job_version_row(conn: sqlite3.Connection, version_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM job_versions WHERE id = ?", (version_id,)
    ).fetchone()
    if row is None:
        raise OjpError(
            ErrorCode.INVALID_TARGET, f"job version not found: {version_id}"
        )
    return row


def create_child(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    parent_job_id: str,
    lease_id: str,
    task_key: str,
    budget_units: int,
    deadline_us: int,
    operation_id: str | None = None,
) -> CommandResult:
    """Parent（Root, depth=0）の Worker A が Child を発注する（計画書 第8節）。

    Root 資金の内部移動（available -X / child_work +X）と Job・JobVersion の
    公開を 1 つの書込 transaction で行う。Child は作成と同時に OPEN
    （拘束前の公開はない。第6節の表「作成中Child」）。

    検査順序（第10節・第11節。この順序のまま docstring に固定する）:

    1. Actor・親 Job・親 Lease の有効性: lease_id が親 Job の Lease
       （INVALID_TARGET）、閉じた Lease（LEASE_EXPIRED。復活させない）、
       Worker 本人でない（FORBIDDEN）、Lease 期限・親 Job deadline 到来
       （LEASE_EXPIRED）を S1 の共通判定 require_active_lease で検査し、
       Parent が LEASED であることを確認する。Parent が OPEN / 終端 /
       Lease 失効中は作成不可（第8節の表）。generation はクライアントから
       受け取らず、DB の Lease 行の現行値を使う（指定が無いため照合は
       意味を持たない。それ以外の共通判定はそのまま適用する）
    2. 作成可否（カタログ参照・重複・予算件数の検査より先）: 親が depth=1
       （Child からの発注）なら POLICY_LIMIT/MAX_DEPTH で資金移動をしない。
       policy.enabled=false なら POLICY_LIMIT（details なし。計画書が個別の
       reason を定めていない）、policy.max_depth=0 なら POLICY_LIMIT/MAX_DEPTH
       （「enabled=false／depth=0／上限0ならChild 作成不可」第10節）
    3. カタログ参照 → task_key 重複 → 予算・件数:
       - 未知の task_key（カタログ外）は TASK_NOT_ALLOWED
       - 生存中（DRAFT/OPEN/LEASED/SUBMITTED/DISPUTED）または成功済み（DONE）
         の同じ task_key の Child が既にあれば TASK_CONFLICT。FAILED/EXPIRED
         は再発注可能だが新しい Child として作り、累計 max_children を消費
         する（枠は戻らない。第8節・第10節）
       - budget_units > カタログの budget_cap_units は POLICY_LIMIT/TASK_BUDGET
       - U + budget > max_amount_units は POLICY_LIMIT/MAX_AMOUNT、
         U + budget > floor(root_deposit_units * max_ratio_bps / 10000) は
         POLICY_LIMIT/MAX_RATIO（**金額と比率を同時に超える場合は
         MAX_AMOUNT**。第11節）
       - budget > available も POLICY_LIMIT/MAX_AMOUNT（安全網。有効 Lease
         中は available = D - U であり L <= D なので、L 判定が必ず先に
         発火する。第10節の判定式をそのまま残す）
       - 累計 Child 数 + 1 > max_children は POLICY_LIMIT/MAX_CHILDREN

    Child の成功条件は Root 公開 Version の task_catalog の該当エントリから
    導出する（input_values → input_json、expected → conditions_json と
    conditions_hash、verifier_id/hash は Root 公開 Version から継承）。
    timing_policy はカタログ entry に timing_policy があればその値（Root
    Requester の事前許可。第8節の延長）、無ければ Root 公開版を継承する。
    A が timing を引数で指定することはできない（すり替えを許さない）。
    成功条件・検証器・入力・受取人のすり替えは引数として受け付けない
    （第5節「MCP 引数に actor_id・payee_id を受け付けない」、第15節より
    MCP と CLI は同じ Application API を呼ぶ。X06 の期待結果は「拒否」であり
    「黙って無視」ではないため、この関数は input_json / expected_json /
    verifier_id / payee_id をシグネチャに持たず、渡すと TypeError になる）。
    受取人は Child 承認時に当該 Lease の Worker へ確定するため作成時点では
    保持しない（第12節）。budget_units と deadline_us だけが A の指定を
    受け付ける値で、deadline_us は Parent deadline を越えてもよい（第8節）。

    上限判定・task_key 重複判定・Job 作成・available 減額・locked 増額・
    作成数更新（jobs 行の挿入）はすべてこの書込 transaction の中で行い、
    transaction 外で読んだ残高を判断材料にしない（第10節）。拒否された
    作成は transaction ごと rollback され、Job も task_key も確保しない。
    business_key は allocate:{child_id}（Child 拘束の業務効果。第16節の表。
    「Child作成要求operation_idもJobに一意結合」は child_id = job:{op_id}
    の導出で満たす）。
    """
    # 引数の妥当性のうち transaction を必要としない部分（create_root と同じ
    # 段階に揃える）。ゼロ・負の予算はポリシー上限ではなく引数の不正として
    # 拒否する（ledger.allocate_child_work_in_tx の「ゼロChild予算拒否」と
    # 同じ INVALID_ARGUMENT）。deadline_us と now の比較は transaction 内の
    # now が必要なため _apply 側で行う（create_root と同じ構成）
    if not isinstance(task_key, str) or not task_key:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"task_key must be a non-empty str: {task_key!r}",
        )
    if isinstance(budget_units, bool) or not isinstance(budget_units, int):
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"budget_units must be int, got {type(budget_units).__name__}",
        )
    if budget_units <= 0:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            "child budget must be positive (ゼロ・負の Child 予算は拒否)",
        )
    if isinstance(deadline_us, bool) or not isinstance(deadline_us, int):
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"deadline_us must be int, got {type(deadline_us).__name__}",
        )
    if deadline_us < 0:
        raise OjpError(ErrorCode.INVALID_ARGUMENT, "deadline_us must be non-negative")
    # business_key は allocate:{child_id}。child_id は job:{op_id} から導く
    # ため、operation_id 未指定時はここで生成して確定させる（_run_idempotent
    # 内の生成と同じ規則）
    if operation_id is None:
        operation_id = ledger.new_operation_id("create")
    child_id = f"job:{operation_id}"
    payload = {
        "parent_job_id": parent_job_id,
        "lease_id": lease_id,
        "task_key": task_key,
        "budget_units": budget_units,
        "deadline_us": deadline_us,
    }

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        # --- 検査順序 1: Actor・親 Job・親 Lease の有効性 ---
        parent = _get_job_row(c, parent_job_id)
        lease_row = _get_lease_row(c, lease_id)
        require_active_lease(
            c,
            parent_job_id,
            lease_id,
            int(lease_row["generation"]),
            actor_id,
            now,
        )
        if parent["state"] != JobState.LEASED.value:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "parent job must be LEASED to create a child"
                f" (current state: {parent['state']})."
                " Parent が OPEN / 終端 / Lease 失効中は作成不可（第8節の表）",
            )
        # --- 検査順序 2: 作成可否（カタログ参照より先。資金移動なし） ---
        if parent["parent_id"] is not None:
            raise _policy_limit_error(
                domain.PolicyLimitReason.MAX_DEPTH,
                "child jobs cannot create children (depth=1 からの再委託は必ず"
                "拒否する。第8節・第11節。カタログ参照より先に検査)",
            )
        if parent["version_id"] is None:
            # require_active_lease が公開版の存在を検査済み。防御的な確認
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"parent job has no published version: {parent_job_id}",
            )
        parent_version = _get_job_version_row(c, parent["version_id"])
        policy = domain.SubcontractPolicy.model_validate_json(
            parent_version["subcontract_policy"]
        )
        if not policy.enabled:
            # enabled=false なら Child 作成不可（第10節）。計画書はこの拒否に
            # 個別の details.reason を定めていないため、POLICY_LIMIT のみを
            # 返し details は None にする（MAX_DEPTH 等へ読み替えない）
            raise OjpError(
                ErrorCode.POLICY_LIMIT,
                "subcontracting is disabled by the root policy"
                " (enabled=false なら Child 作成不可。第10節)",
            )
        if policy.max_depth == 0:
            raise _policy_limit_error(
                domain.PolicyLimitReason.MAX_DEPTH,
                "subcontract policy allows no children (max_depth=0。第10節)",
            )
        root_id = str(parent["root_id"])
        # --- 検査順序 3a: カタログ参照（TASK_NOT_ALLOWED） ---
        catalog = [
            domain.TaskCatalogEntry.model_validate(entry)
            for entry in json.loads(parent_version["task_catalog"])
        ]
        entry = next((e for e in catalog if e.task_key == task_key), None)
        if entry is None:
            raise OjpError(
                ErrorCode.TASK_NOT_ALLOWED,
                f"task_key is not in the root catalog: {task_key!r}"
                " (カタログ外の成功条件や入力へのすり替えは拒否。第8節・第11節)",
            )
        # --- 検査順序 3b: task_key 重複（TASK_CONFLICT） ---
        # 生存中 = DRAFT/OPEN/LEASED/SUBMITTED/DISPUTED、成功済み = DONE。
        # FAILED/EXPIRED は再発注可能（第8節）
        conflict = c.execute(
            "SELECT id FROM jobs WHERE parent_id = ? AND task_key = ?"
            " AND state IN ('DRAFT', 'OPEN', 'LEASED', 'SUBMITTED', 'DISPUTED',"
            "              'DONE')",
            (parent_job_id, task_key),
        ).fetchone()
        if conflict is not None:
            raise OjpError(
                ErrorCode.TASK_CONFLICT,
                f"task_key {task_key!r} already has a live or succeeded child:"
                f" {conflict['id']}"
                " (失敗・失効したタスクだけ再発注可能。第8節)",
            )
        # --- 検査順序 3c: 予算・件数 ---
        # Child の deadline は作成時点で未来（期限ちょうどは失効側。第7節）。
        # Parent deadline を越える Child deadline は許可する（第8節）
        if now >= deadline_us:
            raise OjpError(
                ErrorCode.INVALID_ARGUMENT,
                "child deadline_us must be in the future:"
                f" now={now} >= deadline={deadline_us}",
            )
        if budget_units > entry.budget_cap_units:
            raise _policy_limit_error(
                domain.PolicyLimitReason.TASK_BUDGET,
                f"child budget {budget_units} exceeds the catalog cap"
                f" {entry.budget_cap_units} for task_key {task_key!r}"
                " (カタログの1件上限。第11節)",
            )
        # 上限判定はこの transaction 内で導出する（transaction 外で先に読んだ
        # 残高を判断材料にしない。第10節）
        usage = ledger.get_subcontract_usage(c, root_id)
        amount_cap = policy.max_amount_units
        ratio_cap = policy.max_ratio_bps * usage.deposit_units // 10_000
        if usage.in_use_units + budget_units > amount_cap:
            raise _policy_limit_error(
                domain.PolicyLimitReason.MAX_AMOUNT,
                f"subcontract usage {usage.in_use_units} + budget {budget_units}"
                f" exceeds max_amount_units {amount_cap}"
                " (金額と比率を同時に超える場合は MAX_AMOUNT。第10節・第11節)",
            )
        if usage.in_use_units + budget_units > ratio_cap:
            raise _policy_limit_error(
                domain.PolicyLimitReason.MAX_RATIO,
                f"subcontract usage {usage.in_use_units} + budget {budget_units}"
                f" exceeds the ratio cap {ratio_cap}"
                f" (floor({usage.deposit_units} * {policy.max_ratio_bps} / 10000)."
                " 第10節)",
            )
        if budget_units > usage.available_units:
            # 安全網。有効 Lease 中は available = D - U であり L <= D のため、
            # 上の L 判定が必ず先に発火する（第10節の判定式をそのまま残す）
            raise _policy_limit_error(
                domain.PolicyLimitReason.MAX_AMOUNT,
                f"child budget {budget_units} exceeds the available balance"
                f" {usage.available_units} (新Child予算 <= available。第10節)",
            )
        if usage.child_count + 1 > policy.max_children:
            raise _policy_limit_error(
                domain.PolicyLimitReason.MAX_CHILDREN,
                f"child count {usage.child_count} + 1 exceeds max_children"
                f" {policy.max_children}"
                " (累計作成件数。失敗した Child の枠は戻らない。第10節)",
            )
        # --- Child Job・JobVersion の作成と資金拘束を同じ transaction で確定 ---
        # 固定するもの: 作成時の A（requester_id）、creator_lease_id = 現在の
        # Parent Lease、Root policy（artifact access は Root 公開版を継承、
        # timing はカタログ entry の事前許可があればそれを使い、無ければ
        # Root 公開版を継承）、指定 task_key（第8節）。Child からの再委託は
        # 必ず拒否されるため、Child の JobVersion には無効化した policy と
        # 空のカタログを保存する（スキーマの CHECK「Childのparent_idは
        # Rootのみ」と一致）
        child_version_id = f"version:{child_id}:1"
        c.execute(
            "INSERT INTO jobs (id, root_id, parent_id, requester_id, state,"
            " row_version, created_at_us, task_key, creator_lease_id)"
            " VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)",
            (
                child_id,
                root_id,
                parent_job_id,
                actor_id,
                JobState.OPEN.value,
                now,
                task_key,
                lease_id,
            ),
        )
        child_input_json = ledger.canonical_json_dumps(entry.input_values)
        child_conditions_json = ledger.canonical_json_dumps(entry.expected)
        child_conditions_hash = hashlib.sha256(
            child_conditions_json.encode("utf-8")
        ).hexdigest()
        child_policy = domain.SubcontractPolicy(
            enabled=False,
            max_amount_units=0,
            max_ratio_bps=0,
            max_children=0,
            max_depth=0,
        )
        # Child の timing_policy: カタログ entry に timing_policy があれば
        # それを使う（Root Requester の事前許可。第8節の延長）。無ければ
        # 従来どおり Root 公開版の timing を継承する。A が引数で timing を
        # 指定することはできない（すり替えを許さない。カタログの値だけを
        # 使う）
        if entry.timing_policy is not None:
            child_timing_json = ledger.canonical_json_dumps(
                entry.timing_policy.model_dump()
            )
        else:
            child_timing_json = parent_version["timing_policy"]
        c.execute(
            "INSERT INTO job_versions (id, job_id, version, title, budget_units,"
            " asset, input_json, verifier_id, verifier_hash, conditions_json,"
            " conditions_hash, subcontract_policy, task_catalog, timing_policy,"
            " artifact_access_policy, deadline_us)"
            " VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                child_version_id,
                child_id,
                f"{task_key} (child of {parent_job_id})",
                budget_units,
                domain.ASSET_MOCK_USDC,
                child_input_json,
                parent_version["verifier_id"],
                parent_version["verifier_hash"],
                child_conditions_json,
                child_conditions_hash,
                ledger.canonical_json_dumps(child_policy.model_dump()),
                "[]",
                child_timing_json,
                parent_version["artifact_access_policy"],
                deadline_us,
            ),
        )
        c.execute(
            "UPDATE jobs SET version_id = ? WHERE id = ?",
            (child_version_id, child_id),
        )
        allocation = ledger.allocate_child_work_in_tx(
            c,
            root_id=root_id,
            child_id=child_id,
            amount_units=budget_units,
            operation_id=op_id,
            now_us=now,
        )
        after = ledger.get_subcontract_usage(c, root_id)
        return {
            "child_id": child_id,
            "version_id": child_version_id,
            "parent_job_id": parent_job_id,
            "root_id": root_id,
            "task_key": task_key,
            "budget_units": budget_units,
            "deadline_us": deadline_us,
            "state": JobState.OPEN.value,
            "child_work_account_id": allocation["child_work_account_id"],
            "available_units": after.available_units,
            "in_use_units": after.in_use_units,
            "child_count": after.child_count,
        }

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="create",
        operation_id=operation_id,
        business_key=f"allocate:{child_id}",
        payload=payload,
        apply_effects=_apply,
    )


# ---------------------------------------------------------------------------
# Submit（計画書 第11節「有効な提出の境界」）
# ---------------------------------------------------------------------------


def submit(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    job_id: str,
    lease_id: str,
    version_id: str,
    artifact_json: str,
    operation_id: str | None = None,
) -> CommandResult:
    """Worker の成果物提出（計画書 第11節「有効な提出の境界」5 手順）。

    シグネチャは MCP の ojp_submit 引数（job_id, lease_id, version_id,
    artifact_json。第13節）だけを持つ。expected / input_values / verifier_id /
    payee_id / amount は受け付けない（A/B が提出と一緒に期待結果・テスト・
    判定器を渡しても採用しない。第11節。create_child と同じ方針で、渡すと
    TypeError になる）。generation もシグネチャに持たせず、lease_id で読んだ
    leases 行の generation をそのまま require_active_lease へ渡す（呼出側に
    世代を要求しない）。

    apply_effects 内の 5 手順（この順序で行う）:

    1. verification.assert_submission_failpoints_allowed を、何も書く前に呼ぶ
    2. 権限・状態の検査（この順序。ここで失敗した場合は SubmissionAttempt
       を残さない。提出権を持たない呼出だから）:
       2-1. _get_job_row → 無ければ INVALID_TARGET
       2-2. _get_lease_row で generation を読み、require_active_lease
            （共通判定: 別 Job → INVALID_TARGET / 閉じた Lease →
            LEASE_EXPIRED / generation → LEASE_EXPIRED / 別 Worker →
            FORBIDDEN / 期限 → LEASE_EXPIRED）
       2-3. Job が LEASED でなければ INVALID_STATE（SUBMITTED への二重提出・
            終端 Job への提出の拒否）
       2-4. version_id が jobs.version_id（公開版）と一致しなければ
            INVALID_TARGET（古い Version の提出拒否）。Lease の version_id
            とも一致すること
       2-5. Root（parent_id IS NULL）が Child を 1 件以上持つ場合、全 Child
            が DONE / FAILED / EXPIRED でなければ CHILDREN_UNRESOLVED
            （第8節「Parent提出は全Childが判定上の終端状態になってから」）。
            Child が DONE で送金待ち（PaymentOperation が PENDING/RETRYABLE）
            でも Parent 提出は妨げない
    3. 固定 JSON 検証: 公開 Version の input_json / conditions_json /
       verifier_id / verifier_hash から verification.verify_artifact を呼ぶ
       （期待値・入力・検証器の版は公開 Version 由来だけを使う）
    4. PASS の場合: submissions に 1 行 INSERT（artifact_json は canonical
       形式、review_due_at_us = now + review_window_seconds * 1_000_000）/
       submission_attempts に outcome='PASS' の監査行 / jobs.state='SUBMITTED'・
       active_lease_id=NULL / leases.closed_reason='submitted' / events に 1 行。
       verification.failpoint_before_submission_commit は submissions と Job
       更新を書いた直後・apply_effects から return する直前に発火する
       （seam が例外を投げれば transaction 全体が rollback して保存失敗を
       再現できる）
    5. FAIL の場合: submission_attempts に outcome='FAIL'・reason=<理由コード>
       を INSERT する。Job は LEASED のまま、Lease は開いたまま、submissions
       は作らず、review_due_at も支払い予約も作らない。

    **FAIL のエラー伝播**: apply_effects の中で例外を投げると Attempt 行も
    rollback されてしまうため、apply_effects は FAIL 情報を結果 dict として
    返し、_run_idempotent が commit した**後**に、submit 本体が
    OjpError(VERIFICATION_FAILED, details={"reason": ..., "attempt_id": ...})
    を投げる。これにより同じ operation_id での再送は _run_idempotent の
    replay 経路で保存済み結果（outcome=FAIL）を読み、同じ VERIFICATION_FAILED
    を決定的に再現する。

    **business_key について**: submit は business_key=None とし、operation_id
    の冪等性だけで重複提出を防ぐ。submit:{job_id} のような業務キーを付けると
    FAIL 後の期限内修正再提出（同じ Lease・新しい operation_id）が business_key
    衝突で作れなくなるためである。Job 当たり有効 Submission 1 件は、
    submissions_one_valid_per_job UNIQUE index と手順 2-3 の LEASED 検査
    （SUBMITTED 以外は拒否）で保証される。

    検証器の版不整合・入力不備は verify_artifact が OjpError(
    VERIFICATION_UNAVAILABLE) を投げる（Worker の検証 FAIL とは区別。
    第11節）。この例外は submit の検証 FAIL 経路とは違い transaction ごと
    rollback される（Attempt に残すべき「Worker の提出内容」の欠陥では
    ないため）。

    期限内の修正再提出（同じ Lease・新しい operation_id）は成功する。
    JobVersion の変更は不可（手順 2-4 が拒否する）。
    """
    payload = {
        "job_id": job_id,
        "lease_id": lease_id,
        "version_id": version_id,
        "artifact_json": artifact_json,
    }

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        # 1. failpoint seam は test mode の DB でのみ有効。何も書く前に検査
        verification.assert_submission_failpoints_allowed(c)
        # 2. 権限・状態の検査（ここで失敗した場合は Attempt を残さない）
        job = _get_job_row(c, job_id)
        lease_row = _get_lease_row(c, lease_id)
        require_active_lease(
            c, job_id, lease_id, int(lease_row["generation"]), actor_id, now
        )
        if job["state"] != JobState.LEASED.value:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"job must be LEASED to submit (current state: {job['state']})."
                " 有効提出後の二重提出・終端 Job への提出は拒否される",
            )
        if version_id != job["version_id"]:
            raise OjpError(
                ErrorCode.INVALID_TARGET,
                "version_id does not match the published version"
                f" (published: {job['version_id']}, got: {version_id})."
                " 古い Version の提出は拒否される",
            )
        if version_id != lease_row["version_id"]:
            raise OjpError(
                ErrorCode.INVALID_TARGET,
                "version_id does not match the lease's version"
                f" (lease: {lease_row['version_id']}, got: {version_id})",
            )
        # 2-5. Parent 提出は全 Child が判定上の終端状態になってから
        if job["parent_id"] is None:
            unresolved = c.execute(
                "SELECT id, state FROM jobs WHERE parent_id = ?"
                " AND state NOT IN (?, ?, ?)",
                (
                    job_id,
                    JobState.DONE.value,
                    JobState.FAILED.value,
                    JobState.EXPIRED.value,
                ),
            ).fetchall()
            if unresolved:
                raise OjpError(
                    ErrorCode.CHILDREN_UNRESOLVED,
                    f"job {job_id} has children not in a terminal state:"
                    f" {[{'id': r['id'], 'state': r['state']} for r in unresolved]}."
                    " Parent提出は全Childが判定上の終端状態になってから"
                    "（第8節）。Child DONE の送金待ちは妨げない",
                )
        # 3. 固定 JSON 検証（期待値・入力・検証器の版は公開 Version 由来）
        version_row = _get_job_version_row(c, version_id)
        if (
            version_row["input_json"] is None
            or version_row["conditions_json"] is None
            or version_row["verifier_id"] is None
            or version_row["verifier_hash"] is None
        ):
            raise OjpError(
                ErrorCode.VERIFICATION_UNAVAILABLE,
                f"published version {version_id} lacks verification inputs"
                " (input_json / conditions_json / verifier_id / verifier_hash)",
            )
        conditions_hash = hashlib.sha256(
            version_row["conditions_json"].encode("utf-8")
        ).hexdigest()
        if version_row["conditions_hash"] is not None and (
            conditions_hash != version_row["conditions_hash"]
        ):
            raise OjpError(
                ErrorCode.VERIFICATION_UNAVAILABLE,
                "published conditions_json does not match conditions_hash"
                f" (version: {version_id})",
            )
        outcome = verification.verify_artifact(
            raw_artifact=artifact_json,
            input_values=json.loads(version_row["input_json"]),
            expected=json.loads(version_row["conditions_json"]),
            verifier_id=version_row["verifier_id"],
            verifier_hash_value=version_row["verifier_hash"],
        )
        attempt_id = f"attempt:{op_id}"
        if outcome.result == domain.VerificationResult.FAIL:
            # 5. FAIL: Attempt だけ記録して LEASED のまま。apply_effects の
            #    中で例外を投げると Attempt 行も rollback されるため、
            #    結果を返して commit 後に submit 本体がエラーを投げる
            c.execute(
                "INSERT INTO submission_attempts (id, job_id, lease_id,"
                " input_hash, outcome, reason, attempted_at_us)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    attempt_id,
                    job_id,
                    lease_id,
                    outcome.input_hash,
                    domain.VerificationResult.FAIL.value,
                    outcome.reason,
                    now,
                ),
            )
            return {
                "outcome": domain.VerificationResult.FAIL.value,
                "reason": outcome.reason,
                "attempt_id": attempt_id,
                "job_state": JobState.LEASED.value,
                "input_hash": outcome.input_hash,
            }
        # 4. PASS: submissions 1 行 + 監査用 attempt 1 行 + Job SUBMITTED 化
        #    + Lease を submitted で閉じる + events 1 行
        submission_id = f"submission:{op_id}"
        timing, _deadline_us = _get_job_version_timing(c, version_id)
        review_due_at_us = now + timing.review_window_seconds * 1_000_000
        c.execute(
            "INSERT INTO submissions (id, job_id, lease_id, version_id,"
            " artifact_json, artifact_hash, verification_result,"
            " verification_evidence, submitted_at_us, valid_at_us,"
            " review_due_at_us)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                submission_id,
                job_id,
                lease_id,
                version_id,
                outcome.canonical_artifact,
                outcome.artifact_hash,
                domain.VerificationResult.PASS.value,
                outcome.evidence,
                now,
                now,
                review_due_at_us,
            ),
        )
        c.execute(
            "INSERT INTO submission_attempts (id, job_id, lease_id,"
            " input_hash, outcome, reason, attempted_at_us)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                attempt_id,
                job_id,
                lease_id,
                outcome.input_hash,
                domain.VerificationResult.PASS.value,
                outcome.reason,
                now,
            ),
        )
        cursor = c.execute(
            "UPDATE jobs SET state = ?, active_lease_id = NULL,"
            " row_version = row_version + 1"
            " WHERE id = ? AND state = ? AND active_lease_id = ?"
            " AND row_version = ?",
            (
                JobState.SUBMITTED.value,
                job_id,
                JobState.LEASED.value,
                lease_id,
                int(job["row_version"]),
            ),
        )
        if cursor.rowcount != 1:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "job state changed concurrently during submit"
                f" (job={job_id})",
            )
        c.execute(
            "UPDATE leases SET closed_reason = ? WHERE id = ?"
            " AND closed_reason IS NULL",
            (LeaseClosedReason.SUBMITTED.value, lease_id),
        )
        c.execute(
            "INSERT INTO events (root_id, job_id, actor_id, action, object_id,"
            " at_us) VALUES (?, ?, ?, ?, ?, ?)",
            (
                job["root_id"],
                job_id,
                actor_id,
                "submit",
                submission_id,
                now,
            ),
        )
        # failpoint: submissions と Job 更新を書いた直後・return 直前に発火。
        # seam が例外を投げれば transaction 全体が rollback する（保存失敗）
        verification.fire_before_submission_commit(c)
        return {
            "submission_id": submission_id,
            "artifact_hash": outcome.artifact_hash,
            "verification": domain.VerificationResult.PASS.value,
            "state": JobState.SUBMITTED.value,
            "review_due_at": review_due_at_us,
            "valid_at": now,
            "attempt_id": attempt_id,
        }

    try:
        result = _run_idempotent(
            conn,
            actor_id=actor_id,
            kind="submit",
            operation_id=operation_id,
            business_key=None,
            payload=payload,
            apply_effects=_apply,
        )
    except OjpError:
        raise
    except sqlite3.OperationalError as exc:
        if db.is_db_busy(exc):
            # DB_BUSY は _run_idempotent の扱いを変えない（同一 operation_id
            # での transaction 再試行・有限回で打ち切り）
            raise
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            "storage failure while persisting the submission"
            f" (sqlite error: {exc})",
        ) from exc
    except Exception as exc:
        # 保存フェーズ（submissions・Job 更新・events の書込）で発生した
        # 予期しない例外は再試行可能エラーへ正規化する（第11節「一時的
        # DB 失敗は再試行可能エラーとして Worker の検証 FAIL と区別する」）。
        # transaction は rollback 済み。生の例外を呼出側へ漏らさない
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            f"storage failure while persisting the submission: {exc!r}",
        ) from exc
    if result.data.get("outcome") == domain.VerificationResult.FAIL.value:
        # commit 後に投げる。同じ operation_id の再送は replay 経路で
        # 保存済み結果から同じ VERIFICATION_FAILED を再現する（決定的）
        raise OjpError(
            ErrorCode.VERIFICATION_FAILED,
            f"artifact verification failed: {result.data['reason']}",
            details={
                "reason": result.data["reason"],
                "attempt_id": result.data["attempt_id"],
            },
        )
    return result


# ---------------------------------------------------------------------------
# Approval / Dispute（計画書 第12節）
# ---------------------------------------------------------------------------


def _get_valid_submission_row(
    conn: sqlite3.Connection, job_id: str, submission_id: str
) -> sqlite3.Row:
    """Job の保存済み有効 Submission 行を引く（無ければ INVALID_TARGET）。

    「その Job の」Submission であることも検査する（別 Job の submission_id
    渡しは INVALID_TARGET）。
    """
    row = conn.execute(
        "SELECT * FROM submissions WHERE id = ?", (submission_id,)
    ).fetchone()
    if row is None or row["job_id"] != job_id:
        raise OjpError(
            ErrorCode.INVALID_TARGET,
            f"submission not found for job {job_id}: {submission_id}",
        )
    return row


def _approve_effects(
    conn: sqlite3.Connection,
    *,
    job: sqlite3.Row,
    submission: sqlite3.Row,
    actor_id: str,
    decided_by: str,
    reason: str | None,
    operation_id: str,
    now: int,
) -> dict[str, Any]:
    """approve / 自動承認 / 裁定 PASS で共有する効果の適用（1 transaction 内）。

    - acceptances に decision='APPROVED'・decided_by・reason を 1 件
    - jobs.state='DONE'・row_version+1・active_lease_id=NULL（条件付き UPDATE）
    - Child は ledger.child_approval_in_tx ＋ payout:{child_id} の
      PaymentOperation、Root は ledger.parent_approval_in_tx ＋
      payout:{root_id}（受取人は submissions.lease_id の Worker）
    - events に 1 行

    Job の検収（DONE）と送金状態を分離する: DONE は送金結果に依存せず、
    paid は process_payments が Receipt を確定したときだけ増える
    （APPROVED / DONE だけでは paid を増やさない）。
    """
    job_id = str(job["id"])
    root_id = str(job["root_id"])
    # 受取人は当該 Lease の Worker（amount / payee を引数に持たない。第12節）
    lease = conn.execute(
        "SELECT worker_id FROM leases WHERE id = ?", (submission["lease_id"],)
    ).fetchone()
    if lease is None:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            f"lease not found for submission: {submission['lease_id']}",
        )
    payee_id = str(lease["worker_id"])
    conn.execute(
        "INSERT INTO acceptances (job_id, submission_id, decision, decided_by,"
        " reason, decided_at_us) VALUES (?, ?, ?, ?, ?, ?)",
        (
            job_id,
            submission["id"],
            domain.AcceptanceDecision.APPROVED.value,
            decided_by,
            reason,
            now,
        ),
    )
    cursor = conn.execute(
        "UPDATE jobs SET state = ?, active_lease_id = NULL,"
        " row_version = row_version + 1"
        " WHERE id = ? AND row_version = ?",
        (JobState.DONE.value, job_id, int(job["row_version"])),
    )
    if cursor.rowcount != 1:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            "job state changed concurrently during approve"
            f" (job={job_id})",
        )
    if job["parent_id"] is not None:
        amount_units = _read_child_work_units(conn, job_id)
        business_key = f"payout:{job_id}"
        data = ledger.child_approval_in_tx(
            conn,
            root_id=root_id,
            child_id=job_id,
            amount_units=amount_units,
            operation_id=operation_id,
            now_us=now,
        )
        payment = _attach_payment_operation(
            conn,
            data=data,
            operation_id=operation_id,
            business_key=business_key,
            root_id=root_id,
            job_id=job_id,
            payee_id=payee_id,
            kind=PaymentKind.PAYOUT,
        )
    else:
        amount_units = _read_available_units(conn, root_id)
        business_key = f"payout:{root_id}"
        data = ledger.parent_approval_in_tx(
            conn,
            root_id=root_id,
            amount_units=amount_units,
            operation_id=operation_id,
            now_us=now,
        )
        payment = _attach_payment_operation(
            conn,
            data=data,
            operation_id=operation_id,
            business_key=business_key,
            root_id=root_id,
            job_id=root_id,
            payee_id=payee_id,
            kind=PaymentKind.PAYOUT,
        )
    conn.execute(
        "INSERT INTO events (root_id, job_id, actor_id, action, object_id,"
        " at_us) VALUES (?, ?, ?, ?, ?, ?)",
        (root_id, job_id, actor_id, "approve", submission["id"], now),
    )
    return {
        "job_id": job_id,
        "submission_id": str(submission["id"]),
        "state": JobState.DONE.value,
        "decision": domain.AcceptanceDecision.APPROVED.value,
        "decided_by": decided_by,
        "amount_units": amount_units,
        "payee_id": payee_id,
        **payment,
    }


def approve(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    job_id: str,
    submission_id: str,
    operation_id: str | None = None,
) -> CommandResult:
    """Requester の検収承認（計画書 第12節）。

    シグネチャは amount / payee_id / payee を持たない（渡すと TypeError。
    第12節「amount/payee を approve 引数に持たせず、受取人は当該 Lease の
    Worker とする」）。受取人は submissions.lease_id から引いた
    leases.worker_id。

    検査順序（この順序で確定する）:

    1. _get_job_row → 無ければ INVALID_TARGET
    2. Job が SUBMITTED でなければ INVALID_STATE（DISPUTED・終端・LEASED
       を拒否。approve と dispute の競合は、先に commit した有効遷移が
       手順 2 の状態検査で勝つ）
    3. submission_id がその Job の保存済み有効 Submission でなければ
       INVALID_TARGET
    4. actor_id がその Job の requester_id（Child なら作成時の A、Root なら
       Root Requester）でなければ FORBIDDEN。非検収者・無関係 Actor・
       Worker 本人・Root Requester による Child approve を拒否する
    5. 保存済み Submission の verification_result が 'PASS'、version_id が
       現在の公開版と一致、artifact_hash が保存 artifact_json の canonical
       hash と一致することを必須にする。不一致は INVALID_STATE
    6. now >= review_due_at_us の場合も承認自体は許可する（期限後は自動
       承認と同じ結論になる。ただし有効な異議が既に記録されていれば
       手順 2 の状態検査で DISPUTED として拒否される）

    効果（1 transaction）: acceptances に APPROVED／jobs.state='DONE'／
    Child なら child_work -X / child_payout +X ＋ payout:{child_id} の
    PaymentOperation、Root なら available -X / parent_payout +X ＋
    payout:{root_id}／events 1 行。送金は process_payments が別途処理する
    （DONE は送金結果に依存しない）。

    冪等性: 同一 operation_id の再送は _run_idempotent の replay。別
    operation_id での二重 approve は business_key=payout:{...} の
    既存 PaymentOperation を返す（Acceptance・PaymentOperation・Receipt は
    それぞれ 1 件だけ。N05・第16節）。ただし業務キー再利用の経路でも
    権限・対象・状態の前置検査（Job 存在・Submission 所属・requester_id・
    既存 APPROVED Acceptance）は _run_idempotent の前に読取 transaction
    で確定する（別 Actor・誤った submission_id は再利用経路でも拒否）。
    """
    payload = {
        "job_id": job_id,
        "submission_id": submission_id,
    }

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        # 1. Job の存在
        job = _get_job_row(c, job_id)
        # 2. SUBMITTED でなければ INVALID_STATE
        if job["state"] != JobState.SUBMITTED.value:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"job must be SUBMITTED to approve (current state: {job['state']})."
                " DISPUTED・終端・LEASED の approve は拒否される",
            )
        # 3. 有効 Submission
        submission = _get_valid_submission_row(c, job_id, submission_id)
        # 4. 検収者権限（Job の requester_id のみ）
        if actor_id != job["requester_id"]:
            raise OjpError(
                ErrorCode.FORBIDDEN,
                f"actor {actor_id} is not the requester of job {job_id}"
                f" (requester: {job['requester_id']})."
                " 検収はその Job の Requester のみ（第12節）",
            )
        # 5. 保存済み PASS・公開版一致・artifact_hash 一致
        if submission["verification_result"] != domain.VerificationResult.PASS.value:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "stored submission is not a verified PASS"
                f" (result: {submission['verification_result']})",
            )
        if job["version_id"] is None or submission["version_id"] != job["version_id"]:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "submission version does not match the published version"
                f" (published: {job['version_id']},"
                f" submission: {submission['version_id']})",
            )
        artifact_hash = hashlib.sha256(
            str(submission["artifact_json"]).encode("utf-8")
        ).hexdigest()
        if artifact_hash != submission["artifact_hash"]:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "stored artifact_json does not match the stored artifact_hash"
                f" (submission: {submission['id']})",
            )
        # 6. now >= review_due_at でも承認は許可（自動承認と同じ結論）
        return _approve_effects(
            c,
            job=job,
            submission=submission,
            actor_id=actor_id,
            decided_by=actor_id,
            reason=None,
            operation_id=op_id,
            now=now,
        )

    # business_key は payout:{job_id}（Child も Root も同じ形。第16節の表）。
    # Job が存在しない場合は _apply 内の _get_job_row が INVALID_TARGET を
    # 投げるため、ここでは business_key=None のまま通す
    job_row = conn.execute(
        "SELECT 1 FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    business_key = f"payout:{job_id}" if job_row is not None else None

    # 前置検査（読取 transaction）: _run_idempotent の reuse_existing_payment
    # 経路（既存 payout:{job_id} があると apply_effects を呼ばずに既存結果を
    # 返す）でも権限・状態・対象の検査がすり抜けないように、業務キー再利用の
    # 可否を先に確定する（R2。N05 の再利用経路そのものは維持する）:
    #   0. operation_id 照合（最優先。第16節）: 同じ operation_id を別
    #      Actor / 別 payload（別 submission_id・別 job_id）で再利用した
    #      場合は、ドメイン検査より先に IDEMPOTENCY_CONFLICT とする。
    #      一致する再送は _run_idempotent の replay 経路に任せる
    #   1. Job が無ければ INVALID_TARGET
    #   2. submission_id がその Job の保存済み Submission でなければ
    #      INVALID_TARGET
    #   3. actor_id がその Job の requester_id でなければ FORBIDDEN
    #   4. Job が SUBMITTED ならそのまま進む（通常の承認経路）
    #   5. Job が SUBMITTED ではないが、その Job の acceptances 行が存在し
    #      submission_id が一致し decision='APPROVED' なら、既存結果を返す
    #      経路（業務キー再利用）へ進んでよい
    #   6. それ以外（LEASED / DISPUTED / 終端で Acceptance が無い、または
    #      Acceptance の submission_id が違う）は INVALID_STATE
    with db.transaction(conn, immediate=False):
        _reject_operation_id_reuse(
            conn,
            operation_id=operation_id,
            actor_id=actor_id,
            kind="approve",
            payload=payload,
        )
        job = _get_job_row(conn, job_id)
        submission = conn.execute(
            "SELECT job_id FROM submissions WHERE id = ?", (submission_id,)
        ).fetchone()
        if submission is None or submission["job_id"] != job_id:
            raise OjpError(
                ErrorCode.INVALID_TARGET,
                f"submission not found for job {job_id}: {submission_id}",
            )
        if actor_id != job["requester_id"]:
            raise OjpError(
                ErrorCode.FORBIDDEN,
                f"actor {actor_id} is not the requester of job {job_id}"
                f" (requester: {job['requester_id']})."
                " 検収はその Job の Requester のみ（第12節）",
            )
        if job["state"] != JobState.SUBMITTED.value:
            acceptance = conn.execute(
                "SELECT submission_id, decision FROM acceptances"
                " WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if not (
                acceptance is not None
                and acceptance["submission_id"] == submission_id
                and acceptance["decision"]
                == domain.AcceptanceDecision.APPROVED.value
            ):
                raise OjpError(
                    ErrorCode.INVALID_STATE,
                    f"job must be SUBMITTED to approve"
                    f" (current state: {job['state']})."
                    " 既存の APPROVED Acceptance が無い状態での再利用も拒否される",
                )

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="approve",
        operation_id=operation_id,
        business_key=business_key,
        payload=payload,
        apply_effects=_apply,
        reuse_existing_payment=True,
        payment_kind=PaymentKind.PAYOUT,
    )


# dispute で受理する固定の理由コード（計画書 第12節）。価格交渉・新条件・
# 主観的な不満はこの集合に入れないことで受理しない。
DISPUTE_REASON_CODES = frozenset({"CONDITION_MISMATCH", "ARTIFACT_INTEGRITY"})


def _auto_approve_submission(
    conn: sqlite3.Connection,
    *,
    submission_id: str,
    actor_id: str,
    now: int,
) -> dict[str, Any]:
    """1 件の Submission を自動承認する（呼出側の transaction 内で使う）。

    approve_due_submissions と dispute の期限後経路が共有する効果の適用。
    operation_id は auto-approve:{submission_id}（第16節）。Job が既に
    SUBMITTED でなければ（承認済み・終端等）何もせず現在状態を返す。
    acceptances.decided_by へは actor_id を記録する（system Actor からの
    呼び出しでは system、dispute の期限後経路では異議を出した検収者。
    認可は呼出側が済ませている: approve_due_submissions は
    _require_system_actor、dispute は手順 4 の検収者検査）。
    """
    submission = conn.execute(
        "SELECT * FROM submissions WHERE id = ?", (submission_id,)
    ).fetchone()
    if submission is None:
        raise OjpError(
            ErrorCode.INVALID_TARGET, f"submission not found: {submission_id}"
        )
    job = _get_job_row(conn, str(submission["job_id"]))
    if job["state"] != JobState.SUBMITTED.value:
        return {"job_id": job["id"], "state": job["state"], "skipped": True}
    operation_id = f"auto-approve:{submission_id}"
    existing = conn.execute(
        "SELECT 1 FROM operations WHERE operation_id = ?", (operation_id,)
    ).fetchone()
    if existing is not None:
        # 既に自動承認の Operation がある（冪等）。Job は SUBMITTED では
        # ないはずだが、状態検査を通ってきた場合はスキップとして返す
        return {"job_id": job["id"], "state": job["state"], "skipped": True}
    conn.execute(
        "INSERT INTO operations (operation_id, actor_id, kind, payload_hash,"
        " business_key, status, result, created_at_us)"
        " VALUES (?, ?, 'auto-approve', ?, NULL, 'SUCCEEDED', NULL, ?)",
        (
            operation_id,
            actor_id,
            _payload_hash({"submission_id": submission_id, "auto": True}),
            now,
        ),
    )
    conn.execute(
        "INSERT INTO operations (operation_id, actor_id, kind, payload_hash,"
        " business_key, status, result, created_at_us)"
        " VALUES (?, ?, ?, ?, NULL, 'SUCCEEDED', NULL, ?)",
        (
            f"{operation_id}:payment",
            actor_id,
            PaymentKind.PAYOUT.value,
            _payload_hash(
                {
                    "submission_id": submission_id,
                    "auto": True,
                    "derived": "payment",
                }
            ),
            now,
        ),
    )
    data = _approve_effects(
        conn,
        job=job,
        submission=submission,
        actor_id=actor_id,
        decided_by=actor_id,
        reason="auto-approved by review window expiry",
        operation_id=operation_id,
        now=now,
    )
    conn.execute(
        "UPDATE operations SET result = ? WHERE operation_id = ?",
        (
            json.dumps(data, ensure_ascii=False, sort_keys=True),
            operation_id,
        ),
    )
    return data


def dispute(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    job_id: str,
    submission_id: str,
    condition_id: str,
    reason_code: str,
    evidence: str | None = None,
    operation_id: str | None = None,
) -> CommandResult:
    """検収期限内の異議（計画書 第12節）。

    検査順序（この順序で確定する）:

    1. _get_job_row → 無ければ INVALID_TARGET
    2. Job が SUBMITTED でなければ INVALID_STATE
    3. submission_id がその Job の有効 Submission でなければ INVALID_TARGET
    4. actor_id がその Job の requester_id または Root の requester_id で
       なければ FORBIDDEN（第5節「異議はそのJobのRequesterまたはRoot
       Requesterが提出できる」。Worker 本人・無関係 Actor は拒否）
    5. reason_code が固定の許可集合（CONDITION_MISMATCH /
       ARTIFACT_INTEGRITY）に無ければ INVALID_ARGUMENT。価格交渉・新条件・
       主観的な不満はこの制約で表現する
    6. condition_id が公開 Version の conditions_json のキー（固定 condition）
       でなければ INVALID_ARGUMENT
    7. now >= review_due_at_us なら新規異議を拒否して自動承認処理へ進む
       （第12節）: approve_due_submissions と同じ自動承認経路
       （auto-approve:{submission_id}、冪等）を確定してから
       OjpError(DISPUTE_WINDOW_CLOSED) を投げる（commit 後に本体が投げる。
       自動承認が既に済んでいれば冪等に何もしない）
    8. 既に有効な異議があれば INVALID_STATE（disputes.submission_id UNIQUE。
       期限延長・再オープンなし）

    効果（1 transaction）: disputes に status='OPEN'・opened_by=actor_id・
    due_at_us = now + dispute_window_seconds * 1_000_000／jobs.state='DISPUTED'・
    row_version+1／events 1 行。**資金は予約前の対象資金をそのまま保持する**
    （口座残高・PaymentOperation を一切作らない・動かさない）。

    approve と dispute が競合したら先に commit した有効遷移が勝つ
    （BEGIN IMMEDIATE の直列化と手順 2 の状態検査で成立。プロセス内 Lock
    に頼らない）。
    """
    payload = {
        "job_id": job_id,
        "submission_id": submission_id,
        "condition_id": condition_id,
        "reason_code": reason_code,
        "evidence": evidence,
    }

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        # 1. Job の存在
        job = _get_job_row(c, job_id)
        # 2. SUBMITTED でなければ INVALID_STATE
        if job["state"] != JobState.SUBMITTED.value:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"job must be SUBMITTED to dispute (current state: {job['state']})."
                " 期限延長・再オープンなし。既に承認・異議・終端済みの Job への"
                " 異議は拒否される",
            )
        # 3. 有効 Submission
        submission = _get_valid_submission_row(c, job_id, submission_id)
        # 4. 権限: その Job の Requester または Root Requester
        root = _get_job_row(c, str(job["root_id"]))
        allowed = {job["requester_id"], root["requester_id"]}
        if actor_id not in allowed:
            raise OjpError(
                ErrorCode.FORBIDDEN,
                f"actor {actor_id} is not the requester of job {job_id} nor the"
                f" root requester ({sorted(allowed)})."
                " 異議はそのJobのRequesterまたはRoot Requesterのみ（第5節）",
            )
        # 5. 固定の理由コード
        if reason_code not in DISPUTE_REASON_CODES:
            raise OjpError(
                ErrorCode.INVALID_ARGUMENT,
                f"reason_code must be one of {sorted(DISPUTE_REASON_CODES)}:"
                f" {reason_code!r}. 価格交渉・新条件・主観的な不満は受理しない",
            )
        # 6. 固定 condition（公開 Version の期待 JSON のキー）
        version_row = _get_job_version_row(c, str(job["version_id"]))
        if version_row["conditions_json"] is None:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"published version has no conditions_json: {job['version_id']}",
            )
        conditions = json.loads(version_row["conditions_json"])
        if condition_id not in conditions:
            raise OjpError(
                ErrorCode.INVALID_ARGUMENT,
                f"condition_id must be a key of the published conditions"
                f" ({sorted(conditions.keys())}): {condition_id!r}",
            )
        # 7. 期限後は新規異議を拒否して自動承認へ進む
        if now >= int(submission["review_due_at_us"]):
            # approve_due_submissions と同じ自動承認経路（auto-approve:{submission_id}、
            # 冪等）。既に承認済みなら Job は SUBMITTED ではないため何もしない。
            # 例外をここで投げると自動承認も rollback されるため、
            # window_closed を結果として返し commit 後に本体が投げる
            # （submit の VERIFICATION_FAILED と同じ伝播パターン）
            _auto_approve_submission(
                c, submission_id=str(submission["id"]), actor_id=actor_id, now=now
            )
            return {
                "window_closed": True,
                "job_id": job_id,
                "submission_id": submission_id,
                "review_due_at_us": int(submission["review_due_at_us"]),
            }
        # 8. 既に有効な異議があれば INVALID_STATE（最大 1 件・再オープンなし）
        existing = c.execute(
            "SELECT 1 FROM disputes WHERE submission_id = ?", (submission_id,)
        ).fetchone()
        if existing is not None:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"a dispute already exists for submission {submission_id}"
                " (最大1件・期限延長・再オープンなし)",
            )
        timing, _deadline_us = _get_job_version_timing(c, str(job["version_id"]))
        dispute_id = f"dispute:{op_id}"
        due_at_us = now + timing.dispute_window_seconds * 1_000_000
        c.execute(
            "INSERT INTO disputes (id, job_id, submission_id, opened_by,"
            " reason_code, condition_id, evidence, opened_at_us, due_at_us,"
            " status, resolution)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
            (
                dispute_id,
                job_id,
                submission_id,
                actor_id,
                reason_code,
                condition_id,
                evidence,
                now,
                due_at_us,
                domain.DisputeStatus.OPEN.value,
            ),
        )
        cursor = c.execute(
            "UPDATE jobs SET state = ?, row_version = row_version + 1"
            " WHERE id = ? AND row_version = ?",
            (JobState.DISPUTED.value, job_id, int(job["row_version"])),
        )
        if cursor.rowcount != 1:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "job state changed concurrently during dispute"
                f" (job={job_id})",
            )
        c.execute(
            "INSERT INTO events (root_id, job_id, actor_id, action, object_id,"
            " at_us) VALUES (?, ?, ?, ?, ?, ?)",
            (job["root_id"], job_id, actor_id, "dispute", dispute_id, now),
        )
        return {
            "dispute_id": dispute_id,
            "job_id": job_id,
            "submission_id": submission_id,
            "state": JobState.DISPUTED.value,
            "due_at_us": due_at_us,
        }

    result = _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="dispute",
        operation_id=operation_id,
        business_key=None,
        payload=payload,
        apply_effects=_apply,
    )
    if result.data.get("window_closed"):
        # commit 後に投げる（自動承認を残したまま）。同じ operation_id の
        # 再送は replay 経路で保存済み結果から同じ DISPUTE_WINDOW_CLOSED を
        # 再現する（決定的）
        raise OjpError(
            ErrorCode.DISPUTE_WINDOW_CLOSED,
            "review window is closed: now="
            f">= review_due_at={result.data['review_due_at_us']}."
            " 期限後は自動承認される（第12節）",
            details={
                "review_due_at_us": int(result.data["review_due_at_us"]),
                "submission_id": str(result.data["submission_id"]),
            },
        )
    return result


# ---------------------------------------------------------------------------
# Lifecycle: approve_due_submissions / resolve_due_disputes
# （計画書 第12節・第15節）
# ---------------------------------------------------------------------------


def _collect_due_submissions(
    conn: sqlite3.Connection, now: int
) -> list[sqlite3.Row]:
    """検収期限到来分の Submission を収集する（読み取りだけ）。

    対象: Job が SUBMITTED かつ now >= review_due_at_us かつ disputes に
    OPEN の異議が無い Submission（第12節「既に有効な異議が記録されていれば
    承認タイマーは何もしない」）。
    """
    return conn.execute(
        "SELECT s.* FROM submissions s JOIN jobs j ON j.id = s.job_id"
        " WHERE j.state = ? AND s.review_due_at_us <= ?"
        " AND NOT EXISTS (SELECT 1 FROM disputes d WHERE d.submission_id = s.id"
        "                 AND d.status = 'OPEN')"
        " ORDER BY s.id",
        (JobState.SUBMITTED.value, now),
    ).fetchall()


def approve_due_submissions(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    escrow: ledger.EscrowPort | None = None,
) -> list[CommandResult]:
    """検収期限到来分の自動承認（計画書 第15節 Lifecycle: approve_due_submissions）。

    tick から呼ばれる想定。Requester 無応答でも review_due_at を過ぎたら
    保存済み PASS の提出を自動承認する（第12節・N11）。

    - 対象: Job が SUBMITTED かつ now >= review_due_at_us かつ disputes に
      OPEN の異議が無い Submission。有効な異議がある Submission は承認
      タイマーの対象外（何もしない）
    - 効果は _auto_approve_submission を使う（手動 approve と同じ Acceptance
      ＋ DONE ＋ 支払い予約。operation_id=auto-approve:{submission_id}）。
      手動 approve と自動 approve で Acceptance と支払い予約が 1 組だけ
      になることは business_key=payout:{job_id} の一意性が保証する（N05）
    - 認可は expire_due_leases と同じく _require_system_actor（対象の
      収集前と各書込 transaction 内の両方で検査）
    - 1 件ずつ独立した transaction（_run_idempotent 相当）で処理し、
      now は各 transaction 内で 1 回だけ採取する。1 件の失敗で他の
      対象を巻き込まない（例外は呼出側へ伝播するが、それまでに確定
      した対象は残る）
    """
    del escrow  # 自動承認は Escrow port を使わない（送金は process_payments）
    with db.transaction(conn, immediate=False):
        _require_system_actor(conn, actor_id)
    with db.transaction(conn, immediate=False):
        preview_now = clock.now_for_read_snapshot(conn)
        targets = _collect_due_submissions(conn, preview_now)
    results: list[CommandResult] = []
    for submission_row in targets:
        submission_id = str(submission_row["id"])

        def _apply(
            c: sqlite3.Connection,
            op_id: str,
            now: int,
            _submission_id: str = submission_id,
        ) -> dict[str, Any]:
            _require_system_actor(c, actor_id)
            # auto-approve:{submission_id} の Operation 行は
            # _auto_approve_submission の中で直接 INSERT するため、
            # ここでは _run_idempotent を使わず transaction だけを所有する
            return _auto_approve_submission(
                c, submission_id=_submission_id, actor_id=actor_id, now=now
            )

        # operation_id は auto-approve:{submission_id} で固定（第16節）。
        # _auto_approve_submission が Operation 行を直接 INSERT するため、
        # _run_idempotent を使わず DB_BUSY 再試行付きの transaction を自前で
        # 回す（expire_due_leases と同じ粒度）
        data: dict[str, Any] | None = None
        for attempt in range(DB_BUSY_MAX_ATTEMPTS):
            try:
                with db.transaction(conn, immediate=True):
                    now_us = clock.now_for_write_transaction(conn)
                    data = _auto_approve_submission(
                        conn,
                        submission_id=submission_id,
                        actor_id=actor_id,
                        now=now_us,
                    )
                break
            except sqlite3.OperationalError as exc:
                if db.is_db_busy(exc) and attempt < DB_BUSY_MAX_ATTEMPTS - 1:
                    delay = min(
                        DB_BUSY_BASE_DELAY_SECONDS * (2**attempt),
                        DB_BUSY_MAX_DELAY_SECONDS,
                    ) * (0.5 + random.random())
                    time.sleep(delay)
                    continue
                raise OjpError(
                    ErrorCode.DB_BUSY,
                    f"database is busy after {attempt + 1} attempts",
                ) from exc
        assert data is not None
        results.append(
            CommandResult(
                data=data,
                operation_id=f"auto-approve:{submission_id}",
            )
        )
    return results


class _ResolveSkipped(Exception):
    """resolve_due_disputes の書込 transaction 内で「この異議への書き込みを
    やめて transaction 全体を rollback する」ことを呼出側へ伝える内部例外。

    無応答のまま書込 transaction の now が due_at_us に達していなかった場合
    （読取 snapshot と書込ロック取得後の時刻の間に裁定可否が変わる場合）に
    使う。    operations 行も含めて rollback されるため DB は一切変更されない。
    """


def resolve_due_disputes(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    escrow: ledger.EscrowPort | None = None,
) -> list[CommandResult]:
    """OPEN の異議を固定判定器で裁定する（第15節 Lifecycle: resolve_due_disputes）。

    tick から呼ばれる想定。各異議について verification.arbitrate を呼ぶ
    （保存 JSON・submissions.version_id の Version の入力と期待値・
    submissions.verification_evidence）。金額の一部裁定はしない
    （PASS / FAIL の二値のみ）。

    - PASS → DONE ＋ Acceptance APPROVED ＋ 支払い予約
      （_approve_effects。operation_id は resolve:{dispute_id}）
    - FAIL → FAILED ＋ Acceptance REJECTED ＋ _job_terminal_fund_effects
      による返却／返金予約（返却・返金の計算をここで書き直さない）
    - 判定器が応答しない（arbitrate が例外）場合:
      書込 transaction の now < due_at_us の間は何もしない（異議 OPEN・
      Job DISPUTED・資金不変。operations 行も残さない）。now >= due_at_us
      （期限ちょうどを含む。失効側優先）になったら必ず保存済み PASS を
      採用して自動承認する。全ての公開 Version が fallback を事前記録する
      （TimingPolicy.unresponsive_arbiter_fallback='stored_pass'。第12節
      「Requester/A の無応答で資金を永久凍結しない」。_approve_effects と
      同じ効果。無応答 fallback も Acceptance と支払い予約を 1 組だけ作る）
    - operation_id は resolve:{dispute_id}（第16節）。system 専用
      （_require_system_actor）。同一 operation_id の再送は保存済み結果の
      replay（actor/kind/payload_hash 照合。IDEMPOTENCY_CONFLICT）
    - 裁定（arbitrate の呼び出し）は書込 transaction の外で行い、効果の
      確定だけを transaction 内で行う。「無応答 fallback を適用するか /
      何もせず OPEN のまま残すか」の期限判定は書込 transaction 内の now で
      行う（第7節「書込ロックを取得した後のサーバー時刻で決め、期限
      ちょうどは失効側を優先する」）。期限前に確定した場合は operations
      行ごと rollback して DB を一切変更せず異議を OPEN のまま残し、結果へ
      {"unresponsive": True, "skipped": True} を返して次の異議へ進む
      （1 件分の失敗で他を巻き込まない）
    - 裁定結果（PASS / FAIL / 無応答 fallback のすべて）は
      disputes.resolution へ outcome / reason / evidence /
      condition_id / verifier_id / verifier_hash / input_hash を含む
      canonical JSON で保存する。FAIL の場合、acceptances.reason にも
      理由コードを含める
    """
    del escrow  # 裁定は Escrow port を使わない（送金は process_payments）
    with db.transaction(conn, immediate=False):
        _require_system_actor(conn, actor_id)
        # 裁定 seam は test mode の DB でのみ有効（代入済みなら realtime は
        # 何も動かさず拒否）
        verification.assert_arbiter_failpoints_allowed(conn)
    with db.transaction(conn, immediate=False):
        disputes = conn.execute(
            "SELECT * FROM disputes WHERE status = ? ORDER BY id",
            (domain.DisputeStatus.OPEN.value,),
        ).fetchall()
    results: list[CommandResult] = []
    for dispute_row in disputes:
        dispute_id = str(dispute_row["id"])
        operation_id = f"resolve:{dispute_id}"
        payload = {"dispute_id": dispute_id}
        existing = conn.execute(
            "SELECT actor_id, kind, payload_hash, status, result FROM operations"
            " WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        if existing is not None:
            # 同一 operation_id の再送（tick の再実行）は保存済み結果の replay
            if (
                existing["actor_id"] != actor_id
                or existing["kind"] != "resolve"
                or existing["payload_hash"] != _payload_hash(payload)
            ):
                raise OjpError(
                    ErrorCode.IDEMPOTENCY_CONFLICT,
                    "operation_id was already used with a different"
                    " actor/kind/payload",
                )
            data = json.loads(existing["result"]) if existing["result"] else {}
            results.append(
                CommandResult(data=data, operation_id=operation_id, replayed=True)
            )
            continue

        # 裁定（書込 transaction の外）。判定器の実行はここで行うが、
        # 「無応答 fallback を適用するか / 何もせず OPEN のまま残すか」の
        # 期限判定は _apply の中で書込 transaction の now で確定する
        # （第7節: 書込ロックを取得した後のサーバー時刻で決める）
        preview_now = clock.now_for_read_snapshot(conn)
        verdict, outcome = _arbitrate_dispute_outcome(
            conn, dispute_row, now_us=preview_now
        )
        if verdict == "unresponsive":
            # 判定器が応答しない。fallback 適用の可否は書込 transaction 内の
            # now で再判定するため、ここでは書込を試みる（期限前に確定した
            # 場合は _apply が _ResolveSkipped を投げて transaction 全体を
            # rollback する）
            def _apply(
                c: sqlite3.Connection,
                op_id: str,
                now: int,
            ) -> dict[str, Any]:
                _require_system_actor(c, actor_id)
                dispute = c.execute(
                    "SELECT * FROM disputes WHERE id = ?", (dispute_id,)
                ).fetchone()
                if (
                    dispute is None
                    or dispute["status"] != domain.DisputeStatus.OPEN.value
                ):
                    return {"dispute_id": dispute_id, "skipped": True}
                # 期限の再判定は書込ロック取得後の now で行う（期限ちょうど
                # は fallback 適用側＝失効側を優先する）
                if now < int(dispute["due_at_us"]):
                    # operations 行ごと rollback して何も書かない
                    raise _ResolveSkipped(dispute_id)
                job = _get_job_row(c, str(dispute["job_id"]))
                submission = c.execute(
                    "SELECT * FROM submissions WHERE id = ?",
                    (dispute["submission_id"],),
                ).fetchone()
                if submission is None:
                    raise OjpError(
                        ErrorCode.INVALID_STATE,
                        f"dispute {dispute_id} references a missing submission:"
                        f" {dispute['submission_id']}",
                    )
                if job["state"] != JobState.DISPUTED.value:
                    return {"dispute_id": dispute_id, "skipped": True}
                # 保存済み PASS への fallback（第12節。全 Version が事前記録）
                version_row = _get_job_version_row(c, str(submission["version_id"]))
                reason = (
                    "dispute resolved by stored_pass fallback after"
                    f" unresponsive arbiter ({dispute_id})"
                )
                payment_operation_id = f"{op_id}:payment"
                _insert_operation(
                    c,
                    operation_id=payment_operation_id,
                    actor_id=actor_id,
                    kind=PaymentKind.PAYOUT.value,
                    payload_hash=_payload_hash(
                        {
                            "derived_from": op_id,
                            "business_key": f"payout:{job['id']}",
                        }
                    ),
                    business_key=None,
                    status=OperationStatus.SUCCEEDED,
                    result=None,
                    now_us=now,
                )
                data = _approve_effects(
                    c,
                    job=job,
                    submission=submission,
                    actor_id=actor_id,
                    decided_by=actor_id,
                    reason=reason,
                    operation_id=op_id,
                    now=now,
                )
                resolution = _dispute_resolution_fields(
                    outcome="UNRESPONSIVE_ARBITER_STORED_PASS",
                    reason="ARBITER_UNRESPONSIVE_STORED_PASS_FALLBACK",
                    evidence=str(submission["verification_evidence"]),
                    condition_id=str(dispute["condition_id"]),
                    verifier_id=str(version_row["verifier_id"]),
                    verifier_hash=str(version_row["verifier_hash"]),
                    input_hash=_stored_evidence_input_hash(
                        str(submission["verification_evidence"])
                    ),
                    arbitration=None,
                    # 判定器の証跡が無い経路。保存済み PASS を採用するため
                    # FAIL 未再現ではなく、異議の condition_id への帰属は
                    # 維持される（condition_matched=True）
                    condition_matched=True,
                )
                c.execute(
                    "UPDATE disputes SET status = ?, resolution = ? WHERE id = ?",
                    (
                        domain.DisputeStatus.RESOLVED.value,
                        resolution,
                        dispute_id,
                    ),
                )
                c.execute(
                    "INSERT INTO events (root_id, job_id, actor_id, action,"
                    " object_id, at_us) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        job["root_id"],
                        job["id"],
                        actor_id,
                        "resolve",
                        dispute_id,
                        now,
                    ),
                )
                return {
                    "dispute_id": dispute_id,
                    "job_id": job["id"],
                    "resolution": "UNRESPONSIVE_ARBITER_STORED_PASS",
                    "approval": data,
                }

        else:

            def _apply(
                c: sqlite3.Connection,
                op_id: str,
                now: int,
                _verdict: str = verdict,
                _outcome: verification.VerificationOutcome | None = outcome,
            ) -> dict[str, Any]:
                _require_system_actor(c, actor_id)
                dispute = c.execute(
                    "SELECT * FROM disputes WHERE id = ?", (dispute_id,)
                ).fetchone()
                if (
                    dispute is None
                    or dispute["status"] != domain.DisputeStatus.OPEN.value
                ):
                    # 既に別の tick で裁定済み（冪等）。何もしない
                    return {"dispute_id": dispute_id, "skipped": True}
                job = _get_job_row(c, str(dispute["job_id"]))
                submission = c.execute(
                    "SELECT * FROM submissions WHERE id = ?",
                    (dispute["submission_id"],),
                ).fetchone()
                if submission is None:
                    raise OjpError(
                        ErrorCode.INVALID_STATE,
                        f"dispute {dispute_id} references a missing submission:"
                        f" {dispute['submission_id']}",
                    )
                if job["state"] != JobState.DISPUTED.value:
                    return {"dispute_id": dispute_id, "skipped": True}
                version_row = _get_job_version_row(c, str(submission["version_id"]))
                # FAIL が異議の condition_id に起因するか（第12節「既存条件へ
                # の FAIL が再現された場合だけ FAILED」）。PASS 経路は常に
                # True。FAIL でも原因 condition が特定できない（構造 FAIL）、
                # または異議の condition_id と一致しない場合は False
                # （異議は成立しなかったとして承認側へ収束する）
                condition_matched = True
                if _verdict == "FAIL":
                    condition_matched = (
                        _outcome is not None
                        and _outcome.failed_condition_id is not None
                        and _outcome.failed_condition_id
                        == str(dispute["condition_id"])
                    )
                if _verdict == "PASS" or not condition_matched:
                    # PASS → DONE ＋ Acceptance APPROVED ＋ 支払い予約。
                    # FAIL が異議の condition_id に起因しない場合も同じ
                    # 承認側へ収束する（資金を凍結しない。第12節「無応答で
                    # 資金を永久凍結しない」と同じ方針）
                    if _verdict == "PASS":
                        reason = (
                            f"dispute resolved by arbitration PASS ({dispute_id})"
                        )
                        outcome_label = "PASS"
                        reason_code = _outcome.reason if _outcome is not None else "OK"
                    else:
                        # FAIL は再現したが異議の condition_id に起因しない
                        # （構造 FAIL・別 condition 起因）。異議は成立しなかった
                        reason_code = _outcome.reason if _outcome is not None else "OK"
                        actual_failed = (
                            _outcome.failed_condition_id
                            if _outcome is not None
                            else None
                        )
                        reason = (
                            f"dispute not upheld ({dispute_id}): arbitration"
                            f" reproduced FAIL but not on condition"
                            f" {dispute['condition_id']!r} (reason:"
                            f" {reason_code}, failed_condition_id:"
                            f" {actual_failed!r}). 第12節「既存条件への FAIL が"
                            " 再現された場合だけ FAILED」に起因しないため承認側へ収束"
                        )
                        outcome_label = "FAIL_NOT_ON_DISPUTED_CONDITION"
                    payment_operation_id = f"{op_id}:payment"
                    _insert_operation(
                        c,
                        operation_id=payment_operation_id,
                        actor_id=actor_id,
                        kind=PaymentKind.PAYOUT.value,
                        payload_hash=_payload_hash(
                            {
                                "derived_from": op_id,
                                "business_key": f"payout:{job['id']}",
                            }
                        ),
                        business_key=None,
                        status=OperationStatus.SUCCEEDED,
                        result=None,
                        now_us=now,
                    )
                    data = _approve_effects(
                        c,
                        job=job,
                        submission=submission,
                        actor_id=actor_id,
                        decided_by=actor_id,
                        reason=reason,
                        operation_id=op_id,
                        now=now,
                    )
                    resolution = _dispute_resolution_fields(
                        outcome=outcome_label,
                        reason=reason_code,
                        evidence=(
                            _outcome.evidence if _outcome is not None else None
                        ),
                        condition_id=str(dispute["condition_id"]),
                        verifier_id=str(version_row["verifier_id"]),
                        verifier_hash=str(version_row["verifier_hash"]),
                        input_hash=(
                            _outcome.input_hash if _outcome is not None else None
                        ),
                        arbitration=_outcome,
                        condition_matched=condition_matched,
                    )
                    c.execute(
                        "UPDATE disputes SET status = ?, resolution = ? WHERE id = ?",
                        (
                            domain.DisputeStatus.RESOLVED.value,
                            resolution,
                            dispute_id,
                        ),
                    )
                    c.execute(
                        "INSERT INTO events (root_id, job_id, actor_id, action,"
                        " object_id, at_us) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            job["root_id"],
                            job["id"],
                            actor_id,
                            "resolve",
                            dispute_id,
                            now,
                        ),
                    )
                    return {
                        "dispute_id": dispute_id,
                        "job_id": job["id"],
                        "resolution": outcome_label,
                        "approval": data,
                    }
                # FAIL（異議の condition_id に起因する）→ FAILED ＋
                # Acceptance REJECTED ＋ 返却／返金予約
                c.execute(
                    "INSERT INTO acceptances (job_id, submission_id, decision,"
                    " decided_by, reason, decided_at_us) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        job["id"],
                        submission["id"],
                        domain.AcceptanceDecision.REJECTED.value,
                        actor_id,
                        f"dispute resolved by arbitration FAIL ({dispute_id}):"
                        f" {_outcome.reason} on condition"
                        f" {dispute['condition_id']!r}",
                        now,
                    ),
                )
                cursor = c.execute(
                    "UPDATE jobs SET state = ?, active_lease_id = NULL,"
                    " row_version = row_version + 1"
                    " WHERE id = ? AND row_version = ?",
                    (JobState.FAILED.value, job["id"], int(job["row_version"])),
                )
                if cursor.rowcount != 1:
                    raise OjpError(
                        ErrorCode.INVALID_STATE,
                        "job state changed concurrently during dispute resolution"
                        f" (job={job['id']})",
                    )
                funds = _job_terminal_fund_effects(
                    c,
                    job=job,
                    actor_id=actor_id,
                    operation_id=f"{op_id}:funds",
                    now=now,
                )
                resolution = _dispute_resolution_fields(
                    outcome="FAIL",
                    reason=_outcome.reason,
                    evidence=_outcome.evidence,
                    condition_id=str(dispute["condition_id"]),
                    verifier_id=str(version_row["verifier_id"]),
                    verifier_hash=str(version_row["verifier_hash"]),
                    input_hash=_outcome.input_hash,
                    arbitration=_outcome,
                    condition_matched=True,
                )
                c.execute(
                    "UPDATE disputes SET status = ?, resolution = ? WHERE id = ?",
                    (domain.DisputeStatus.RESOLVED.value, resolution, dispute_id),
                )
                c.execute(
                    "INSERT INTO events (root_id, job_id, actor_id, action,"
                    " object_id, at_us) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        job["root_id"],
                        job["id"],
                        actor_id,
                        "resolve",
                        dispute_id,
                        now,
                    ),
                )
                return {
                    "dispute_id": dispute_id,
                    "job_id": job["id"],
                    "resolution": "FAIL",
                    "state": JobState.FAILED.value,
                    "funds": funds,
                }

        data: dict[str, Any] | None = None
        for attempt in range(DB_BUSY_MAX_ATTEMPTS):
            try:
                with db.transaction(conn, immediate=True):
                    now_us = clock.now_for_write_transaction(conn)
                    _insert_operation(
                        conn,
                        operation_id=operation_id,
                        actor_id=actor_id,
                        kind="resolve",
                        payload_hash=_payload_hash(payload),
                        business_key=None,
                        status=OperationStatus.SUCCEEDED,
                        result=None,
                        now_us=now_us,
                    )
                    data = _apply(conn, operation_id, now_us)
                    conn.execute(
                        "UPDATE operations SET result = ? WHERE operation_id = ?",
                        (
                            json.dumps(data, ensure_ascii=False, sort_keys=True),
                            operation_id,
                        ),
                    )
                break
            except _ResolveSkipped:
                # 無応答のまま書込 transaction 内の now が期限前に確定した。
                # operations 行を含めて transaction 全体が rollback 済みのため、
                # DB は一切変更されていない。異議は OPEN のまま次へ進む
                results.append(
                    CommandResult(
                        data={
                            "dispute_id": dispute_id,
                            "unresponsive": True,
                            "skipped": True,
                        },
                        operation_id=operation_id,
                    )
                )
                data = None
                break
            except sqlite3.OperationalError as exc:
                if db.is_db_busy(exc) and attempt < DB_BUSY_MAX_ATTEMPTS - 1:
                    delay = min(
                        DB_BUSY_BASE_DELAY_SECONDS * (2**attempt),
                        DB_BUSY_MAX_DELAY_SECONDS,
                    ) * (0.5 + random.random())
                    time.sleep(delay)
                    continue
                raise OjpError(
                    ErrorCode.DB_BUSY,
                    f"database is busy after {attempt + 1} attempts",
                ) from exc
        if data is None:
            # _ResolveSkipped で処理済み（結果は results へ追加済み）
            continue
        results.append(CommandResult(data=data, operation_id=operation_id))
    return results

def _stored_evidence_input_hash(evidence_json: str) -> str | None:
    """保存済み検証証跡（canonical JSON）から input_hash を取り出す。

    無応答 fallback の resolution 記録に使う。parse 不能・欠落の場合は
    None を返す（証跡は submissions.verification_evidence の全文を別途
    保存しているため、この値が欠けても裁定の根拠は失われない）。
    """
    try:
        evidence = json.loads(evidence_json)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(evidence, dict):
        return None
    value = evidence.get("input_hash")
    return value if isinstance(value, str) else None


def _dispute_resolution_fields(
    *,
    outcome: str,
    reason: str,
    evidence: str | None,
    condition_id: str,
    verifier_id: str,
    verifier_hash: str,
    input_hash: str | None,
    arbitration: verification.VerificationOutcome | None,
    condition_matched: bool,
) -> str:
    """disputes.resolution へ保存する canonical JSON を組み立てる（第12節）。

    PASS / FAIL / 無応答 fallback / FAIL 不成立の全経路で、異議の
    condition_id に加えて、裁定の原因 condition（failed_condition_id）と
    判明した期待値・実値（expected_value / actual_value）、FAIL が異議の
    condition_id に起因するか（condition_matched）を記録する。第12節
    「既存条件への FAIL が再現された場合だけ FAILED」の判定根拠を
    resolution へ残す。arbitration が None（無応答 fallback）の経路では
    判定器の証跡が無いため failed_condition_id / expected_value /
    actual_value は None とし、condition_matched は True（保存済み PASS
    を採用するため FAIL の未再現ではない）とする。
    """
    return ledger.canonical_json_dumps(
        {
            "outcome": outcome,
            "reason": reason,
            "evidence": evidence,
            "condition_id": condition_id,
            "verifier_id": verifier_id,
            "verifier_hash": verifier_hash,
            "input_hash": input_hash,
            "failed_condition_id": (
                arbitration.failed_condition_id if arbitration is not None else None
            ),
            "expected_value": (
                arbitration.expected_value if arbitration is not None else None
            ),
            "actual_value": (
                arbitration.actual_value if arbitration is not None else None
            ),
            "condition_matched": condition_matched,
        }
    )


def _arbitrate_dispute_outcome(
    conn: sqlite3.Connection, dispute_row: sqlite3.Row, *, now_us: int
) -> tuple[str, verification.VerificationOutcome | None]:
    """1 件の異議を裁定して (verdict, VerificationOutcome) を返す（書込は行わない）。

    戻り値:
    - ("PASS", outcome) / ("FAIL", outcome): 判定器が応答した場合。
      outcome は裁定の証跡（reason / evidence / input_hash を含む。
      resolution への保存に使う）
    - ("unresponsive", None): 判定器が応答しない場合。保存済み PASS への
      fallback を適用するかどうかは呼出側が書込 transaction 内の now で
      再判定する（第7節: 書込ロック取得後のサーバー時刻で決める）

    arbitrate の例外（failpoint_arbiter_unresponsive seam・
    VERIFICATION_UNAVAILABLE）は「判定器が応答しない」扱いにする。書込
    transaction の外で呼ぶため、例外で DB は一切変更されない。
    now_us は読取 snapshot の時刻（応答があった場合の裁定実行の時刻参照）。
    異議対象の condition_id を arbitrate へ渡し、戻り値の outcome は
    「その condition_id に対する裁定結果」として扱う（FAIL でも
    failed_condition_id が condition_id と一致しない場合は、呼出側が
    FAILED にしない。第12節「既存条件への FAIL が再現された場合だけ
    FAILED」）。
    """
    submission = conn.execute(
        "SELECT * FROM submissions WHERE id = ?", (dispute_row["submission_id"],)
    ).fetchone()
    if submission is None:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            f"dispute {dispute_row['id']} references a missing submission:"
            f" {dispute_row['submission_id']}",
        )
    version_row = _get_job_version_row(conn, str(submission["version_id"]))
    if (
        version_row["input_json"] is None
        or version_row["conditions_json"] is None
        or version_row["verifier_id"] is None
        or version_row["verifier_hash"] is None
    ):
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            "published version lacks verification inputs for arbitration"
            f" (version: {submission['version_id']})",
        )
    try:
        outcome = verification.arbitrate(
            stored_artifact_json=str(submission["artifact_json"]),
            input_values=json.loads(version_row["input_json"]),
            expected=json.loads(version_row["conditions_json"]),
            verifier_id=version_row["verifier_id"],
            verifier_hash_value=version_row["verifier_hash"],
            original_evidence=str(submission["verification_evidence"]),
            condition_id=str(dispute_row["condition_id"]),
        )
    except Exception:
        # 判定器が応答しない。fallback 適用の可否（now >= due_at_us）は
        # 呼出側が書込 transaction 内の now で再判定する
        del now_us
        return "unresponsive", None
    if outcome.result == domain.VerificationResult.PASS.value:
        return "PASS", outcome
    return "FAIL", outcome


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


def _validate_amount_units(amount_units: int) -> None:
    """amount_units を非負 int に限定する（bool は int として受理しない）。

    allocate_child_work / create_child と同じ段階（_run_idempotent より前、
    業務キー照会・Operation 挿入の前）で引数の不正を弾くための検査
    （計画書 第5節の金額検証・第13節 INVALID_ARGUMENT）。ゼロは特例として
    受け付け、呼出側のゼロ no-op 挙動へ流す。ledger 側の負数拒否は
    多重防御として残る。
    """
    if isinstance(amount_units, bool) or not isinstance(amount_units, int):
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT,
            f"amount_units must be int, got {type(amount_units).__name__}",
        )
    if amount_units < 0:
        raise OjpError(
            ErrorCode.INVALID_ARGUMENT, "amount must be non-negative"
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

    amount_units == 0 の正常 no-op は業務キーを消費しない
    （_run_idempotent へは business_key=None で通し、再送用の主 Operation
    だけを保存する）。ゼロ時は payment_kind も None にするため
    `<operation_id>:payment` の派生 Operation と PaymentOperation は
    作られない。負数（および bool・int 以外）は _run_idempotent より前に
    INVALID_ARGUMENT で拒否する（業務キー照会の前に引数検査。第5節・第13節）。
    """
    _validate_amount_units(amount_units)
    payload = {
        "root_id": root_id,
        "child_id": child_id,
        "amount_units": amount_units,
        "payee_id": payee_id,
    }
    zero_no_op = amount_units == 0

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
        business_key=None if zero_no_op else f"payout:{child_id}",
        payload=payload,
        apply_effects=_apply,
        reuse_existing_payment=not zero_no_op,
        payment_kind=None if zero_no_op else PaymentKind.PAYOUT,
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


def _child_done_with_pending_payment(conn: sqlite3.Connection, child_id: str) -> bool:
    """Child が DONE で PaymentOperation が PENDING / RETRYABLE（送金障害中）か。

    計画書 第9節「Child が DONE だが送金障害中の場合は失敗返却を禁止する」
    の判定を DB から導出する。呼出側が flag を渡さなくても、失敗返却の経路
    （Phase 4 の dispute FAIL 等）がこの禁止をすり抜けないようにする。
    """
    job = conn.execute(
        "SELECT state FROM jobs WHERE id = ?", (child_id,)
    ).fetchone()
    if job is None or job["state"] != JobState.DONE.value:
        return False
    payment = ledger.get_payment_operation_by_business_key(conn, f"payout:{child_id}")
    return payment is not None and payment.status in (
        PaymentStatus.PENDING,
        PaymentStatus.RETRYABLE,
    )


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
    Child DONE かつ送金障害中は禁止（INVALID_STATE。計画書 第9節）。
    送金障害中の判定は引数の flag に加えて DB（jobs.state = DONE かつ
    payout:{child_id} の PaymentOperation が PENDING / RETRYABLE）からも
    導出し、どちらかが真なら拒否する。

    amount_units == 0 の正常 no-op は business_key を消費しない
    （_run_idempotent へは business_key=None で通し、再送用の主 Operation
    だけを保存する。正額時に return:{child_id} /
    refund:{root_id}:child-return:{child_id} を確定できるようにするため）。
    負数（および bool・int 以外）は _run_idempotent より前に INVALID_ARGUMENT
    で拒否する（業務キー照会の前に引数検査。第5節・第13節）。
    """
    _validate_amount_units(amount_units)
    payload = {
        "root_id": root_id,
        "child_id": child_id,
        "amount_units": amount_units,
        "parent_terminal_refund_reserved": parent_terminal_refund_reserved,
        "child_done_payment_pending": child_done_payment_pending,
    }
    if amount_units == 0:
        # ゼロ no-op は業務キーを持たない（どちらの経路でも消費しない）
        business_key: str | None = None
    elif parent_terminal_refund_reserved:
        business_key = f"refund:{root_id}:child-return:{child_id}"
    else:
        business_key = f"return:{child_id}"

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        return ledger.child_failure_return_in_tx(
            c,
            root_id=root_id,
            child_id=child_id,
            amount_units=amount_units,
            operation_id=op_id,
            now_us=now,
            parent_terminal_refund_reserved=parent_terminal_refund_reserved,
            child_done_payment_pending=(
                child_done_payment_pending
                or _child_done_with_pending_payment(c, child_id)
            ),
        )

    return _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="return",
        operation_id=operation_id,
        business_key=business_key,
        payload=payload,
        apply_effects=_apply,
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

    amount_units == 0 の正常 no-op は業務キーを消費しない
    （_run_idempotent へは business_key=None で通し、再送用の主 Operation
    だけを保存する）。ゼロ時は payment_kind も None にするため
    `<operation_id>:payment` の派生 Operation と PaymentOperation は
    作られない。負数（および bool・int 以外）は _run_idempotent より前に
    INVALID_ARGUMENT で拒否する（業務キー照会の前に引数検査。第5節・第13節）。

    **Parent DONE のときだけ許される**（計画書 第8節の表「DONE: Root の
    未拘束額は A 向け支払い予約へ移動」）。approve 自体は Phase 4 の範囲で、
    Phase 4 の approve 経路が Parent を DONE にした後にこの予約を確定する。
    対象は Root（depth=0 の Parent）だけ。Child を指定した場合は
    INVALID_TARGET、Parent が DONE でなければ INVALID_STATE。
    """
    _validate_amount_units(amount_units)
    payload = {"root_id": root_id, "amount_units": amount_units, "payee_id": payee_id}
    zero_no_op = amount_units == 0

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        job = _get_job_row(c, root_id)
        if job["parent_id"] is not None:
            raise OjpError(
                ErrorCode.INVALID_TARGET,
                "parent payout target must be a root job"
                f" (parent_id must be NULL): {root_id}",
            )
        if job["state"] != JobState.DONE.value:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                "parent payout reservation is allowed only when the parent job"
                f" is DONE (current state: {job['state']})."
                " available を parent_payout へ移すのは Parent DONE のときだけ"
                "（第8節の表。A 向け支払い予約は Phase 4 の approve 経路が使う）",
            )
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
        business_key=None if zero_no_op else f"payout:{root_id}",
        payload=payload,
        apply_effects=_apply,
        reuse_existing_payment=not zero_no_op,
        payment_kind=None if zero_no_op else PaymentKind.PAYOUT,
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

    amount_units == 0 の正常 no-op は業務キーを消費しない
    （_run_idempotent へは business_key=None で通し、再送用の主 Operation
    だけを保存する）。ゼロ時は payment_kind も None にするため
    `<operation_id>:payment` の派生 Operation と PaymentOperation は
    作られない（payee_id 指定の有無双方で同様）。負数（および bool・int 以外）は
    _run_idempotent より前に INVALID_ARGUMENT で拒否する（業務キー照会の
    前に引数検査。第5節・第13節）。
    """
    _validate_amount_units(amount_units)
    payload = {
        "root_id": root_id,
        "amount_units": amount_units,
        "payee_id": payee_id,
    }
    zero_no_op = amount_units == 0

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
        business_key=None if zero_no_op else f"refund:{root_id}:terminal",
        payload=payload,
        apply_effects=_apply,
        reuse_existing_payment=(payee_id is not None) and not zero_no_op,
        payment_kind=(
            PaymentKind.REFUND if payee_id is not None and not zero_no_op else None
        ),
    )


# ---------------------------------------------------------------------------
# Root Requester / system による返金コマンド（計画書 第14節 `ojp job refund`）
# ---------------------------------------------------------------------------


def refund(
    conn: sqlite3.Connection,
    *,
    actor_id: str,
    job_id: str,
    operation_id: str | None = None,
    escrow: ledger.EscrowPort | None = None,
) -> CommandResult:
    """確定済みの返金予約だけを処理するコマンド（計画書 第14節
    `ojp job refund ROOT`「確定済み返金予約だけを再処理。有効提出からの
    直接返金は拒否」・第6節「Root返金コマンドは既に確定した返金予約を
    処理するだけで、Jobの判定を変更できない」）。

    検査順序（計画書 第14節。この順序どおりに固定する）:

    1. 対象 Job から Root を解決する（jobs.root_id）。Job が無ければ
       INVALID_TARGET
    2. **Actor 権限を先に検査する**: Root Requester または system 以外は
       FORBIDDEN（対象が Child であっても、権限が無ければ INVALID_TARGET
       より先に FORBIDDEN を返す）
    3. 権限があっても**対象が Child なら INVALID_TARGET**（返金対象は
       Root のみ）
    4. Root の返金予約（payment_operations の kind='refund'）が 1 件も
       なければ INVALID_STATE。**新しい返金予約は作らない**
       （reserve_refundable_balance は呼ばない）
    5. 確定済みの返金予約（business_key が refund:{root_id}:terminal および
       refund:{root_id}:child-return:{child_id}）だけを処理する。
       **送金済み（SUCCEEDED）なら既存結果を返す**（新しい送金を作らない・
       金額と受取人を変更しない）
    6. Lifecycle で返金可能額が 0 のときに予約を作らない規則とは区別する
       （残高 0 で予約が存在するなら、その予約の処理結果を返す）

    - **Job の判定を変更できない**: jobs.state を一切更新しない（第6節）
    - 送金処理は既存の process_single_payment を再利用する（自前の送金・
      Receipt 処理を書かない）。process_single_payment が transaction を
      所有するため、権限・対象・予約の検査だけを _run_idempotent の
      transaction で確定し、各予約の送金処理はその commit 後に行う
    - 権限: Root Requester または system（第14節「refundはRoot Requester
      またはsystem」）。jobs.requester_id と participants.kind から解決する
    - escrow は process_single_payment へそのまま渡す（省略時は MockEscrow）
    - 冪等性: 同一 operation_id の再送は _run_idempotent の replay
      （保存済みの予約一覧に対して同じ処理を再実行する。各予約の送金は
      Receipt が正本のため二重にならない）。返金予約ごとの業務効果
      （business_key）は予約作成側が担うため、このコマンドの Operation は
      business_key を持たない
    """
    escrow_port = escrow if escrow is not None else ledger.MockEscrow()
    payload = {"job_id": job_id}

    def _apply(c: sqlite3.Connection, op_id: str, now: int) -> dict[str, Any]:
        # 1. 対象 Job から Root を解決する。Job が無ければ INVALID_TARGET
        job = _get_job_row(c, job_id)
        root_id = str(job["root_id"])
        root = _get_job_row(c, root_id)
        # 2. Actor 権限を先に検査する（対象が Child でも INVALID_TARGET より
        #    先に FORBIDDEN）。権限は Root Requester または system のみ
        if actor_id != root["requester_id"] and not _is_system_actor(c, actor_id):
            raise OjpError(
                ErrorCode.FORBIDDEN,
                f"actor {actor_id!r} is neither the root requester"
                f" ({root['requester_id']!r}) nor a system participant"
                " (refund は Root Requester または system のみ。第14節)",
            )
        # 3. 権限があっても対象が Child なら INVALID_TARGET（返金対象は
        #    Root のみ）
        if job["parent_id"] is not None:
            raise OjpError(
                ErrorCode.INVALID_TARGET,
                f"refund target must be the root job (返金対象は Root のみ):"
                f" {job_id} (root: {root_id})",
            )
        # 4. Root の返金予約が 1 件もなければ INVALID_STATE。
        #    新しい返金予約は作らない（reserve_refundable_balance を呼ばない）
        reservations = c.execute(
            "SELECT * FROM payment_operations WHERE root_id = ? AND kind = ?"
            " ORDER BY operation_id",
            (root_id, PaymentKind.REFUND.value),
        ).fetchall()
        if not reservations:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"root {root_id} has no refund reservations to process"
                " (返金予約なし。refund は確定済み予約の再処理だけを行い、"
                " 新しい返金予約は作らない. 第14節)",
            )
        # 5. 確定済みの返金予約（refund:{root_id}:terminal と
        #    refund:{root_id}:child-return:{child_id}）だけを処理対象として
        #    列挙する。送金自体はこの transaction の commit 後に
        #    process_single_payment が行う（金額と受取人は PaymentOperation
        #    作成時の固定値からしか取らない）
        return {
            "root_id": root_id,
            "job_id": job_id,
            "refund_operation_ids": [
                str(reservation["operation_id"]) for reservation in reservations
            ],
        }

    result = _run_idempotent(
        conn,
        actor_id=actor_id,
        kind="refund",
        operation_id=operation_id,
        business_key=None,
        payload=payload,
        apply_effects=_apply,
    )
    # 5.（続き）各予約の送金処理。送金済み（SUCCEEDED）なら既存結果を返す
    #    （新しい送金を作らない・金額と受取人を変更しない）。6. 残高 0 で
    #    予約が存在する場合も、その予約の処理結果を返す（予約を作らないのは
    #    予約作成側の規則。ここは処理のみ）
    processed = [
        process_single_payment(
            conn, operation_id=payment_operation_id, escrow=escrow_port
        ).data
        for payment_operation_id in result.data["refund_operation_ids"]
    ]
    data = {
        "root_id": result.data["root_id"],
        "job_id": result.data["job_id"],
        "refund_count": len(processed),
        "refunds": processed,
    }
    return CommandResult(
        data=data, operation_id=result.operation_id, replayed=result.replayed
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

    transaction は 2 つに分ける（第9節 3〜5）:

    - T1（transfer）: locked 減額・MockWallet 増額・paid/refunded・Journal・
      Receipt を 1 つの DB transaction で確定して commit する
    - T2（status 更新）: 別の transaction で PaymentOperation を
      SUCCEEDED ＋ receipt_id に更新する

    - PENDING / RETRYABLE を取得し、同じ operation_id で Escrow port の
      transfer を呼ぶ（計画書 第9節 2）
    - Receipt が無いのに paid/refunded を増やさない（Mock が Receipt 正本）
    - 結果を記録する前に停止しても、再起動後に lookup で Receipt を照会して
      SUCCEEDED へ収束させる（第9節 4）
    - DB commit 前の障害は T1 全体 rollback。commit 後の応答消失は
      Receipt が正本
    - 再試行で Job を FAILED にしない（有限回で打ち切らない。本モジュールは
      Job 状態を一切触らない）
    - SUCCEEDED 済みの再実行は Receipt の存在と金額・受取人・原資の一致を
      必ず照合する（Receipt が正本）。欠落・不一致は成功として返さず、
      理由を last_error に記録する（既に SUCCEEDED へ確定した整合状態は
      後から書き換えず、運用者が内容を確認して是正する）
    - 未確定の送金の失敗は種類にかかわらず attempt_count・last_error を
      記録し、status=RETRYABLE・next_retry_at_us をバックオフで設定する
      （返金への切替は行わない）
    - failpoint（障害注入）が代入済みの場合は test mode の DB でのみ
      進み、realtime では T1 を開始する前に何も動かさず拒否する
      （第14節。送金失敗としては記録しない）
    """
    escrow_port = escrow if escrow is not None else ledger.MockEscrow()
    # failpoint は test mode の DB でのみ有効。realtime で代入済みなら
    # 台帳・Wallet・Receipt・PaymentOperation を一切変更する前に拒否する。
    ledger.assert_failpoints_allowed(conn)

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

    def _read_payment_or_raise() -> Any:
        payment = ledger.get_payment_operation(conn, operation_id)
        if payment is None:
            raise OjpError(
                ErrorCode.INVALID_STATE,
                f"payment operation not found: {operation_id}",
            )
        return payment

    for attempt in range(DB_BUSY_MAX_ATTEMPTS):
        try:
            # T1（transfer）: PaymentOperation を読み、Escrow port の
            # transfer（locked 減額・Wallet 増額・Journal・Receipt）までを
            # この transaction で確定して commit する。SUCCEEDED 済みの
            # 照合分岐・失敗の記録もこの transaction 内で完結する。
            with db.transaction(conn, immediate=True):
                payment = _read_payment_or_raise()
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
            # (c) Receipt 確定後・アプリの status 更新前の注入点。T1 の
            # commit 直後・T2 の前に発火させると、「Receipt は commit 済み・
            # PaymentOperation は未更新」の中断状態になる（再起動後の
            # 収束の検証用）。realtime での拒否は先頭の前置検査
            # （assert_failpoints_allowed）が何も動かす前に行うため、ここで
            # 発火するのは test mode の DB のみ。
            ledger._fire_failpoint(
                conn, ledger.failpoint_after_receipt, "after_receipt"
            )
            # T2（status 更新）: 別の transaction で PaymentOperation を
            # SUCCEEDED ＋ receipt_id に更新する。
            with db.transaction(conn, immediate=True):
                # (b) commit 後の応答消失の注入点。send 系（T1）を別
                # transaction で確定させた直後・この status 更新 transaction
                # の commit 前に発火させると、「Receipt は commit 済み・
                # PaymentOperation は未更新」の中断状態になる（再起動後の
                # 収束の検証用）。
                ledger._fire_failpoint(
                    conn, ledger.failpoint_after_commit, "after_commit"
                )
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
