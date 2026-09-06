"""OJP ドメイン型・状態・policy・不変条件（PoC計画書 第5節・第6節・第10節・第11節）。

Phase 1 は型と不変条件の定義のみ。サービス層・トランザクション境界は Phase 2 以降。
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

UNIT_SCALE = 1_000_000
SQLITE_MAX_I64 = 9_223_372_036_854_775_807
ASSET_MOCK_USDC = "mock-USDC"

_AMOUNT_RE = re.compile(r"^[0-9]+\.[0-9]{6}$")


class ErrorCode(StrEnum):
    """代表エラーコード（計画書 第13節）。"""

    FORBIDDEN = "FORBIDDEN"
    INVALID_STATE = "INVALID_STATE"
    INVALID_TARGET = "INVALID_TARGET"
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    CLAIM_CONFLICT = "CLAIM_CONFLICT"
    TASK_NOT_ALLOWED = "TASK_NOT_ALLOWED"
    TASK_CONFLICT = "TASK_CONFLICT"
    POLICY_LIMIT = "POLICY_LIMIT"
    CHILDREN_UNRESOLVED = "CHILDREN_UNRESOLVED"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"
    VERIFICATION_UNAVAILABLE = "VERIFICATION_UNAVAILABLE"
    DISPUTE_WINDOW_CLOSED = "DISPUTE_WINDOW_CLOSED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    DB_BUSY = "DB_BUSY"
    CLOCK_ERROR = "CLOCK_ERROR"
    CLOCK_BACKWARD = "CLOCK_BACKWARD"
    MODE_MISMATCH = "MODE_MISMATCH"


class PolicyLimitReason(StrEnum):
    """再委託上限の拒否理由（計画書 第10節・第11節の details.reason）。

    TASK_BUDGET / MAX_AMOUNT / MAX_RATIO / MAX_CHILDREN / MAX_DEPTH は計画書が
    列挙する 5 種。金額と比率を同時に超える場合は MAX_AMOUNT を返す。
    DISABLED は「enabled=false なら Child 作成不可」（第10節）に対応する理由で、
    計画書が個別の reason を定めていないため本 PoC で追加した実装判断
    （政策上の拒否であることを POLICY_LIMIT で揃える）。
    """

    TASK_BUDGET = "TASK_BUDGET"
    MAX_AMOUNT = "MAX_AMOUNT"
    MAX_RATIO = "MAX_RATIO"
    MAX_CHILDREN = "MAX_CHILDREN"
    MAX_DEPTH = "MAX_DEPTH"
    DISABLED = "DISABLED"


class OjpError(Exception):
    """ドメインエラーの基底。code は ErrorCode の値。

    details は POLICY_LIMIT の拒否理由（計画書 第10節・第11節の
    details.reason）のような構造化された補足情報。既存の code / message の
    契約は変えず、details は省略可能な追加情報として扱う。
    """

    def __init__(
        self,
        code: ErrorCode | str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = str(code)
        self.message = message
        self.details = details


class MoneyError(OjpError):
    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.INVALID_ARGUMENT, message)


def parse_amount_units(value: Any, *, allow_zero: bool = False) -> int:
    """6桁小数文字列を mock-USDC units の整数へ変換する。

    文字列以外（float/int を含む）・負数・小数6桁以外・SQLite 64bit 整数範囲超を拒否する。
    ゼロは allow_zero の文脈でのみ許容する。
    """
    if isinstance(value, bool) or not isinstance(value, str):
        raise MoneyError(
            f"amount must be a 6-decimal string like '100.000000', got {type(value).__name__}"
        )
    if not _AMOUNT_RE.match(value):
        if value.startswith("-"):
            raise MoneyError(f"amount must be non-negative: {value!r}")
        if "." in value:
            frac = value.split(".", 1)[1]
            if len(frac) > 6 and frac.isdigit():
                raise MoneyError(f"amount must have exactly 6 decimal digits: {value!r}")
        raise MoneyError(f"amount must be a 6-decimal string like '100.000000': {value!r}")
    whole, frac = value.split(".")
    units = int(whole) * UNIT_SCALE + int(frac)
    if units > SQLITE_MAX_I64:
        raise MoneyError(f"amount exceeds SQLite 64-bit integer range: {value!r}")
    if units == 0 and not allow_zero:
        raise MoneyError("amount must be positive")
    return units


def format_amount_units(units: Any) -> str:
    """mock-USDC units の整数を6桁小数文字列へ変換する。"""
    if isinstance(units, bool) or not isinstance(units, int):
        raise MoneyError(f"units must be int, got {type(units).__name__}")
    if units < 0:
        raise MoneyError(f"units must be non-negative: {units}")
    if units > SQLITE_MAX_I64:
        raise MoneyError(f"units exceed SQLite 64-bit integer range: {units}")
    whole, frac = divmod(units, UNIT_SCALE)
    return f"{whole}.{frac:06d}"


class ParticipantKind(StrEnum):
    HUMAN = "human"
    AGENT = "agent"
    SYSTEM = "system"


class JobState(StrEnum):
    DRAFT = "DRAFT"
    OPEN = "OPEN"
    LEASED = "LEASED"
    SUBMITTED = "SUBMITTED"
    DISPUTED = "DISPUTED"
    DONE = "DONE"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


TERMINAL_JOB_STATES = frozenset({JobState.DONE, JobState.FAILED, JobState.EXPIRED})


class LeaseClosedReason(StrEnum):
    SUBMITTED = "submitted"
    ABANDONED = "abandoned"
    EXPIRED = "expired"


class Bucket(StrEnum):
    """排他的な資金区分（計画書 第9節）。"""

    AVAILABLE = "available"
    CHILD_WORK = "child_work"
    CHILD_PAYOUT = "child_payout"
    PARENT_PAYOUT = "parent_payout"
    REFUND = "refund"
    PAID = "paid"
    REFUNDED = "refunded"


LOCKED_BUCKETS = frozenset(
    {Bucket.CHILD_WORK, Bucket.CHILD_PAYOUT, Bucket.PARENT_PAYOUT, Bucket.REFUND}
)


class PaymentKind(StrEnum):
    PAYOUT = "payout"
    REFUND = "refund"


class PaymentStatus(StrEnum):
    PENDING = "PENDING"
    RETRYABLE = "RETRYABLE"
    SUCCEEDED = "SUCCEEDED"


class AcceptanceDecision(StrEnum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class DisputeStatus(StrEnum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


class OperationStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"


class ClockMode(StrEnum):
    REALTIME = "realtime"
    TEST = "test"


AmountUnits = Annotated[int, Field(ge=0, le=SQLITE_MAX_I64, strict=True)]
PositiveAmountUnits = Annotated[int, Field(gt=0, le=SQLITE_MAX_I64, strict=True)]
SignedAmountUnits = Annotated[int, Field(le=SQLITE_MAX_I64, ge=-SQLITE_MAX_I64, strict=True)]
TimestampUs = Annotated[int, Field(ge=0, le=SQLITE_MAX_I64, strict=True)]
StrictNonNegativeInt = Annotated[int, Field(ge=0, strict=True)]
StrictInt = Annotated[int, Field(strict=True)]


class _StrictModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Amount(_StrictModel):
    """JSON I/O 用の金額文字列。形式のみ検証しゼロを許す（ゼロ拒否は文脈側）。"""

    amount: str

    @field_validator("amount")
    @classmethod
    def _check(cls, v: str) -> str:
        parse_amount_units(v, allow_zero=True)
        return v

    def to_units(self, *, allow_zero: bool = False) -> int:
        return parse_amount_units(self.amount, allow_zero=allow_zero)


class Participant(_StrictModel):
    id: str
    label: str
    kind: ParticipantKind


class ActorContext(_StrictModel):
    """サーバー起動設定から解決される実行者。MCP 引数では受け付けない。"""

    actor_id: str
    kind: ParticipantKind


class SubcontractPolicy(_StrictModel):
    """再委託上限（計画書 第10節）。比率は basis points 整数。"""

    enabled: bool
    max_amount_units: AmountUnits
    max_ratio_bps: Annotated[int, Field(ge=0, le=10_000, strict=True)]
    max_children: StrictNonNegativeInt
    max_depth: Annotated[int, Field(ge=0, le=1, strict=True)]


class TimingPolicy(_StrictModel):
    """デモ既定値: Lease 60秒 / heartbeat 20秒 / 検収30秒 / 異議判定30秒。"""

    lease_seconds: Annotated[int, Field(gt=0, strict=True)] = 60
    heartbeat_seconds: Annotated[int, Field(gt=0, strict=True)] = 20
    review_window_seconds: Annotated[int, Field(gt=0, strict=True)] = 30
    dispute_window_seconds: Annotated[int, Field(gt=0, strict=True)] = 30


class ArtifactAccessPolicy(_StrictModel):
    """成果物の利用権（計画書 第5節「Actorと成果物への権限」）。

    Requester が資金拠出した Child の有効成果物は、A 停止・Parent 終了後も
    Root Requester が取得できるよう、利用権を JobVersion に事前明記する。
    """

    requester_can_read: bool = True


MAX_TASK_INPUT_ELEMENTS = 1_000
MAX_TASK_INPUT_ABS = 1_000_000_000
MAX_ARTIFACT_BYTES = 1_048_576
MAX_ARTIFACT_DEPTH = 8


def _validate_task_input(v: list[int]) -> list[int]:
    if len(v) > MAX_TASK_INPUT_ELEMENTS:
        raise ValueError(f"input exceeds {MAX_TASK_INPUT_ELEMENTS} elements")
    for x in v:
        if isinstance(x, bool) or not isinstance(x, int):
            raise ValueError(f"input must contain only int: {x!r}")
        if abs(x) > MAX_TASK_INPUT_ABS:
            raise ValueError(f"input value out of range: {x}")
    return v


def _validate_expected(v: dict[str, int]) -> dict[str, int]:
    for key, x in v.items():
        if isinstance(x, bool) or not isinstance(x, int):
            raise ValueError(f"expected values must be int: {key}={x!r}")
        if abs(x) > MAX_TASK_INPUT_ABS:
            raise ValueError(f"expected value out of range: {key}={x}")
    return v


class TaskCatalogEntry(_StrictModel):
    """Root 公開Versionに固定される Child タスクのカタログ要素。"""

    task_key: str = Field(min_length=1)
    input_values: list[StrictInt]
    expected: dict[str, StrictInt]
    budget_cap_units: Annotated[int, Field(gt=0, le=SQLITE_MAX_I64, strict=True)]

    @field_validator("input_values")
    @classmethod
    def _validate_input(cls, v: list[int]) -> list[int]:
        return _validate_task_input(v)

    @field_validator("expected")
    @classmethod
    def _validate_expected(cls, v: dict[str, int]) -> dict[str, int]:
        return _validate_expected(v)


class RootTaskDefinition(_StrictModel):
    """Root の最小実証タスク sum-v1 の定義。"""

    task_key: str = Field(min_length=1)
    verifier: str = Field(min_length=1)
    input_values: list[StrictInt]
    expected: dict[str, StrictInt]

    @field_validator("input_values")
    @classmethod
    def _validate_input(cls, v: list[int]) -> list[int]:
        return _validate_task_input(v)

    @field_validator("expected")
    @classmethod
    def _validate_expected(cls, v: dict[str, int]) -> dict[str, int]:
        return _validate_expected(v)


class Job(_StrictModel):
    id: str
    root_id: str
    parent_id: str | None = None
    requester_id: str
    state: JobState
    version_id: str | None = None
    active_lease_id: str | None = None
    row_version: Annotated[int, Field(ge=0, strict=True)]
    created_at_us: TimestampUs
    task_key: str | None = None
    creator_lease_id: str | None = None

    def is_root(self) -> bool:
        return self.parent_id is None and self.root_id == self.id

    def is_child(self) -> bool:
        return self.parent_id is not None

    def is_terminal(self) -> bool:
        return self.state in TERMINAL_JOB_STATES


class JobVersion(_StrictModel):
    """公開後 immutable。UNIQUE(job_id, version)。PoC は公開版1つ。"""

    id: str
    job_id: str
    version: Annotated[int, Field(ge=1, strict=True)]
    title: str
    budget_units: PositiveAmountUnits
    asset: str = ASSET_MOCK_USDC
    deadline_us: TimestampUs
    subcontract_policy: SubcontractPolicy
    timing_policy: TimingPolicy
    task_catalog: list[TaskCatalogEntry] = Field(default_factory=list)


class Lease(_StrictModel):
    id: str
    job_id: str
    worker_id: str
    version_id: str
    generation: Annotated[int, Field(ge=1, strict=True)]
    claimed_at_us: TimestampUs
    heartbeat_at_us: TimestampUs
    expires_at_us: TimestampUs
    closed_reason: LeaseClosedReason | None = None

    def is_active(self) -> bool:
        return self.closed_reason is None


class VerificationResult(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"


class Submission(_StrictModel):
    id: str
    job_id: str
    lease_id: str
    version_id: str
    artifact_json: str
    artifact_hash: str
    verification_result: VerificationResult
    verification_evidence: str
    submitted_at_us: TimestampUs
    valid_at_us: TimestampUs
    review_due_at_us: TimestampUs


class SubmissionAttempt(_StrictModel):
    id: str
    job_id: str
    lease_id: str
    input_hash: str
    outcome: VerificationResult
    reason: str
    attempted_at_us: TimestampUs


class BudgetAccount(_StrictModel):
    id: str
    root_id: str
    owner_job_id: str
    bucket: Bucket
    purpose: str | None = None
    amount_units: AmountUnits
    beneficiary_id: str | None = None
    source_key: str | None = None


class JournalTransaction(_StrictModel):
    operation_id: str
    reason: str
    created_at_us: TimestampUs


class JournalEntry(_StrictModel):
    operation_id: str
    entry_no: Annotated[int, Field(ge=0, strict=True)]
    account_id: str
    delta_units: SignedAmountUnits


class Operation(_StrictModel):
    operation_id: str
    actor_id: str
    kind: str
    payload_hash: str
    business_key: str | None = None
    status: OperationStatus
    result: str | None = None


class PaymentOperation(_StrictModel):
    operation_id: str
    business_key: str
    root_id: str
    job_id: str
    source_account_id: str
    amount_units: PositiveAmountUnits
    payee_id: str
    kind: PaymentKind
    status: PaymentStatus
    attempt_count: Annotated[int, Field(ge=0, strict=True)] = 0
    next_retry_at_us: TimestampUs | None = None
    last_error: str | None = None
    receipt_id: str | None = None


class MockWallet(_StrictModel):
    participant_id: str
    asset: str = ASSET_MOCK_USDC
    balance_units: AmountUnits


class TransferReceipt(_StrictModel):
    """送金の正本（計画書 第9節）。commit 後の応答消失時は Receipt が正本。

    source_account_id（migration 003 で追加、NOT NULL）は Mock.transfer が
    照合する 3 属性（金額・受取人・原資）の 1 つ。
    """

    receipt_id: str
    operation_id: str
    amount_units: AmountUnits
    payee_id: str
    asset: str = ASSET_MOCK_USDC
    source_account_id: str


class Acceptance(_StrictModel):
    job_id: str
    submission_id: str
    decision: AcceptanceDecision
    decided_by: str
    reason: str | None = None
    decided_at_us: TimestampUs


class Dispute(_StrictModel):
    id: str
    job_id: str
    submission_id: str
    opened_by: str
    reason_code: str
    condition_id: str
    evidence: str | None = None
    opened_at_us: TimestampUs
    due_at_us: TimestampUs
    status: DisputeStatus
    resolution: str | None = None


class Event(_StrictModel):
    root_id: str
    job_id: str
    actor_id: str
    action: str
    object_id: str | None = None
    at_us: TimestampUs


class RuntimeClock(_StrictModel):
    singleton_id: int = 1
    mode: ClockMode
    test_now_utc_us: TimestampUs | None = None
