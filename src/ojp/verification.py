"""固定検証器 sum-v1 と提出の障害注入 seam（計画書 第11節）。

純粋な検証ロジックだけを持ち、DB へは触れない（failpoint seam を除く）。
期待値・対象入力・検証器の版は引数で受け取ったもの（＝公開 Version 由来）
だけを使い、提出された成果物の中に期待値・テスト・判定器が書かれていても
一切参照しない（第11節: A/B が提出と一緒に期待結果・テスト・判定器を渡して
も採用しない）。

検証 FAIL（Worker の成果物の欠陥）と VERIFICATION_UNAVAILABLE（検証器の版
不整合・入力不備といったサーバー側の問題。再試行可能エラー）を区別する
（第11節「検証器例外・一時的 DB 失敗は VERIFICATION_UNAVAILABLE 等の
再試行可能エラーで、Worker の検証 FAIL とは区別する」）。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Callable

from . import clock, ledger
from .domain import (
    ClockMode,
    ErrorCode,
    MAX_ARTIFACT_BYTES,
    MAX_ARTIFACT_DEPTH,
    MAX_TASK_INPUT_ABS,
    MAX_TASK_INPUT_ELEMENTS,
    OjpError,
    VerificationResult,
)

VERIFIER_ID = "sum-v1"

# sum-v1 が合計値を格納する公開 expected・成果物のキー（計画書 第11節の
# 固定タスク。Root・Child とも期待 JSON は {"sum": <合計>} の形を持つ）。
SUM_RESULT_KEY = "sum"

# 検証器の版を表す仕様。verifier_hash() はこれを canonical JSON 化した文字列の
# sha256 で、規則を変更すれば hash も変わる。テストから同じ値を再計算できる。
_VERIFIER_SPEC = {
    "verifier_id": VERIFIER_ID,
    "rules": [
        "artifact utf-8 bytes <= 1048576",
        "artifact must parse as JSON without NaN/Infinity/-Infinity",
        "no duplicate keys in JSON objects",
        "nesting depth <= 8",
        "top level must be a JSON object",
        "key set must exactly match the published expected keys",
        "values must be int (bool is not accepted)",
        "values must exactly match the published expected values",
        "the 'sum' value must exactly match the sum of the published"
        " input_values",
    ],
    "published_inputs_consistency": (
        "published expected['sum'] must equal the sum of the published"
        " input_values (otherwise the verifier refuses the version as"
        " VERIFICATION_UNAVAILABLE)"
    ),
    "task": "sum of the published input_values",
}


def verifier_hash() -> str:
    """検証器の版を表す固定 hash（仕様文字列の canonical JSON の sha256）。"""
    canonical = ledger.canonical_json_dumps(_VERIFIER_SPEC)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class VerificationOutcome:
    """verify_artifact の結果。FAIL でも input_hash は必ず入れる。"""

    result: VerificationResult
    reason: str  # FAIL 理由コード。PASS のときは "OK"
    canonical_artifact: str | None  # PASS のときだけ canonical JSON
    artifact_hash: str | None  # PASS のときだけ canonical の sha256
    input_hash: str  # 提出された生成果物の sha256（FAIL でも必ず入れる）
    evidence: str  # 検証証跡の canonical JSON 文字列（時刻に依存しない決定的な内容）


# ---------------------------------------------------------------------------
# failpoint（障害注入）の seam。ledger の failpoint と同じ「テストが属性へ
# 関数を代入する」方式。test mode の DB でのみ発火する（realtime では
# 代入済みでも拒否する。第14節「failpoint は専用 test mode のみで有効」）。
# ---------------------------------------------------------------------------

failpoint_before_submission_commit: Callable[[str], None] | None = None


def assert_submission_failpoints_allowed(conn: sqlite3.Connection) -> None:
    """failpoint が代入済みなら、test mode の DB でなければ拒否する。

    submit の apply_effects の先頭・submissions / submission_attempts / jobs /
    leases / events を一切変更しない位置で呼ぶ前置検査（ledger.
    assert_failpoints_allowed と同じ考え方）。
    """
    if failpoint_before_submission_commit is None:
        return
    if clock.read_mode(conn) != ClockMode.TEST:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            "failpoint before_submission_commit is only available in test mode",
        )


def fire_before_submission_commit(conn: sqlite3.Connection) -> None:
    """PASS 判定後に submissions と Job 更新を書いた直後・apply_effects から
    return する直前に発火する。seam が例外を投げれば transaction 全体が
    rollback し、保存失敗（取得不能な提出）を再現できる。"""
    if failpoint_before_submission_commit is None:
        return
    if clock.read_mode(conn) != ClockMode.TEST:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            "failpoint before_submission_commit is only available in test mode",
        )
    failpoint_before_submission_commit("before_submission_commit")


# ---------------------------------------------------------------------------
# 裁定（arbitrate）と裁定用の failpoint seam（計画書 第12節・第17節
# 「裁定応答なし」）。submit の seam と同じ「テストが属性へ関数を代入する」
# 方式で、test mode の DB でのみ有効。
# ---------------------------------------------------------------------------

# 強制的に FAIL の VerificationOutcome を返させる seam（既存条件への
# FAIL 再現 fixture 用。X14「証拠付き FAIL」）。引数は dispute を識別する
# ラベル文字列。
arbiter_result_override: Callable[[str], VerificationOutcome] | None = None

# 例外を投げて「固定判定器が応答しない」を再現する seam（X14「裁定応答なし」）。
failpoint_arbiter_unresponsive: Callable[[str], None] | None = None


def assert_arbiter_failpoints_allowed(conn: sqlite3.Connection) -> None:
    """裁定 seam が代入済みなら、test mode の DB でなければ拒否する。

    resolve_due_disputes が何も書く前に呼ぶ前置検査（assert_submission_
    failpoints_allowed と同じ考え方。realtime では代入済みでも拒否する）。
    """
    if arbiter_result_override is None and failpoint_arbiter_unresponsive is None:
        return
    if clock.read_mode(conn) != ClockMode.TEST:
        raise OjpError(
            ErrorCode.INVALID_STATE,
            "arbitration failpoints are only available in test mode",
        )


# ---------------------------------------------------------------------------
# JSON 検証の内部 helper
# ---------------------------------------------------------------------------


class _NonFiniteConstantError(Exception):
    """json.loads が NaN / Infinity / -Infinity リテラルに遭遇した。"""


class _DuplicateKeyError(Exception):
    """JSON object に重複キーがあった。"""


def _parse_constant(name: str) -> Any:
    raise _NonFiniteConstantError(name)


def _object_pairs_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:
            raise _DuplicateKeyError(key)
        obj[key] = value
    return obj


def _container_depth(value: Any) -> int:
    """JSON 値の入れ子段数。トップレベルの object/array を 1 と数え、
    葉（str/int/float/bool/None）は 0 と数える。"""
    if isinstance(value, dict):
        return 1 + max((_container_depth(v) for v in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((_container_depth(v) for v in value), default=0)
    return 0


def _verify_published_inputs(
    *,
    input_values: Any,
    expected: Any,
) -> tuple[list[int], dict[str, int]]:
    """input_values / expected（公開 Version 由来）の健全性を検査する。

    入力と期待値は Root Requester が公開した Version 由来であり、Worker の
    責任ではないため、不備は FAIL ではなく VERIFICATION_UNAVAILABLE を投げる
    （第11節）。
    """
    if isinstance(input_values, bool) or not isinstance(input_values, list):
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            f"published input_values must be a list: {type(input_values).__name__}",
        )
    if len(input_values) > MAX_TASK_INPUT_ELEMENTS:
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            f"published input_values exceeds {MAX_TASK_INPUT_ELEMENTS} elements",
        )
    for x in input_values:
        if isinstance(x, bool) or not isinstance(x, int):
            raise OjpError(
                ErrorCode.VERIFICATION_UNAVAILABLE,
                f"published input_values must contain only int: {x!r}",
            )
        if abs(x) > MAX_TASK_INPUT_ABS:
            raise OjpError(
                ErrorCode.VERIFICATION_UNAVAILABLE,
                f"published input value out of range: {x}",
            )
    if isinstance(expected, bool) or not isinstance(expected, dict):
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            f"published expected must be a dict: {type(expected).__name__}",
        )
    for key, v in expected.items():
        if isinstance(v, bool) or not isinstance(v, int):
            raise OjpError(
                ErrorCode.VERIFICATION_UNAVAILABLE,
                f"published expected values must be int: {key}={v!r}",
            )
        if abs(v) > MAX_TASK_INPUT_ABS:
            raise OjpError(
                ErrorCode.VERIFICATION_UNAVAILABLE,
                f"published expected value out of range: {key}={v}",
            )
    return input_values, expected


def _evidence(
    *,
    input_hash: str,
    result: VerificationResult,
    reason: str,
    expected: dict[str, int],
    computed_sum: int,
) -> str:
    """検証証跡。時刻を含まない決定的な内容（同じ入力に対して常に同じ値）。"""
    return ledger.canonical_json_dumps(
        {
            "verifier_id": VERIFIER_ID,
            "verifier_hash": verifier_hash(),
            "input_hash": input_hash,
            "result": result.value,
            "reason": reason,
            "expected": expected,
            "computed_sum": computed_sum,
        }
    )


def verify_artifact(
    *,
    raw_artifact: str,
    input_values: list[int],
    expected: dict[str, Any],
    verifier_id: str,
    verifier_hash_value: str,
) -> VerificationOutcome:
    """固定検証器 sum-v1（計画書 第11節）。

    検証規則（この順序で判定し、最初に該当した理由コードを返す）:

    0. 公開 Version 自体の整合: sum(input_values) が expected の当該キー
       （SUM_RESULT_KEY）の値と一致しなければ FAIL ではなく
       OjpError(VERIFICATION_UNAVAILABLE)。公開 Version の不整合は
       Worker の責任ではない（第11節）。
    1. raw_artifact の UTF-8 バイト長 > MAX_ARTIFACT_BYTES
       → ARTIFACT_TOO_LARGE
    2. JSON として parse できない → ARTIFACT_NOT_JSON
    3. NaN / Infinity / -Infinity を含む → ARTIFACT_NOT_FINITE
    4. JSON object に重複キーがある → ARTIFACT_DUPLICATE_KEY
    5. ネスト深さ > MAX_ARTIFACT_DEPTH → ARTIFACT_TOO_DEEP
    6. トップレベルが JSON object でない → ARTIFACT_NOT_OBJECT
    7. キー集合が expected のキー集合と完全一致しない → ARTIFACT_KEY_MISMATCH
    8. 値が整数でない（bool は受理しない。type(v) is int で判定）
       → ARTIFACT_TYPE_INVALID
    9. 値が合計計算の結果（公開 input_values から計算した正確な合計）と
       一致しない → ARTIFACT_VALUE_MISMATCH。**判定の根拠を公開入力の
       計算値に置く**（手順 0 で expected との一致は確認済み。結果は
       同じでも根拠が異なる）
    - 上記すべてを通過 → PASS / "OK"

    verifier_id / verifier_hash_value が VERIFIER_ID / verifier_hash() と
    一致しない場合は FAIL ではなく OjpError(VERIFICATION_UNAVAILABLE) を
    投げる（Worker の成果物の欠陥ではなく、サーバー側の版不整合であるため）。
    input_values の上限超過・型違反も同じ理由で VERIFICATION_UNAVAILABLE
    （入力は Root Requester が公開した Version 由来であり、Worker の責任では
    ない）。
    """
    if verifier_id != VERIFIER_ID or verifier_hash_value != verifier_hash():
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            "verifier version mismatch: the published version references"
            f" verifier {verifier_id!r}/{verifier_hash_value!r} but this server"
            f" provides {VERIFIER_ID!r}/{verifier_hash()!r}",
        )
    clean_input, clean_expected = _verify_published_inputs(
        input_values=input_values, expected=expected
    )
    # 0. 公開 Version 自体の整合: 入力から合計を計算し、expected の当該キー
    #    と一致することを確認する（計画書 第11節「検証器は整数のみ、キー完全
    #    一致、正確な合計を検証する」）。不一致は FAIL ではなく
    #    VERIFICATION_UNAVAILABLE（公開 Version 自体の不整合であり、Worker
    #    の責任ではない）。
    if SUM_RESULT_KEY not in clean_expected:
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            f"published expected must contain the key {SUM_RESULT_KEY!r}"
            f" (got keys: {sorted(clean_expected.keys())})",
        )
    computed_sum = sum(clean_input)
    if clean_expected[SUM_RESULT_KEY] != computed_sum:
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            "published expected does not match the published input_values:"
            f" expected[{SUM_RESULT_KEY!r}]={clean_expected[SUM_RESULT_KEY]}"
            f" but sum(input_values)={computed_sum}"
            " (公開 Version 自体の不整合。Worker の責任ではない)",
        )

    raw_bytes = raw_artifact.encode("utf-8")
    input_hash = hashlib.sha256(raw_bytes).hexdigest()

    def _fail(reason: str) -> VerificationOutcome:
        return VerificationOutcome(
            result=VerificationResult.FAIL,
            reason=reason,
            canonical_artifact=None,
            artifact_hash=None,
            input_hash=input_hash,
            evidence=_evidence(
                input_hash=input_hash,
                result=VerificationResult.FAIL,
                reason=reason,
                expected=clean_expected,
                computed_sum=computed_sum,
            ),
        )

    # 1. サイズ
    if len(raw_bytes) > MAX_ARTIFACT_BYTES:
        return _fail("ARTIFACT_TOO_LARGE")
    # 2〜4. JSON 構造（parse 不可 / NaN / Infinity / 重複キー）
    try:
        parsed = json.loads(
            raw_artifact,
            parse_constant=_parse_constant,
            object_pairs_hook=_object_pairs_no_duplicates,
        )
    except _NonFiniteConstantError:
        return _fail("ARTIFACT_NOT_FINITE")
    except _DuplicateKeyError:
        return _fail("ARTIFACT_DUPLICATE_KEY")
    except (json.JSONDecodeError, ValueError):
        return _fail("ARTIFACT_NOT_JSON")
    # 5. 深さ
    if _container_depth(parsed) > MAX_ARTIFACT_DEPTH:
        return _fail("ARTIFACT_TOO_DEEP")
    # 6. トップレベルが object
    if not isinstance(parsed, dict):
        return _fail("ARTIFACT_NOT_OBJECT")
    # 7. キー集合の完全一致（過不足どちらも不一致）
    if set(parsed.keys()) != set(clean_expected.keys()):
        return _fail("ARTIFACT_KEY_MISMATCH")
    # 8. 値が整数（bool は受理しない）
    for key, value in parsed.items():
        if type(value) is not int:
            return _fail("ARTIFACT_TYPE_INVALID")
    # 9. 値の一致。SUM_RESULT_KEY（"sum"）の値は公開 input_values から計算した
    #    正確な合計と比較する（判定の根拠を公開入力に置く。手順 0 で
    #    expected との一致は確認済み。結果は同じでも根拠が異なる）。
    #    補助キー（サイズ境界 fixture の pad キー等）は公開 expected の
    #    固定値との比較を維持する（合計の計算根拠を持つのは SUM_RESULT_KEY
    #    だけであるため）
    for key, value in parsed.items():
        reference = computed_sum if key == SUM_RESULT_KEY else clean_expected[key]
        if value != reference:
            return _fail("ARTIFACT_VALUE_MISMATCH")
    # PASS: 保存対象を canonical 形式にして hash を計算
    canonical = ledger.canonical_json_dumps(parsed)
    artifact_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return VerificationOutcome(
        result=VerificationResult.PASS,
        reason="OK",
        canonical_artifact=canonical,
        artifact_hash=artifact_hash,
        input_hash=input_hash,
        evidence=_evidence(
            input_hash=input_hash,
            result=VerificationResult.PASS,
            reason="OK",
            expected=clean_expected,
            computed_sum=computed_sum,
        ),
    )


def arbitrate(
    *,
    stored_artifact_json: str,
    input_values: list[int],
    expected: dict[str, Any],
    verifier_id: str,
    verifier_hash_value: str,
    original_evidence: str,
) -> VerificationOutcome:
    """固定判定器による異議の再検証（計画書 第12節「DISPUTED」の裁定）。

    保存 JSON（submissions.artifact_json）・Claim 時 Version の入力と期待値
    （job_versions の input_json / conditions_json）・元の検証証跡
    （submissions.verification_evidence）で再検証する。verify_artifact を
    再利用し、金額の一部裁定は行わない（PASS / FAIL の二値のみ）。

    検証前の整合検査（不整合は OjpError(VERIFICATION_UNAVAILABLE)):

    - original_evidence が妥当な JSON object であり、verifier_id /
      verifier_hash が再検証の入力（公開 Version 由来）と一致すること。
      不一致は「保存時の検証と今の再検証が同じ検証器を見ていない」状態
      （サーバー側の問題）であるため
    - evidence の input_hash は提出時の生成果物の sha256 だが、
      submissions.artifact_json には canonical 形式が保存されるため、
      生成果物が既に canonical だった提出以外では hash が一致しない
      （生成果物のバイト列は保存されず復元できない）。そのため
      input_hash は 64 桁 hex の形式検査のみ行い、成果物の同一性は
      決定的な再検証（verify_artifact が保存成果物から提出時と同じ
      PASS と artifact_hash を再現すること）で担保する
    - arbiter_result_override seam が代入済みなら、再検証の代わりにその
      FAIL を返す（既存条件への FAIL 再現 fixture。test mode で
      resolve_due_disputes が呼ぶ。この関数自体は純粋なので mode 検査は
      呼出側の assert_arbiter_failpoints_allowed が担う）
    - failpoint_arbiter_unresponsive seam が代入済みなら例外を投げる
      （「固定判定器が応答しない」の再現。呼出側は transaction を
      rollback して異議を OPEN のまま残す）
    """
    raw_bytes = stored_artifact_json.encode("utf-8")
    try:
        original = json.loads(original_evidence)
    except (json.JSONDecodeError, ValueError):
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            "stored verification_evidence is not valid JSON",
        )
    if not isinstance(original, dict):
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            "stored verification_evidence must be a JSON object",
        )
    if original.get("verifier_id") != verifier_id:
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            "stored evidence verifier_id does not match the published version:"
            f" {original.get('verifier_id')!r} != {verifier_id!r}",
        )
    if original.get("verifier_hash") != verifier_hash_value:
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            "stored evidence verifier_hash does not match the published version",
        )
    input_hash_value = original.get("input_hash")
    if (
        not isinstance(input_hash_value, str)
        or len(input_hash_value) != 64
        or any(c not in "0123456789abcdef" for c in input_hash_value)
    ):
        raise OjpError(
            ErrorCode.VERIFICATION_UNAVAILABLE,
            "stored evidence input_hash is malformed (expected 64 hex chars)",
        )
    del raw_bytes  # 生成果物の hash は再検証できない（docstring 参照）
    if failpoint_arbiter_unresponsive is not None:
        failpoint_arbiter_unresponsive("arbiter_unresponsive")
    if arbiter_result_override is not None:
        return arbiter_result_override("arbiter_result_override")
    return verify_artifact(
        raw_artifact=stored_artifact_json,
        input_values=input_values,
        expected=expected,
        verifier_id=verifier_id,
        verifier_hash_value=verifier_hash_value,
    )
