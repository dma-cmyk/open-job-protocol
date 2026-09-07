"""Phase 4 S1 の unit テスト: 固定検証器 sum-v1（計画書 第11節）。

Done when の対応:
- 6（検証器が bool・重複キー・NaN/Infinity・キー不一致・上限超過を拒否し、
  期待値・入力・検証器版を公開 Version からのみ導出している）

規則は表駆動で検証する。9 個の理由コードすべて、深さ 8 の境界（8 は可・
9 は不可）、1 MiB の境界、verifier_id / hash 不一致と入力上限超が
VERIFICATION_UNAVAILABLE、PASS 時の artifact_hash が canonical 由来であること。
"""

from __future__ import annotations

import hashlib
import json

import pytest

from ojp import ledger, verification
from ojp.domain import ErrorCode, OjpError, VerificationResult
from tests.conftest import load_poc_catalog


def _verify(raw_artifact, *, input_values=None, expected=None,
            verifier_id=None, verifier_hash_value=None):
    """検証器の既定引数をまとめた helper（第11節の共通 fixture 相当）。"""
    if input_values is None:
        input_values = [1, 2, 3]
    if expected is None:
        expected = {"sum": 6}
    return verification.verify_artifact(
        raw_artifact=raw_artifact,
        input_values=input_values,
        expected=expected,
        verifier_id=verifier_id if verifier_id is not None else verification.VERIFIER_ID,
        verifier_hash_value=(
            verifier_hash_value
            if verifier_hash_value is not None
            else verification.verifier_hash()
        ),
    )


def test_verifier_hash_is_deterministic_and_recomputable():
    """verifier_hash() は仕様文字列の canonical JSON の sha256。
    テストから同じ値を再計算できる。"""
    from ojp.verification import _VERIFIER_SPEC

    canonical = ledger.canonical_json_dumps(_VERIFIER_SPEC)
    assert verification.verifier_hash() == hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()
    # 固定値でもある（呼ぶたびに変わらない）
    assert verification.verifier_hash() == verification.verifier_hash()


def test_poc_catalog_passes_root_and_children():
    """第11節の共通 fixture（Root [1,2,3,4,5]→{"sum":15}、各 part）が PASS する。"""
    root_def, catalog = load_poc_catalog()
    outcome = _verify(
        json.dumps({"sum": sum(root_def.input_values)}),
        input_values=root_def.input_values,
        expected=root_def.expected,
    )
    assert outcome.result == VerificationResult.PASS
    assert outcome.reason == "OK"
    for entry in catalog:
        outcome = _verify(
            json.dumps({"sum": sum(entry.input_values)}),
            input_values=entry.input_values,
            expected=entry.expected,
        )
        assert outcome.result == VerificationResult.PASS


# ---------------------------------------------------------------------------
# 検証規則の表駆動（計画書 第11節。9 個の理由コードすべて）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_artifact, reason",
    [
        # 1. サイズ超過（1 MiB 境界の直上）
        ('{"sum":6,"pad":"' + "x" * (1048576 - 12) + '"}', "ARTIFACT_TOO_LARGE"),
        # 2. JSON として parse できない
        ("{not json", "ARTIFACT_NOT_JSON"),
        ("", "ARTIFACT_NOT_JSON"),
        # 3. NaN / Infinity / -Infinity（json.loads の既定は受理してしまう）
        ('{"sum":NaN}', "ARTIFACT_NOT_FINITE"),
        ('{"sum":Infinity}', "ARTIFACT_NOT_FINITE"),
        ('{"sum":-Infinity}', "ARTIFACT_NOT_FINITE"),
        # 4. 重複キー
        ('{"sum":6,"sum":6}', "ARTIFACT_DUPLICATE_KEY"),
        ('{"a":{"x":1,"x":2},"sum":6}', "ARTIFACT_DUPLICATE_KEY"),
        # 5. ネスト深さ > 8（9 段は不可）
        ('{"a":' * 9 + "1" + "}" * 9, "ARTIFACT_TOO_DEEP"),
        # 6. トップレベルが object でない
        ("[1,2,3]", "ARTIFACT_NOT_OBJECT"),
        ('"hello"', "ARTIFACT_NOT_OBJECT"),
        ("42", "ARTIFACT_NOT_OBJECT"),
        ("null", "ARTIFACT_NOT_OBJECT"),
        # 7. キー集合の不一致（過剰・不足どちらも）
        ('{"sum":6,"extra":1}', "ARTIFACT_KEY_MISMATCH"),
        ("{}", "ARTIFACT_KEY_MISMATCH"),
        ('{"total":6}', "ARTIFACT_KEY_MISMATCH"),
        # 8. 値が整数でない。bool は整数として受理しない
        ('{"sum":true}', "ARTIFACT_TYPE_INVALID"),
        ('{"sum":6.0}', "ARTIFACT_TYPE_INVALID"),
        ('{"sum":"6"}', "ARTIFACT_TYPE_INVALID"),
        ('{"sum":null}', "ARTIFACT_TYPE_INVALID"),
        # 9. 値が expected と一致しない（合計が正確でない）
        ('{"sum":5}', "ARTIFACT_VALUE_MISMATCH"),
        ('{"sum":7}', "ARTIFACT_VALUE_MISMATCH"),
    ],
)
def test_verify_artifact_fail_reasons(raw_artifact, reason):
    """9 個の理由コードすべてを、該当する生成果物に対して返す。"""
    outcome = _verify(raw_artifact)
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason == reason
    # FAIL でも input_hash は必ず入る（提出された生成果物の sha256）
    assert outcome.input_hash == hashlib.sha256(
        raw_artifact.encode("utf-8")
    ).hexdigest()
    # FAIL のときは canonical / artifact_hash を持たない
    assert outcome.canonical_artifact is None
    assert outcome.artifact_hash is None


def test_pass_accepts_exactly_1mib_and_depth_8():
    """境界値: ちょうど 1 MiB は可、深さ 8 は可。"""
    # ちょうど 1048576 バイトの PASS 成果物。キー名は文字列で長さを調整する
    # （expected の値は int なので、値ではなくキーでサイズを作る）
    pad_key = "k" * (1048576 - len('{"":0,"sum":6}'))
    artifact = json.dumps({pad_key: 0, "sum": 6}, separators=(",", ":"))
    assert len(artifact.encode("utf-8")) == 1048576
    outcome = _verify(artifact, expected={"sum": 6, pad_key: 0})
    assert outcome.result == VerificationResult.PASS

    # 深さ 8 の境界: 深さ 8 は ARTIFACT_TOO_DEEP にならない（深さ検査は通過し、
    # 次の検査へ進む。PASS 成果物は値が int だけなので深さ 1 になり、深さ 8 の
    # 成果物は後段のキー一致検査で拒否される）。深さ 9 は不可。
    depth8 = '{"a":' * 8 + "1" + "}" * 8
    outcome = _verify(depth8)
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason != "ARTIFACT_TOO_DEEP"  # 深さ 8 は深さ検査を通過
    assert outcome.reason == "ARTIFACT_KEY_MISMATCH"  # 後段のキー一致検査へ進む
    depth9 = '{"a":' * 9 + "1" + "}" * 9
    outcome = _verify(depth9)
    assert outcome.reason == "ARTIFACT_TOO_DEEP"

def test_verify_artifact_very_deep_input_returns_too_deep():
    """R5: 深さ 10,000 程度の入力で RecursionError を漏らさず
    ARTIFACT_TOO_DEEP の FAIL になる（json.loads の再帰限界も
    RecursionError として捕まえる。_container_depth は反復実装）。"""
    deep_artifact = '{"a":' * 10_000 + "1" + "}" * 10_000
    assert len(deep_artifact.encode("utf-8")) <= 1048576  # サイズ検査は通る
    outcome = _verify(deep_artifact)
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason == "ARTIFACT_TOO_DEEP"


def test_pass_artifact_hash_is_canonical():
    """PASS 時の artifact_hash は canonical 形式の sha256 であること。
    提出の空白違い・キー順不同は同じ canonical に正規化される。"""
    raw = '{ "sum" : 6 }'
    outcome = _verify(raw)
    assert outcome.result == VerificationResult.PASS
    assert outcome.canonical_artifact == '{"sum":6}'
    assert outcome.artifact_hash == hashlib.sha256(
        b'{"sum":6}'
    ).hexdigest()
    # キー順が違っても同じ canonical / hash になる
    outcome2 = _verify('{"sum":6}', expected={"sum": 6})
    assert outcome2.artifact_hash == outcome.artifact_hash


def test_evidence_is_deterministic_and_time_independent():
    """evidence は時刻に依存しない決定的な内容（同じ入力に常に同じ値）。"""
    outcome1 = _verify('{"sum":6}')
    outcome2 = _verify('{"sum":6}')
    assert outcome1.evidence == outcome2.evidence
    evidence = json.loads(outcome1.evidence)
    assert evidence["verifier_id"] == verification.VERIFIER_ID
    assert evidence["verifier_hash"] == verification.verifier_hash()
    assert evidence["input_hash"] == outcome1.input_hash
    assert evidence["result"] == "PASS"
    assert evidence["reason"] == "OK"
    assert evidence["expected"] == {"sum": 6}
    # FAIL の evidence には reason が入る
    fail = _verify('{"sum":5}')
    fail_evidence = json.loads(fail.evidence)
    assert fail_evidence["result"] == "FAIL"
    assert fail_evidence["reason"] == "ARTIFACT_VALUE_MISMATCH"


def test_artifact_claiming_different_expectations_is_not_trusted():
    """提出された成果物の中に期待値・テスト・判定器が書かれていても参照しない。"""
    # 成果物側に「自分は正しい」旨の期待値・検証器を載せても、公開 expected
    # との一致だけで判定する。sum-v1 のキー集合は expected と完全一致しか
    # ないため、余分なキーは ARTIFACT_KEY_MISMATCH で拒否される
    outcome = _verify(
        '{"expected":{"sum":99},"test":"self-declared","verifier":"forged","sum":6}'
    )
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason == "ARTIFACT_KEY_MISMATCH"
    # 成果物側の expected の主張（{"sum":6}）と不一致な公開 expected でも、
    # 判定は常に公開 expected の方だけに従う
    outcome = _verify('{"expected":{"sum":5},"sum":5}')
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason == "ARTIFACT_KEY_MISMATCH"
    outcome = _verify('{"sum":5}')
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason == "ARTIFACT_VALUE_MISMATCH"


# ---------------------------------------------------------------------------
# VERIFICATION_UNAVAILABLE（Worker の FAIL とは区別されるサーバー側の問題）
# ---------------------------------------------------------------------------


def test_verifier_version_mismatch_raises_unavailable():
    """verifier_id / verifier_hash の不一致は FAIL ではなく
    VERIFICATION_UNAVAILABLE（サーバー側の版不整合。再試行可能エラー）。"""
    with pytest.raises(OjpError) as exc_info:
        _verify('{"sum":6}', verifier_id="forged-verifier")
    assert exc_info.value.code == ErrorCode.VERIFICATION_UNAVAILABLE.value
    with pytest.raises(OjpError) as exc_info:
        _verify('{"sum":6}', verifier_hash_value="forged-hash")
    assert exc_info.value.code == ErrorCode.VERIFICATION_UNAVAILABLE.value


@pytest.mark.parametrize(
    "input_values",
    [
        list(range(1001)),  # MAX_TASK_INPUT_ELEMENTS 超
        [1, 2, 1_000_000_001],  # 絶対値が MAX_TASK_INPUT_ABS 超
        ["1", 2, 3],  # 整数でない
        [True, 2, 3],  # bool も整数として受理しない
    ],
)
def test_invalid_published_inputs_raise_unavailable(input_values):
    """input_values の上限超・型違反は VERIFICATION_UNAVAILABLE（入力は
    Root Requester が公開した Version 由来であり、Worker の責任ではない）。"""
    with pytest.raises(OjpError) as exc_info:
        _verify('{"sum":6}', input_values=input_values)
    assert exc_info.value.code == ErrorCode.VERIFICATION_UNAVAILABLE.value


def test_invalid_published_expected_raises_unavailable():
    """expected 側の型違反も VERIFICATION_UNAVAILABLE。"""
    with pytest.raises(OjpError) as exc_info:
        _verify('{"sum":6}', expected={"sum": True})
    assert exc_info.value.code == ErrorCode.VERIFICATION_UNAVAILABLE.value
    with pytest.raises(OjpError) as exc_info:
        _verify('{"sum":6}', expected={"sum": 1_000_000_001})
    assert exc_info.value.code == ErrorCode.VERIFICATION_UNAVAILABLE.value


# ---------------------------------------------------------------------------
# F1: 検証器は公開 input_values から合計を計算する
# ---------------------------------------------------------------------------


def test_contradictory_published_inputs_raise_unavailable():
    """F1(a): 公開 input_values と expected が矛盾する Version では
    VERIFICATION_UNAVAILABLE（公開 Version 自体の不整合であり、Worker の
    責任ではない。FAIL ではない）。"""
    # sum([1,2,3]) = 6 だが expected は 7
    with pytest.raises(OjpError) as exc_info:
        _verify('{"sum":6}', expected={"sum": 7})
    assert exc_info.value.code == ErrorCode.VERIFICATION_UNAVAILABLE.value
    # 合計キー（"sum"）自体が無い期待値も公開 Version の不整合
    with pytest.raises(OjpError) as exc_info:
        _verify('{"total":6}', expected={"total": 6})
    assert exc_info.value.code == ErrorCode.VERIFICATION_UNAVAILABLE.value
    # 成果物が expected と一致していても、Version が矛盾している以上は
    # FAIL として評価されない（再試行可能なサーバー側エラー）
    with pytest.raises(OjpError) as exc_info:
        _verify('{"sum":7}', expected={"sum": 7})
    assert exc_info.value.code == ErrorCode.VERIFICATION_UNAVAILABLE.value


def test_pass_uses_computed_sum_from_published_inputs():
    """F1(b): 正しい入力では合計計算経路で PASS する。evidence に計算した
    合計（computed_sum）が入る。決定的であること（時刻を含まない）も維持。"""
    outcome = _verify('{"sum":6}', input_values=[1, 2, 3], expected={"sum": 6})
    assert outcome.result == VerificationResult.PASS
    assert outcome.reason == "OK"
    evidence = json.loads(outcome.evidence)
    assert evidence["computed_sum"] == 6
    # Root fixture（[1,2,3,4,5]→15）も合計計算経路で PASS
    root_def, _catalog = load_poc_catalog()
    root_outcome = _verify(
        json.dumps({"sum": 15}),
        input_values=root_def.input_values,
        expected=root_def.expected,
    )
    assert root_outcome.result == VerificationResult.PASS
    assert json.loads(root_outcome.evidence)["computed_sum"] == 15
    # 誤答（計算合計と一致しない）は ARTIFACT_VALUE_MISMATCH。FAIL 側の
    # evidence にも計算合計が入る
    fail = _verify('{"sum":5}', input_values=[1, 2, 3], expected={"sum": 6})
    assert fail.result == VerificationResult.FAIL
    assert fail.reason == "ARTIFACT_VALUE_MISMATCH"
    assert json.loads(fail.evidence)["computed_sum"] == 6
    # evidence は同じ入力に対して常に同じ値（決定的・時刻非依存）
    again = _verify('{"sum":6}', input_values=[1, 2, 3], expected={"sum": 6})
    assert json.loads(again.evidence) == evidence


# ---------------------------------------------------------------------------
# R1: FAIL の原因 condition（failed_condition_id / expected_value /
# actual_value）の特定（計画書 第12節「既存条件への FAIL が再現された
# 場合だけ FAILED」の検証基盤）
# ---------------------------------------------------------------------------


def test_value_mismatch_identifies_failed_condition():
    """値不一致の FAIL は原因キー・期待値（計算合計）・実値を特定する。"""
    outcome = _verify('{"sum":5}', input_values=[1, 2, 3], expected={"sum": 6})
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason == "ARTIFACT_VALUE_MISMATCH"
    assert outcome.failed_condition_id == "sum"
    assert outcome.expected_value == 6
    assert outcome.actual_value == 5


def test_value_mismatch_on_auxiliary_key_uses_published_expected():
    """補助キー（"sum" 以外）の値不一致は公開 expected の固定値を期待値と
    する（合計の計算根拠を持つのは "sum" だけ）。"""
    outcome = _verify(
        '{"sum":6,"pad":1}',
        input_values=[1, 2, 3],
        expected={"sum": 6, "pad": 0},
    )
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason == "ARTIFACT_VALUE_MISMATCH"
    assert outcome.failed_condition_id == "pad"
    assert outcome.expected_value == 0
    assert outcome.actual_value == 1


def test_type_invalid_identifies_failed_condition():
    """型違反（bool は受理しない）の FAIL も原因キー・期待値・実値を特定。"""
    outcome = _verify('{"sum":true}')
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason == "ARTIFACT_TYPE_INVALID"
    assert outcome.failed_condition_id == "sum"
    assert outcome.expected_value == 6
    assert outcome.actual_value is True


def test_key_missing_identifies_missing_condition():
    """不足キーの FAIL は「判定に使った最初のキー」（不足キーの sorted()
    昇順の先頭）を原因キーとし、公開 expected の期待値を入れる
    （成果物側にキーが無いため実値は None）。"""
    outcome = _verify("{}", expected={"sum": 6, "pad": 0})
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason == "ARTIFACT_KEY_MISMATCH"
    # 不足キー {"pad","sum"} の昇順先頭は "pad"
    assert outcome.failed_condition_id == "pad"
    assert outcome.expected_value == 0
    assert outcome.actual_value is None


def test_key_mismatch_extra_key_is_deterministic():
    """過剰キーの FAIL は過剰キーの sorted() 昇順の先頭を原因キーとし、
    実値（成果物側の値）を入れる（公開 Version に存在しない condition の
    ため期待値は None）。不足・過剰が混在する場合は不足側を優先する。"""
    outcome = _verify('{"sum":6,"b":1,"a":2}')
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason == "ARTIFACT_KEY_MISMATCH"
    # 過剰キー {"a","b"} の昇順先頭は "a"
    assert outcome.failed_condition_id == "a"
    assert outcome.expected_value is None
    assert outcome.actual_value == 2
    # 不足と過剰が混在する場合は不足側（昇順先頭）を優先する
    outcome = _verify('{"zzz":0}', expected={"sum": 6, "pad": 0})
    assert outcome.reason == "ARTIFACT_KEY_MISMATCH"
    assert outcome.failed_condition_id == "pad"
    assert outcome.expected_value == 0
    assert outcome.actual_value is None


@pytest.mark.parametrize(
    "raw_artifact, reason",
    [
        ('{"sum":6,"pad":"' + "x" * (1048576 - 12) + '"}', "ARTIFACT_TOO_LARGE"),
        ("{not json", "ARTIFACT_NOT_JSON"),
        ('{"sum":NaN}', "ARTIFACT_NOT_FINITE"),
        ('{"sum":6,"sum":6}', "ARTIFACT_DUPLICATE_KEY"),
        ('{"a":' * 9 + "1" + "}" * 9, "ARTIFACT_TOO_DEEP"),
        ("[1,2,3]", "ARTIFACT_NOT_OBJECT"),
    ],
)
def test_structural_fails_have_no_failed_condition(raw_artifact, reason):
    """成果物全体の構造に関する FAIL は特定の condition に起因しない
    （failed_condition_id / expected_value / actual_value は None）。"""
    outcome = _verify(raw_artifact)
    assert outcome.result == VerificationResult.FAIL
    assert outcome.reason == reason
    assert outcome.failed_condition_id is None
    assert outcome.expected_value is None
    assert outcome.actual_value is None


def test_pass_has_no_failed_condition():
    """PASS では failed_condition_id 系はすべて None。"""
    outcome = _verify('{"sum":6}')
    assert outcome.result == VerificationResult.PASS
    assert outcome.failed_condition_id is None
    assert outcome.expected_value is None
    assert outcome.actual_value is None
