"""金額の整数変換の Unit テスト（計画書 第5節）。"""

from __future__ import annotations

import pytest

from ojp.domain import (
    SQLITE_MAX_I64,
    UNIT_SCALE,
    MoneyError,
    format_amount_units,
    parse_amount_units,
)


class TestParseAmountUnits:
    def test_valid_amounts(self) -> None:
        assert parse_amount_units("100.000000") == 100 * UNIT_SCALE
        assert parse_amount_units("0.000001") == 1
        assert parse_amount_units("20.000000") == 20 * UNIT_SCALE
        assert parse_amount_units("0.000000", allow_zero=True) == 0

    def test_negative_rejected(self) -> None:
        with pytest.raises(MoneyError, match="non-negative"):
            parse_amount_units("-1.000000")

    def test_more_than_six_decimals_rejected(self) -> None:
        with pytest.raises(MoneyError, match="6 decimal"):
            parse_amount_units("1.0000000")
        with pytest.raises(MoneyError, match="6 decimal"):
            parse_amount_units("1.1234567")

    def test_fewer_than_six_decimals_rejected(self) -> None:
        with pytest.raises(MoneyError):
            parse_amount_units("1.5")
        with pytest.raises(MoneyError):
            parse_amount_units("1")
        with pytest.raises(MoneyError):
            parse_amount_units("100")

    def test_float_and_int_rejected(self) -> None:
        with pytest.raises(MoneyError):
            parse_amount_units(100.0)
        with pytest.raises(MoneyError):
            parse_amount_units(100_000_000)
        with pytest.raises(MoneyError):
            parse_amount_units(True)

    def test_sqlite_i64_overflow_rejected(self) -> None:
        over = SQLITE_MAX_I64 // UNIT_SCALE + 1
        with pytest.raises(MoneyError, match="64-bit"):
            parse_amount_units(f"{over}.000000")

    def test_zero_rejected_by_default(self) -> None:
        with pytest.raises(MoneyError, match="positive"):
            parse_amount_units("0.000000")

    def test_garbage_rejected(self) -> None:
        with pytest.raises(MoneyError):
            parse_amount_units("abc.def")
        with pytest.raises(MoneyError):
            parse_amount_units("1.00000a")
        with pytest.raises(MoneyError):
            parse_amount_units("1..000000")
        with pytest.raises(MoneyError):
            parse_amount_units("")


class TestFormatAmountUnits:
    def test_format(self) -> None:
        assert format_amount_units(100_000_000) == "100.000000"
        assert format_amount_units(20_000_000) == "20.000000"
        assert format_amount_units(1) == "0.000001"
        assert format_amount_units(0) == "0.000000"

    def test_roundtrip(self) -> None:
        for units in (0, 1, 999_999, 20_000_000, 123_456_789_012_345):
            assert parse_amount_units(format_amount_units(units), allow_zero=True) == units

    def test_invalid_units_rejected(self) -> None:
        with pytest.raises(MoneyError):
            format_amount_units(-1)
        with pytest.raises(MoneyError):
            format_amount_units(1.5)
        with pytest.raises(MoneyError):
            format_amount_units(True)
        with pytest.raises(MoneyError):
            format_amount_units(SQLITE_MAX_I64 + 1)
