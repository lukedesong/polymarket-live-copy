from datetime import datetime, timezone
from decimal import Decimal

import pytest

from world_cup_mm.models import parse_clob_token_ids, parse_decimal, parse_utc_datetime


def test_parse_clob_token_ids_accepts_gamma_json_string():
    assert parse_clob_token_ids('["yes-token", "no-token"]') == (
        "yes-token",
        "no-token",
    )


def test_parse_clob_token_ids_rejects_empty_or_non_string_tokens():
    with pytest.raises(ValueError, match="invalid_clob_token_ids"):
        parse_clob_token_ids('[]')
    with pytest.raises(ValueError, match="invalid_clob_token_ids"):
        parse_clob_token_ids('["yes-token", 3]')


def test_parse_utc_datetime_normalizes_gamma_offset():
    assert parse_utc_datetime("2026-07-18 21:00:00+00") == datetime(
        2026, 7, 18, 21, tzinfo=timezone.utc
    )


def test_parse_utc_datetime_rejects_naive_time():
    with pytest.raises(ValueError, match="datetime_missing_timezone"):
        parse_utc_datetime("2026-07-18 21:00:00")


def test_parse_decimal_preserves_exact_source_text_and_defaults_missing_to_zero():
    assert parse_decimal("123.4500") == Decimal("123.4500")
    assert parse_decimal(None) == Decimal("0")
