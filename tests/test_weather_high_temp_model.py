from decimal import Decimal

import pytest

from weather_high_temp_model import (
    FORECAST_EXCLUDE_NEGATIVE,
    HTT_SKIP_REASON,
    HTT_WALLET,
    YES_OPTIONAL_REASON,
    WeatherModelError,
    attach_asks,
    buckets_from_gamma_event,
    forecast_exclude_no_ev,
    no_ev,
    parse_bucket_bounds,
    parse_station_icao,
    qualify_no_books,
    taker_fee,
    truncated_ensemble,
    whole_degree,
)
from weather_high_temp_qualify import (
    qualify_event,
    validate_weather_public_get,
)


D = Decimal


NYC_TITLES = (
    "69°F or below",
    "70-71°F",
    "72-73°F",
    "74-75°F",
    "76-77°F",
    "78-79°F",
    "80-81°F",
    "82-83°F",
    "84-85°F",
    "86-87°F",
    "88°F or higher",
)


def _market(title: str, yes_id: str, no_id: str, fee_rate: str = "0.05") -> dict:
    return {
        "groupItemTitle": title,
        "outcomes": '["Yes", "No"]',
        "clobTokenIds": json_tokens(yes_id, no_id),
        "feeSchedule": {"exponent": 1, "rate": float(fee_rate), "takerOnly": True},
    }


def json_tokens(yes_id: str, no_id: str) -> str:
    return f'["{yes_id}", "{no_id}"]'


def nyc_event() -> dict:
    markets = [
        _market(title, f"yes-{index}", f"no-{index}")
        for index, title in enumerate(NYC_TITLES)
    ]
    return {
        "slug": "highest-temperature-in-nyc-on-september-11-2026",
        "resolutionSource": "https://www.weather.gov/wrh/timeseries?site=klga",
        "markets": markets,
    }


def priced_nyc(
    *,
    yes_asks: dict[str, Decimal],
    no_asks: dict[str, Decimal],
):
    buckets = buckets_from_gamma_event(nyc_event())
    return attach_asks(
        buckets,
        yes_asks={bucket.yes_token_id: yes_asks[bucket.title] for bucket in buckets},
        no_asks={bucket.no_token_id: no_asks[bucket.title] for bucket in buckets},
    )


def nyc_market_prices(*, overpriced_tail: bool = False) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    yes = {
        "69°F or below": D("0.0005"),
        "70-71°F": D("0.0005"),
        "72-73°F": D("0.0005"),
        "74-75°F": D("0.0005"),
        "76-77°F": D("0.0005"),
        "78-79°F": D("0.14"),
        "80-81°F": D("0.665"),
        "82-83°F": D("0.215"),
        "84-85°F": D("0.0065"),
        "86-87°F": D("0.0045"),
        "88°F or higher": D("0.0005"),
    }
    no = {title: D("1") - price for title, price in yes.items()}
    if overpriced_tail:
        yes["84-85°F"] = D("0.15")
        no["84-85°F"] = D("0.80")
    return yes, no


def test_parse_bucket_titles_and_klga_station():
    assert parse_bucket_bounds("69°F or below") == (None, D("69"), "F")
    assert parse_bucket_bounds("70-71°F") == (D("70"), D("71"), "F")
    assert parse_bucket_bounds("88°F or higher") == (D("88"), None, "F")
    assert parse_bucket_bounds("33°C") == (D("33"), D("33"), "C")
    assert parse_station_icao(
        "https://www.weather.gov/wrh/timeseries?site=klga"
    ) == "KLGA"
    assert whole_degree(D("80.9")) == D("80")


def test_dead_buckets_are_htt_race_skips_when_observed_max_is_80():
    yes, no = nyc_market_prices(overpriced_tail=True)
    buckets = priced_nyc(yes_asks=yes, no_asks=no)
    members = [D("81")] * 10 + [D("82")] * 8 + [D("83")] * 2
    receipt = qualify_no_books(
        buckets,
        observed_max=D("80"),
        members=members,
    )
    by_title = {row["title"]: row for row in receipt["buckets"]}
    for title in NYC_TITLES[:6]:
        assert by_title[title]["status"] == "dead"
        assert by_title[title]["qualify"] is False
        assert by_title[title]["skip_reason"] == HTT_SKIP_REASON
    assert by_title["80-81°F"]["status"] == "possible"
    assert receipt["htt_wallet"] == HTT_WALLET
    assert receipt["poly_live_trading_armed"] is False


def test_truncated_ensemble_cannot_fall_below_observed_max():
    truncated = truncated_ensemble(
        [D("70"), D("79.9"), D("86")],
        observed_max=D("80.2"),
    )
    assert truncated == (D("80"), D("80"), D("86"))


def test_overpriced_live_tail_no_qualifies_after_fees():
    yes, no = nyc_market_prices(overpriced_tail=True)
    buckets = priced_nyc(yes_asks=yes, no_asks=no)
    members = [D("81")] * 20 + [D("82")] * 10
    receipt = qualify_no_books(
        buckets,
        observed_max=D("80"),
        members=members,
        min_edge=D("0.04"),
        model_weight=D("0.8"),
        spread_inflation=D("0"),
    )
    by_title = {row["title"]: row for row in receipt["buckets"]}
    tail = by_title["84-85°F"]
    assert tail["status"] == "possible"
    assert tail["qualify"] is True
    assert Decimal(tail["ev"]) >= D("0.04")
    center = by_title["80-81°F"]
    assert center["qualify"] is False


def test_matching_model_and_market_does_not_qualify():
    yes = {title: D("0.09") for title in NYC_TITLES}
    yes["80-81°F"] = D("0.34")
    yes["82-83°F"] = D("0.34")
    no = {title: D("0.99") for title in NYC_TITLES}
    buckets = priced_nyc(yes_asks=yes, no_asks=no)
    members = [D("81")] * 10 + [D("82")] * 10
    receipt = qualify_no_books(
        buckets,
        observed_max=D("80"),
        members=members,
        spread_inflation=D("0"),
        model_weight=D("0.5"),
    )
    assert all(row["qualify"] is False for row in receipt["buckets"])


def test_forecast_exclude_basket_is_negative_after_weather_fees():
    yes, no = nyc_market_prices()
    buckets = priced_nyc(yes_asks=yes, no_asks=no)
    # Point forecast misses the neighboring bucket half the time.
    members = [D("81")] * 20 + [D("83")] * 20
    receipt = qualify_no_books(
        buckets,
        observed_max=D("80"),
        members=members,
        spread_inflation=D("0"),
        model_weight=D("1"),
    )
    diagnosis = receipt["forecast_exclude_diagnosis"]
    assert diagnosis["label"] == FORECAST_EXCLUDE_NEGATIVE
    assert Decimal(diagnosis["ev"]) < D("0")


def test_yes_cluster_is_optional_and_never_qualifies():
    yes, no = nyc_market_prices(overpriced_tail=True)
    buckets = priced_nyc(yes_asks=yes, no_asks=no)
    members = [D("81")] * 10
    receipt = qualify_no_books(
        buckets,
        observed_max=D("80"),
        members=members,
    )
    optional = receipt["yes_optional"]
    assert optional["qualify"] is False
    assert optional["claimed_positive_edge"] is False
    assert optional["skip_reason"] == YES_OPTIONAL_REASON


def test_missing_fee_rate_does_not_qualify():
    event = nyc_event()
    event["markets"][8]["feeSchedule"] = {}
    buckets = buckets_from_gamma_event(event)
    yes, no = nyc_market_prices(overpriced_tail=True)
    priced = attach_asks(
        buckets,
        yes_asks={bucket.yes_token_id: yes[bucket.title] for bucket in buckets},
        no_asks={bucket.no_token_id: no[bucket.title] for bucket in buckets},
    )
    receipt = qualify_no_books(
        priced,
        observed_max=D("80"),
        members=[D("81")] * 10,
        spread_inflation=D("0"),
        model_weight=D("1"),
    )
    tail = next(row for row in receipt["buckets"] if row["title"] == "84-85°F")
    assert tail["qualify"] is False
    assert tail["skip_reason"] == "MISSING_FEE_RATE"


def test_taker_fee_is_rate_times_p_times_one_minus_p():
    assert taker_fee(rate=D("0.05"), price=D("0.80")) == D("0.008")
    ev = no_ev(p_yes=D("0.05"), no_ask=D("0.80"), fee_rate=D("0.05"))
    assert ev == D("0.142")


def test_qualify_event_skips_without_observation_or_ensemble():
    event = nyc_event()

    def books(_token: str) -> dict:
        raise AssertionError("no books without observation")

    missing_obs = qualify_event(
        event,
        observed_max=None,
        ensemble_members=[D("81")],
        book_for_token=books,
        ts_utc="2026-09-11T00:00:00Z",
    )
    assert missing_obs["skip_reason"] == "MISSING_OBSERVED_MAX"
    assert missing_obs["poly_live_trading_armed"] is False

    missing_ens = qualify_event(
        event,
        observed_max=D("80"),
        ensemble_members=(),
        book_for_token=books,
        ts_utc="2026-09-11T00:00:00Z",
    )
    assert missing_ens["skip_reason"] == "MISSING_ENSEMBLE"


def test_qualify_event_builds_no_receipt_without_arming_live_trading():
    event = nyc_event()
    yes, no = nyc_market_prices(overpriced_tail=True)
    buckets = buckets_from_gamma_event(event)
    yes_by_token = {bucket.yes_token_id: yes[bucket.title] for bucket in buckets}
    no_by_token = {bucket.no_token_id: no[bucket.title] for bucket in buckets}

    def books(token: str) -> dict:
        price = yes_by_token.get(token) or no_by_token[token]
        return {"asks": [{"price": str(price), "size": "20"}]}

    receipt = qualify_event(
        event,
        observed_max=D("80"),
        ensemble_members=[D("81")] * 20 + [D("82")] * 10,
        book_for_token=books,
        ts_utc="2026-09-11T12:00:00Z",
    )
    assert receipt["station"] == "KLGA"
    assert receipt["poly_live_trading_armed"] is False
    assert receipt["yes_claimed_positive_edge"] is False
    assert any(row["qualify"] for row in receipt["buckets"])


def test_weather_public_get_does_not_open_the_live_copy_reader():
    validate_weather_public_get(
        "https://gamma-api.polymarket.com/events?slug=highest-temperature-in-nyc-on-september-11-2026"
    )
    validate_weather_public_get(
        "https://clob.polymarket.com/book?token_id=123"
    )
    with pytest.raises(Exception):
        validate_weather_public_get("https://clob.polymarket.com/auth/api-key")
    with pytest.raises(Exception):
        validate_weather_public_get("https://api.open-meteo.com/v1/forecast")


def test_weather_modules_do_not_import_live_copy_or_arm_trading():
    import weather_high_temp_model as model
    import weather_high_temp_qualify as qualify

    assert "cd90_live_copy" not in model.__dict__
    assert "cd90_live_copy" not in qualify.__dict__
    text = open(qualify.__file__, encoding="utf-8").read()
    assert "os.environ" not in text
    assert "CLOBExecutionAdapter" not in text


def test_unparseable_title_fails_closed():
    with pytest.raises(WeatherModelError):
        parse_bucket_bounds("hottest in nyc")


def test_forecast_exclude_helper_uses_blended_mode_bucket():
    yes, no = nyc_market_prices()
    buckets = priced_nyc(yes_asks=yes, no_asks=no)
    probs = [D("0")] * len(buckets)
    probs[6] = D("0.5")
    probs[7] = D("0.5")
    ev, label = forecast_exclude_no_ev(buckets, probs)
    assert label == FORECAST_EXCLUDE_NEGATIVE
    assert ev < D("0")
