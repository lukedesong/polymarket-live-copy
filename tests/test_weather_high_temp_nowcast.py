from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional
from zoneinfo import ZoneInfo

from weather_high_temp_model import FORECAST_COVERAGE_BAND, attach_asks, buckets_from_gamma_event
from weather_high_temp_nowcast import (
    DEAD_EXCLUDED_FROM_S4,
    TAIL_RISK_UNKNOWN,
    HourlyMember,
    linear_temp_at,
    member_final_c,
    qualify_nowcast_no_books,
    remaining_max_c,
)


D = Decimal
TZ = ZoneInfo("Asia/Shanghai")
DAY = datetime(2026, 9, 12, 0, 0, tzinfo=TZ).date()


def _hour(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 9, 12, hour, minute, tzinfo=TZ)


def _series(pairs: list[tuple[int, float]]) -> HourlyMember:
    hours = tuple(_hour(hour) for hour, _temp in pairs)
    temps = tuple(D(str(temp)) for _hour_v, temp in pairs)
    return HourlyMember(hours=hours, temps_c=temps)


def _gz_event() -> dict:
    titles = ("30°C or below", "31°C", "32°C", "33°C", "34°C or higher")
    markets = []
    for index, title in enumerate(titles):
        markets.append(
            {
                "groupItemTitle": title,
                "outcomes": '["Yes", "No"]',
                "clobTokenIds": f'["yes-{index}", "no-{index}"]',
                "feeSchedule": {"exponent": 1, "rate": 0.05, "takerOnly": True},
            }
        )
    return {
        "slug": "highest-temperature-in-guangzhou-on-september-12-2026",
        "resolutionSource": "https://www.weather.gov/wrh/timeseries?site=zggg",
        "markets": markets,
    }


def _priced(*, yes: dict[str, Decimal], no: dict[str, Decimal]):
    buckets = buckets_from_gamma_event(_gz_event())
    return attach_asks(
        buckets,
        yes_asks={bucket.yes_token_id: yes[bucket.title] for bucket in buckets},
        no_asks={bucket.no_token_id: no[bucket.title] for bucket in buckets},
    )


def _track(at_obs: float, remaining: float, past_peak: Optional[float] = None) -> HourlyMember:
    pairs = []
    for hour in range(14, 24):
        temp = at_obs if hour <= 17 else remaining
        pairs.append((hour, temp))
    if past_peak is not None:
        pairs[0] = (14, past_peak)
    return _series(pairs)


def test_linear_interp_is_midpoint_and_does_not_extrapolate():
    member = _series([(16, 30.0), (17, 32.0)])
    assert linear_temp_at(member, _hour(16, 30)) == D("31")
    assert linear_temp_at(member, _hour(15, 0)) is None
    assert linear_temp_at(member, _hour(18, 0)) is None


def test_past_peak_is_dropped_from_remaining_max():
    member = _track(30.754, 31.5, past_peak=35.0)
    assert remaining_max_c(member, when=_hour(16, 30), event_date=DAY) == D("31.5")
    final = member_final_c(
        member,
        when=_hour(16, 30),
        event_date=DAY,
        delta_c=D("1.246"),
        observed_max_c=D("32"),
    )
    assert final == D("32.746")


def test_observed_max_floors_a_cool_remaining_track():
    member = _track(30.0, 30.0)
    final = member_final_c(
        member,
        when=_hour(16, 30),
        event_date=DAY,
        delta_c=D("0"),
        observed_max_c=D("32"),
    )
    assert final == D("32")


def test_no_decay_coefficient_exists():
    import weather_high_temp_nowcast as nowcast

    source = open(nowcast.__file__, encoding="utf-8").read()
    assert "decay_factor" not in source
    assert "衰减系数" not in source


def test_adverse_model_screens_no_and_zero_members_are_not_dead():
    yes = {
        "30°C or below": D("0.01"),
        "31°C": D("0.02"),
        "32°C": D("0.70"),
        "33°C": D("0.20"),
        "34°C or higher": D("0.01"),
    }
    no = {
        "30°C or below": D("0.99"),
        "31°C": D("0.98"),
        "32°C": D("0.30"),
        "33°C": D("0.99"),
        "34°C or higher": D("0.70"),
    }
    buckets = _priced(yes=yes, no=no)
    ecmwf = [_track(30.754, 31.5) for _ in range(45)] + [_track(30.754, 32.0) for _ in range(6)]
    gfs = [_track(30.813, 31.5) for _ in range(31)]
    icon = [_track(31.530, 31.5) for _ in range(36)] + [_track(31.530, 32.6) for _ in range(4)]
    receipt = qualify_nowcast_no_books(
        buckets,
        observed_max=D("32"),
        observed_max_c=D("32"),
        obs_temp_c=D("32"),
        obs_time=_hour(16, 30),
        event_date=DAY,
        unit="C",
        model_members={"ecmwf_ifs025": ecmwf, "gfs025": gfs, "icon_seamless_eps": icon},
        min_edge=D("0.04"),
        model_weight=D("0.5"),
        coverage_alpha=D("0.85"),
    )
    by_title = {row["title"]: row for row in receipt["buckets"]}
    assert receipt["poly_live_trading_armed"] is False
    assert receipt["coverage_alpha"] == "0.85"
    assert receipt["model_weight"] == "0.5"
    assert by_title["32°C"]["skip_reason"] == FORECAST_COVERAGE_BAND
    assert by_title["32°C"]["qualify"] is False
    thirty_three = by_title["33°C"]
    assert Decimal(thirty_three["p_adverse"]) == D("6") / D("51")
    assert thirty_three["tail_unknown"] is False
    assert thirty_three["qualify"] is False
    far = by_title["34°C or higher"]
    assert far["status"] == "possible"
    assert far["tail_unknown"] is True
    assert far["skip_reason"] == TAIL_RISK_UNKNOWN
    assert far["qualify"] is False
    dead = by_title["30°C or below"]
    assert dead["status"] == "dead"
    assert dead["qualify"] is False
    assert dead["skip_reason"] == DEAD_EXCLUDED_FROM_S4
    assert dead["race_vs_htt"] is False


def test_coverage_band_still_blocks_even_when_adverse_ev_looks_good():
    yes = {
        "30°C or below": D("0.01"),
        "31°C": D("0.01"),
        "32°C": D("0.96"),
        "33°C": D("0.01"),
        "34°C or higher": D("0.01"),
    }
    no = {
        "30°C or below": D("0.99"),
        "31°C": D("0.99"),
        "32°C": D("0.20"),
        "33°C": D("0.99"),
        "34°C or higher": D("0.99"),
    }
    buckets = _priced(yes=yes, no=no)
    members = [_track(32.0, 32.0) for _ in range(20)]
    receipt = qualify_nowcast_no_books(
        buckets,
        observed_max=D("32"),
        observed_max_c=D("32"),
        obs_temp_c=D("32"),
        obs_time=_hour(16, 30),
        event_date=DAY,
        unit="C",
        model_members={"ecmwf_ifs025": members},
        min_edge=D("0.04"),
        model_weight=D("0.5"),
    )
    center = next(row for row in receipt["buckets"] if row["title"] == "32°C")
    assert center["in_coverage_band"] is True
    assert center["qualify"] is False
    assert center["skip_reason"] == FORECAST_COVERAGE_BAND


def test_outside_band_still_needs_min_edge_0_04():
    yes = {
        "30°C or below": D("0.01"),
        "31°C": D("0.01"),
        "32°C": D("0.93"),
        "33°C": D("0.04"),
        "34°C or higher": D("0.01"),
    }
    no = {
        "30°C or below": D("0.99"),
        "31°C": D("0.99"),
        "32°C": D("0.10"),
        "33°C": D("0.97"),
        "34°C or higher": D("0.70"),
    }
    buckets = _priced(yes=yes, no=no)
    ecmwf = [_track(32.0, 32.0) for _ in range(49)] + [_track(32.0, 33.0) for _ in range(2)]
    receipt = qualify_nowcast_no_books(
        buckets,
        observed_max=D("32"),
        observed_max_c=D("32"),
        obs_temp_c=D("32"),
        obs_time=_hour(16, 30),
        event_date=DAY,
        unit="C",
        model_members={"ecmwf_ifs025": ecmwf},
        min_edge=D("0.04"),
        model_weight=D("0.5"),
    )
    row = next(item for item in receipt["buckets"] if item["title"] == "33°C")
    if row["in_coverage_band"]:
        assert row["qualify"] is False
        assert row["skip_reason"] == FORECAST_COVERAGE_BAND
    else:
        assert Decimal(row["ev"]) < D("0.04")
        assert row["qualify"] is False
        assert row["skip_reason"] == "NO_EDGE_BELOW_MIN"
