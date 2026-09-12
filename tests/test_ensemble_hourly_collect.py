from ensemble_hourly_collect import member_keys, parse_hourly_location


def test_parse_hourly_location_keeps_control_and_members():
    city = {"slug": "guangzhou", "lat": 23.39, "lon": 113.3, "tz": "Asia/Shanghai"}
    loc = {
        "hourly": {
            "time": ["2026-09-12T16:00", "2026-09-12T17:00"],
            "temperature_2m": [30.7, 31.1],
            "temperature_2m_member01": [30.5, 32.0],
        },
        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "°C",
            "temperature_2m_member01": "°C",
        },
    }
    rec = parse_hourly_location(city, "ecmwf_ifs025", loc, "2026-09-12T10:00:00Z")
    assert rec["city"] == "guangzhou"
    assert rec["n_members"] == 2
    assert rec["hours"] == ["2026-09-12T16:00", "2026-09-12T17:00"]
    assert rec["members"][0] == [30.7, 31.1]
    assert rec["members"][1] == [30.5, 32.0]
    assert member_keys(loc["hourly_units"])[0] == "temperature_2m"


def test_hourly_collector_is_record_only():
    source = open("app/ensemble_hourly_collect.py", encoding="utf-8").read()
    assert "POLYMARKET_LIVE_TRADING" not in source
    assert "clob.polymarket.com" not in source
    assert "record_only" in source
