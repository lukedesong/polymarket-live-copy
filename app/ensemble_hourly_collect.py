#!/usr/bin/env python3
"""Hourly ensemble temperature collector for shadow 4 nowcast.

RECORD ONLY. Open-Meteo Ensemble API, no key, no orders.
Writes /opt/ensemble-collect/data/ensemble_hourly/YYYY-MM-DD/HHMMZ.json
when deployed next to ensemble_daily_collect.py.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ENSEMBLE_API_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
MODELS = {
    "ecmwf_ifs025": 51,
    "gfs025": 31,
    "icon_seamless_eps": 40,
}
FORECAST_DAYS = 2
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 5
REQUEST_TIMEOUT_SEC = 120
CITY_CHUNK = 12


def _layout():
    cfg = SCRIPT_DIR / "config" / "ensemble_cities.json"
    if cfg.exists():
        return cfg, SCRIPT_DIR / "data" / "ensemble_hourly", SCRIPT_DIR / "data" / "hourly_status.json"
    repo_root = SCRIPT_DIR.parent
    alt = repo_root / "research" / "data" / "wx_bt" / "events_slim.json"
    return alt if alt.exists() else None, SCRIPT_DIR / "data" / "ensemble_hourly", None


def load_cities(cities_path):
    if cities_path is None or not Path(cities_path).exists():
        raise FileNotFoundError("ensemble city config missing")
    raw = json.loads(Path(cities_path).read_text())
    if isinstance(raw, list) and raw and "slug" in raw[0]:
        cities = []
        for row in raw:
            slug, lat, lon, tz = row.get("slug"), row.get("lat"), row.get("lon"), row.get("tz")
            if None in (slug, lat, lon, tz):
                continue
            cities.append({"slug": slug, "lat": lat, "lon": lon, "tz": tz})
        if not cities:
            raise RuntimeError("no cities with lat/lon/tz")
        return cities
    raise RuntimeError(f"unsupported city config shape: {cities_path}")


def _chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def fetch_model_chunk(cities, model):
    lats = ",".join(str(city["lat"]) for city in cities)
    lons = ",".join(str(city["lon"]) for city in cities)
    params = {
        "latitude": lats,
        "longitude": lons,
        "hourly": "temperature_2m",
        "models": model,
        "forecast_days": FORECAST_DAYS,
        "timezone": "auto",
    }
    url = f"{ENSEMBLE_API_URL}?{urllib.parse.urlencode(params)}"
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "polymarket-ensemble-hourly-collect/1.0"}
            )
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(f"API error: {data.get('reason')}")
            if isinstance(data, dict):
                data = [data]
            if not isinstance(data, list) or len(data) != len(cities):
                got = len(data) if isinstance(data, list) else type(data).__name__
                raise RuntimeError(
                    f"unexpected hourly shape model={model}: got={got} expected={len(cities)}"
                )
            return data
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            print(
                f"[warn] model={model} cities={len(cities)} attempt {attempt}/{MAX_RETRIES}: {exc}",
                file=sys.stderr,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC * attempt)
    print(f"[error] model={model} chunk failed: {last_err}", file=sys.stderr)
    return None


def member_keys(hourly_units):
    keys = sorted(
        (key for key in hourly_units if key.startswith("temperature_2m")),
        key=lambda key: (key != "temperature_2m", key),
    )
    if not keys:
        raise ValueError("no temperature_2m member fields")
    return keys


def parse_hourly_location(city, model, location_json, fetched_at_iso):
    if not isinstance(location_json, dict):
        raise ValueError(f"location payload is not an object: {type(location_json).__name__}")
    if location_json.get("error"):
        raise ValueError(location_json.get("reason", "unknown per-location API error"))
    hourly = location_json["hourly"]
    units = location_json["hourly_units"]
    times = hourly["time"]
    keys = member_keys(units)
    members = []
    for key in keys:
        series = hourly.get(key) or []
        row = []
        for index in range(len(times)):
            if index >= len(series) or series[index] is None:
                row.append(None)
            else:
                row.append(float(series[index]))
        if all(value is None for value in row):
            continue
        members.append(row)
    if not members:
        raise ValueError("no hourly member values")
    return {
        "fetched_at_utc": fetched_at_iso,
        "city": city["slug"],
        "lat": city["lat"],
        "lon": city["lon"],
        "timezone": city["tz"],
        "model": model,
        "hours": list(times),
        "members": members,
        "n_members": len(members),
    }


def main():
    fetched_at = datetime.now(timezone.utc)
    fetched_at_iso = fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    cities_path, output_root, status_path = _layout()
    cities = load_cities(cities_path)
    print(f"[info] hourly collect {len(cities)} cities from {cities_path}")
    all_records = []
    missing = []
    model_summary = {}
    for model in MODELS:
        summary = {"cities_ok": 0, "cities_missing": 0, "member_counts": set()}
        for chunk in _chunks(cities, CITY_CHUNK):
            raw = fetch_model_chunk(chunk, model)
            if raw is None:
                for city in chunk:
                    missing.append((city["slug"], model, "request_failed"))
                    summary["cities_missing"] += 1
                continue
            for city, loc in zip(chunk, raw):
                try:
                    rec = parse_hourly_location(city, model, loc, fetched_at_iso)
                    summary["member_counts"].add(rec["n_members"])
                    all_records.append(rec)
                    summary["cities_ok"] += 1
                except Exception as exc:  # noqa: BLE001
                    print(f"[warn] city={city['slug']} model={model}: {exc}", file=sys.stderr)
                    missing.append((city["slug"], model, str(exc)))
                    summary["cities_missing"] += 1
        model_summary[model] = summary
    output_root.mkdir(parents=True, exist_ok=True)
    out_dir = output_root / fetched_at.strftime("%Y-%m-%d")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{fetched_at.strftime('%H%MZ')}.json"
    out_path.write_text(json.dumps(all_records, ensure_ascii=False))
    mismatch = False
    print("===== hourly run summary =====")
    print(f"fetched_at_utc: {fetched_at_iso}")
    print(f"records: {len(all_records)} missing: {len(missing)}")
    for model, expected in MODELS.items():
        seen = sorted(model_summary.get(model, {}).get("member_counts", set()))
        flag = "" if seen == [expected] else "  <-- MISMATCH"
        print(
            f"  {model}: ok={model_summary[model]['cities_ok']}/{len(cities)} "
            f"members={seen} expected={expected}{flag}"
        )
        if seen != [expected]:
            mismatch = True
    print(f"output_file: {out_path} ({out_path.stat().st_size} bytes)")
    status = {
        "last_run_utc": fetched_at_iso,
        "last_run_ok": (len(missing) == 0) and (not mismatch) and bool(all_records),
        "n_records": len(all_records),
        "n_cities": len(cities),
        "missing_count": len(missing),
        "member_count_mismatch": mismatch,
        "output_file": str(out_path),
        "record_only": True,
        "real_order_submitted": False,
    }
    if status_path is not None:
        status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2))
    if not all_records:
        sys.exit(1)


if __name__ == "__main__":
    main()
