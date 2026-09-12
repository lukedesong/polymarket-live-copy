#!/usr/bin/env python3
"""Shadow 4: nowcast-reprice forecast sleeve (RECORD ONLY).

Parallel to wx-s3-shadow. Does not modify it. Never submits CLOB orders.
Does not read POLYMARKET_LIVE_TRADING as an arming switch. Dead leftover
is not booked here.

Frozen s3 gates: 85% coverage band, min_edge 0.04, 50/50 market blend,
px>=0.99 skip, $100/bin, walk the ask book. Probability engine only:
hourly members + obs bias on remaining hours + observed-max floor;
screen still-possible NO with the most adverse model. Zero members is
tail unknown, not dead.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

HERE = Path(__file__).resolve().parent
for _candidate in (HERE, HERE.parent / "app"):
    if (_candidate / "weather_high_temp_model.py").exists():
        sys.path.insert(0, str(_candidate))
        break

from weather_high_temp_model import (  # noqa: E402
    FORECAST_COVERAGE_BAND,
    WeatherModelError,
    attach_asks,
    buckets_from_gamma_event,
    no_ev,
    parse_station_icao,
    taker_fee,
    whole_degree,
)
from weather_high_temp_nowcast import (  # noqa: E402
    DEAD_EXCLUDED_FROM_S4,
    HourlyMember,
    qualify_nowcast_no_books,
)
from weather_high_temp_qualify import _best_ask  # noqa: E402


GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
WRH_TOKEN_JS = "https://www.weather.gov/source/wrh/apiKey.js"
WRH_PAGE = "https://www.weather.gov/wrh/timeseries?site={icao}"
SYNOPTIC = "https://api.synopticdata.com/v2/stations/timeseries"
UA = {"User-Agent": "wx-s4-shadow/1.0", "Accept": "application/json"}
WRH_UA = {"User-Agent": "Mozilla/5.0"}
ALLOWED_HOSTS = {
    "gamma-api.polymarket.com",
    "clob.polymarket.com",
    "www.weather.gov",
    "api.synopticdata.com",
}
SELL = Decimal("0.99")
FEE = Decimal("0.05")
MIN_SHARES = Decimal("5")
MODELS = ("ecmwf_ifs025", "gfs025", "icon_seamless_eps")
SLUG_DATE = re.compile(r"on-([a-z]+)-(\d+)-(\d{4})$")
MONTHS = {
    name: index
    for index, name in enumerate(
        "january february march april may june july august september october november december".split(),
        1,
    )
}
FILE_TS = re.compile(r"^(\d{2})(\d{2})Z\.json$")

DEFAULT_CFG = {
    "shadow_name": "wx-s4-shadow",
    "aum_usd": "5000",
    "size_per_bin_usd": "100",
    "leftover_ask_max": "0.99",
    "min_shares": "5",
    "shares_decimals": 2,
    "min_edge": "0.04",
    "coverage_alpha": "0.85",
    "model_weight": "0.5",
    "tick_s": 20,
    "ensemble_data_root": "/opt/ensemble-collect/data/ensemble_hourly",
    "ensemble_city_config": "/opt/ensemble-collect/config/ensemble_cities.json",
    "clock_watch_path": "/opt/noaa-max-clock/data/city_watch.json",
    "real_order_submitted": False,
    "record_only": True,
}

LEDGER_COLS = [
    "ts",
    "kind",
    "event",
    "cid",
    "token",
    "shares",
    "px",
    "usdc",
    "fee",
    "reason",
    "book",
    "official",
    "city",
    "date",
    "bin",
    "unit",
    "sleeve",
    "observed_max",
    "observed_whole",
    "coverage_band",
    "in_coverage_band",
    "p_model",
    "p_adverse",
    "p_market_devig",
    "p_blend",
    "ev",
    "obs_temp_c",
    "obs_time_utc",
    "tail_unknown",
    "nowcast_deltas",
    "min_edge",
    "ensemble_file_used",
    "n_members",
    "market_no_ask",
    "market_yes_ask",
    "exit_type",
    "exit_price",
    "pnl",
    "skip_reason",
]


def D(value: Any) -> Decimal:
    return Decimal(str(value))


def city_of(slug: str) -> str:
    return slug.split("highest-temperature-in-", 1)[1].split("-on-", 1)[0]


def event_date_of(slug: str) -> date | None:
    matched = SLUG_DATE.search(slug or "")
    if not matched or matched.group(1) not in MONTHS:
        return None
    return date(int(matched.group(3)), MONTHS[matched.group(1)], int(matched.group(2)))


def c_to_unit(celsius: Decimal, unit: str) -> Decimal:
    if unit.upper() == "F":
        return celsius * Decimal("9") / Decimal("5") + Decimal("32")
    return celsius


def quantize_shares(shares: Decimal, decimals: int) -> Decimal:
    quant = Decimal("1").scaleb(-decimals)
    return shares.quantize(quant, rounding=ROUND_DOWN)


def buy_cost(shares: Decimal, px: Decimal, fee_rate: Decimal) -> Decimal:
    return shares * (px + taker_fee(rate=fee_rate, price=px))


def rest_099_proceeds(shares: Decimal) -> Decimal:
    return shares * SELL


def validate_s3_get(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    if parsed.scheme != "https" or parsed.username or parsed.password:
        raise RuntimeError(f"GET rejected: {url}")
    host = parsed.hostname
    if host not in ALLOWED_HOSTS:
        raise RuntimeError(f"GET host rejected: {host}")
    if host == "gamma-api.polymarket.com" and parsed.path == "/events":
        return
    if host == "gamma-api.polymarket.com" and parsed.path == "/markets" and query.get("clob_token_ids"):
        return
    if host == "clob.polymarket.com" and parsed.path == "/book" and query.get("token_id"):
        return
    if host == "www.weather.gov" and parsed.path == "/source/wrh/apiKey.js":
        return
    if host == "api.synopticdata.com" and parsed.path == "/v2/stations/timeseries":
        return
    raise RuntimeError(f"GET path rejected: {url}")


def headers_for(url: str, extra: dict | None = None) -> dict[str, str]:
    host = urllib.parse.urlparse(url).hostname
    merged = dict(WRH_UA if host in {"www.weather.gov", "api.synopticdata.com"} else UA)
    if extra:
        merged.update(extra)
    return merged


def http_json(url: str, timeout: float = 20, headers: dict | None = None) -> Any:
    validate_s3_get(url)
    req = urllib.request.Request(url, headers=headers_for(url, headers), method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def http_text(url: str, timeout: float = 20) -> str:
    validate_s3_get(url)
    req = urllib.request.Request(url, headers=headers_for(url), method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def clob_book(token: str) -> dict[str, Any]:
    return http_json(f"{CLOB}/book?token_id={token}")


def best_bid(book: dict[str, Any]) -> Decimal | None:
    bids = book.get("bids")
    if not isinstance(bids, list) or not bids:
        return None
    prices = []
    for level in bids:
        if not isinstance(level, dict) or level.get("price") is None:
            continue
        price = D(level["price"])
        if price > 0:
            prices.append(price)
    return max(prices) if prices else None


def ask_size_at(book: dict[str, Any], price: Decimal) -> Decimal:
    asks = book.get("asks")
    if not isinstance(asks, list):
        return Decimal("0")
    total = Decimal("0")
    for level in asks:
        if not isinstance(level, dict):
            continue
        if D(level.get("price") or "0") != price:
            continue
        total += D(level.get("size") or "0")
    return total


def ask_levels(book: dict[str, Any]) -> list[tuple[Decimal, Decimal]]:
    merged: dict[Decimal, Decimal] = {}
    for level in book.get("asks") or []:
        if not isinstance(level, dict):
            continue
        px = D(level.get("price") or "0")
        sz = D(level.get("size") or "0")
        if px <= 0 or sz <= 0:
            continue
        merged[px] = merged.get(px, Decimal("0")) + sz
    return sorted(merged.items())


def walk_ask_clips(
    book: dict[str, Any],
    *,
    budget: Decimal,
    fee_rate: Decimal,
    ask_max: Decimal,
    min_sh: Decimal,
    decimals: int,
    p_yes: Decimal,
    min_ev: Decimal,
    dead: bool,
) -> list[dict[str, Decimal]]:
    remain = budget
    clips: list[dict[str, Decimal]] = []
    if remain <= 0:
        return clips
    for px, sz in ask_levels(book):
        if px >= ask_max:
            break
        ev = no_ev(p_yes=p_yes, no_ask=px, fee_rate=fee_rate)
        if dead:
            if ev <= 0:
                break
        elif ev < min_ev:
            break
        unit = px + taker_fee(rate=fee_rate, price=px)
        if unit <= 0:
            continue
        max_sh = quantize_shares(remain / unit, decimals)
        level_sz = quantize_shares(sz, decimals)
        take = min(max_sh, level_sz)
        if take < min_sh:
            if level_sz < min_sh:
                continue
            break
        cost = buy_cost(take, px, fee_rate)
        if cost > remain:
            break
        clips.append(
            {
                "px": px,
                "shares": take,
                "cost": cost,
                "fee": take * taker_fee(rate=fee_rate, price=px),
                "ev": ev,
            }
        )
        remain -= cost
        if remain <= 0:
            break
    return clips


class Store:
    def __init__(self, path: Path, aum: Decimal):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.aum = aum
        self.con = sqlite3.connect(str(path))
        self.con.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        cols = ",\n                ".join(
            f"{col} REAL" if col == "ts" else f"{col} TEXT" for col in LEDGER_COLS
        )
        self.con.execute(
            f"""CREATE TABLE IF NOT EXISTS ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {cols}
            )"""
        )
        self.con.execute(
            """CREATE TABLE IF NOT EXISTS pos (
                token TEXT PRIMARY KEY,
                event TEXT,
                cid TEXT,
                shares TEXT,
                cost TEXT,
                status TEXT
            )"""
        )
        self.con.commit()
        self._ensure_ledger_cols()

    def _ensure_ledger_cols(self):
        have = {row[1] for row in self.con.execute("PRAGMA table_info(ledger)")}
        for col in LEDGER_COLS:
            if col in have:
                continue
            typ = "REAL" if col == "ts" else "TEXT"
            self.con.execute(f"ALTER TABLE ledger ADD COLUMN {col} {typ}")
        self.con.commit()

    def get(self, key: str, default=None):
        row = self.con.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
        return default if row is None else row[0]

    def set(self, key: str, value: Any):
        self.con.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", (key, str(value)))
        self.con.commit()

    def cash(self) -> Decimal:
        return D(self.get("cash", str(self.aum)))

    def seen(self, key: str) -> bool:
        return self.get(f"seen:{key}") is not None

    def mark(self, key: str):
        self.set(f"seen:{key}", "1")

    def add_ledger(self, **kw):
        values = [kw.get(col) for col in LEDGER_COLS]
        self.con.execute(
            f"INSERT INTO ledger({','.join(LEDGER_COLS)}) VALUES ({','.join('?' * len(LEDGER_COLS))})",
            values,
        )
        self.con.commit()

    def buy(self, token: str, event: str, cid: str, shares: Decimal, cost: Decimal):
        row = self.con.execute(
            "SELECT shares, cost, status FROM pos WHERE token=?", (token,)
        ).fetchone()
        if row is None:
            self.con.execute(
                "INSERT INTO pos(token,event,cid,shares,cost,status) VALUES(?,?,?,?,?,?)",
                (token, event, cid, str(shares), str(cost), "OPEN"),
            )
        else:
            self.con.execute(
                "UPDATE pos SET shares=?, cost=?, status='OPEN' WHERE token=?",
                (str(D(row[0]) + shares), str(D(row[1]) + cost), token),
            )
        self.set("cash", str(self.cash() - cost))
        self.con.commit()

    def open_pos(self):
        return self.con.execute(
            "SELECT token, event, cid, shares, cost FROM pos WHERE status='OPEN'"
        ).fetchall()

    def close(self, token: str, proceeds: Decimal):
        self.con.execute("UPDATE pos SET status='CLOSED' WHERE token=?", (token,))
        self.set("cash", str(self.cash() + proceeds))
        self.con.commit()

    def token_open_shares(self, token: str) -> Decimal:
        row = self.con.execute(
            "SELECT shares, status FROM pos WHERE token=?", (token,)
        ).fetchone()
        if row is None or row[1] != "OPEN":
            return Decimal("0")
        return D(row[0])

    def token_open_cost(self, token: str) -> Decimal:
        row = self.con.execute(
            "SELECT cost, status FROM pos WHERE token=?", (token,)
        ).fetchone()
        if row is None or row[1] != "OPEN":
            return Decimal("0")
        return D(row[0])

    def snapshot(self) -> dict:
        open_rows = self.open_pos()
        counts = {
            name: self.con.execute(
                "SELECT COUNT(*) FROM ledger WHERE kind=?", (name,)
            ).fetchone()[0]
            for name in ("BUY", "SKIP", "SELL", "LOST", "REDEEM")
        }
        pnl = self.con.execute(
            """SELECT COALESCE(SUM(CAST(pnl AS REAL)),0) FROM ledger
               WHERE kind IN ('SELL','LOST','REDEEM') AND pnl IS NOT NULL"""
        ).fetchone()[0]
        return {
            "cash": str(self.cash()),
            "open_n": len(open_rows),
            "open_cost": str(sum((D(row[4]) for row in open_rows), Decimal("0"))),
            "ledger_n": self.con.execute("SELECT COUNT(*) FROM ledger").fetchone()[0],
            "buy_n": counts["BUY"],
            "skip_n": counts["SKIP"],
            "sell_n": counts["SELL"],
            "lost_n": counts["LOST"],
            "redeem_n": counts["REDEEM"],
            "cumulative_pnl": str(round(float(pnl or 0), 4)),
            "record_only": True,
            "real_order_submitted": False,
            "poly_live_trading_armed": False,
        }


def load_config(path: Path) -> dict:
    cfg = dict(DEFAULT_CFG)
    if path.exists():
        cfg.update(json.loads(path.read_text()))
    return cfg


def resolve_cfg_path(runtime: Path) -> Path:
    for path in (runtime / "config.json", runtime.parent / "config.json", HERE / "config.json"):
        if path.exists():
            return path
    return runtime.parent / "config.json"


def load_cities(path: Path) -> dict:
    if not path.exists():
        return {}
    rows = json.loads(path.read_text())
    return {row["slug"]: row for row in rows}


def parse_ensemble_file_ts(path: Path) -> datetime | None:
    matched = FILE_TS.match(path.name)
    if not matched:
        return None
    try:
        day = datetime.strptime(path.parent.name, "%Y-%m-%d").date()
    except ValueError:
        return None
    return datetime(day.year, day.month, day.day, int(matched.group(1)), int(matched.group(2)), tzinfo=timezone.utc)


def find_latest_ensemble_file(root: Path, cutoff_utc: datetime) -> Path | None:
    best = None
    best_ts = None
    if not root.exists():
        return None
    for path in root.glob("*/*.json"):
        ts = parse_ensemble_file_ts(path)
        if ts is None or ts > cutoff_utc:
            continue
        if best_ts is None or ts > best_ts:
            best, best_ts = path, ts
    return best


def index_hourly(records: list[dict]) -> dict:
    out = {}
    for row in records:
        out[(row["city"], row["model"])] = row
    return out


def hourly_members_for(idx: dict, city: str) -> tuple[dict[str, list[HourlyMember]], int]:
    models: dict[str, list[HourlyMember]] = {}
    n_members = 0
    for model in MODELS:
        rec = idx.get((city, model))
        if rec is None:
            continue
        tz = ZoneInfo(str(rec.get("timezone") or "UTC"))
        hours = []
        for raw in rec.get("hours") or []:
            parsed = datetime.fromisoformat(str(raw))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=tz)
            hours.append(parsed)
        hour_t = tuple(hours)
        members: list[HourlyMember] = []
        for series in rec.get("members") or []:
            temps = []
            for index, _hour in enumerate(hour_t):
                if index >= len(series) or series[index] is None:
                    temps.append(None)
                else:
                    temps.append(D(series[index]))
            members.append(HourlyMember(hours=hour_t, temps_c=tuple(temps)))
        if members:
            models[model] = members
            n_members += len(members)
    return models, n_members


def load_clock_watch(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    body = json.loads(path.read_text())
    cities = body.get("cities")
    return cities if isinstance(cities, dict) else {}


def parse_utc(ts: str) -> datetime:
    text = str(ts).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def local_date_of(ts: str, tz_name: str) -> date:
    return parse_utc(ts).astimezone(ZoneInfo(tz_name)).date()


def event_tz(city: str, icao: str, cities: dict, clock: dict[str, dict]) -> str | None:
    row = clock.get(icao) or {}
    if row.get("tz"):
        return str(row["tz"])
    meta = cities.get(city) or {}
    if meta.get("tz"):
        return str(meta["tz"])
    return None


def wrh_site(*texts: str) -> str | None:
    blob = " ".join(t or "" for t in texts)
    matched = re.search(r"timeseries\?site=([A-Za-z0-9]+)", blob, re.I)
    return matched.group(1).upper() if matched else None


def settlement_mode(description: str, src: str = "") -> str | None:
    if not wrh_site(src, description):
        return None
    if "Show Hourly Data" in (description or ""):
        return "hourly_f"
    return "alltimes_c"


def is_speci(metar: str | None) -> bool:
    return str(metar or "").lstrip().upper().startswith("SPECI")


def is_nws_faa_platform(icao: str) -> bool:
    return (icao or "").upper()[:1] in {"K", "P", "T"}


def is_asos_five_min_auto(metar: str | None) -> bool:
    text = str(metar or "").lstrip().upper()
    return text.startswith("METAR ") and " AUTO " in f" {text} "


def hourly_data_row(icao: str, minute: int, metar: str | None, origin=None) -> bool:
    if is_speci(metar):
        return True
    if is_asos_five_min_auto(metar):
        return False
    if is_nws_faa_platform(icao):
        if origin in (1, 1.0, "1", "1.0"):
            return True
        return 51 <= int(minute) <= 59
    return int(minute) >= 56 or int(minute) <= 4


def whole_f_from_c(tmpc: float) -> int:
    return int(round(float(tmpc) * 9.0 / 5.0 + 32.0))


def whole_c_from_c(tmpc: float) -> int:
    return int(round(float(tmpc)))


_SYNOPTIC_TOKEN: dict[str, Any] = {"v": None, "ts": 0.0}


def synoptic_token() -> str:
    now = time.time()
    if _SYNOPTIC_TOKEN["v"] and now - float(_SYNOPTIC_TOKEN["ts"] or 0) < 3600:
        return str(_SYNOPTIC_TOKEN["v"])
    text = http_text(WRH_TOKEN_JS)
    matched = re.search(r"mesoToken='([^']+)'", text)
    if not matched:
        raise RuntimeError("BLOCK_NOAA_SYNOPTIC_TOKEN")
    _SYNOPTIC_TOKEN["v"] = matched.group(1)
    _SYNOPTIC_TOKEN["ts"] = now
    return matched.group(1)


def wrh_event_day_max(
    icao: str,
    event_date: date,
    tz_name: str | None,
    settle: str,
) -> dict[str, Any] | None:
    # Fetch window only. Dead uses event-local-day rows, not a rolling max.
    token = synoptic_token()
    params = {
        "STID": icao,
        "showemptystations": "1",
        "recent": str(48 * 60),
        "complete": "1",
        "token": token,
        "obtimezone": "utc",
    }
    url = f"{SYNOPTIC}?{urllib.parse.urlencode(params)}"
    payload = http_json(
        url,
        timeout=25,
        headers={
            "Referer": WRH_PAGE.format(icao=icao.lower()),
            "Origin": "https://www.weather.gov",
        },
    )
    if payload.get("SUMMARY", {}).get("RESPONSE_CODE") != 1:
        return None
    station = (payload.get("STATION") or [None])[0] or {}
    station_tz = str(station.get("TIMEZONE") or tz_name or "").strip()
    if not station_tz:
        return {"_error": "MISSING_EVENT_TZ"}
    obs = station.get("OBSERVATIONS") or {}
    times = obs.get("date_time") or []
    temps = obs.get("air_temp_set_1") or []
    metars = obs.get("metar_set_1") or []
    origins = obs.get("metar_origin_set_1") or []
    tz = ZoneInfo(station_tz)
    rows = []
    for index, raw_t in enumerate(times):
        if index >= len(temps) or temps[index] is None or temps[index] == "":
            continue
        local = parse_utc(str(raw_t)).astimezone(tz)
        if local.date() != event_date:
            continue
        tmpc = float(temps[index])
        metar = metars[index] if index < len(metars) else None
        origin = origins[index] if index < len(origins) else None
        if settle != "alltimes_c" and not hourly_data_row(icao, local.minute, metar, origin):
            continue
        rows.append(
            {
                "tmpc": tmpc,
                "max_f": whole_f_from_c(tmpc),
                "max_c_whole": whole_c_from_c(tmpc),
                "utc": parse_utc(str(raw_t)),
                "local": local,
            }
        )
    if not rows:
        return None
    peak_key = (
        (lambda row: (row["max_c_whole"], row["tmpc"]))
        if settle == "alltimes_c"
        else (lambda row: (row["max_f"], row["tmpc"]))
    )
    peak = max(rows, key=peak_key)
    latest = max(rows, key=lambda row: row["utc"])
    peak["tz"] = station_tz
    peak["latest_tmpc"] = latest["tmpc"]
    peak["latest_utc"] = latest["utc"]
    peak["latest_local"] = latest["local"]
    return peak


def observed_bundle_for(
    icao: str,
    unit: str,
    cache: dict,
    *,
    event_date: date,
    tz_name: str | None,
    src: str,
    desc: str,
) -> dict[str, Any]:
    mode = settlement_mode(desc, src)
    if mode is None:
        return {"official": "NOT_WRH_SETTLEMENT"}
    key = (icao, event_date.isoformat(), mode)
    if key in cache:
        cached = cache[key]
        return {"official": "WRH_NO_OBS"} if cached is None else dict(cached)
    try:
        peak = wrh_event_day_max(icao, event_date, tz_name, mode)
    except Exception:  # noqa: BLE001
        cache["_fail"] = int(cache.get("_fail") or 0) + 1
        return {"official": "WRH_FETCH_ERROR"}
    if isinstance(peak, dict) and peak.get("_error") == "MISSING_EVENT_TZ":
        return {"official": "MISSING_EVENT_TZ"}
    if peak is None:
        cache[key] = None
        return {"official": "WRH_NO_OBS"}
    unit_u = unit.upper()
    if mode == "hourly_f":
        if unit_u != "F":
            return {"official": "WRH_UNIT_MISMATCH"}
        value = D(peak["max_f"])
        label = "wrh_hourly_f"
    else:
        if unit_u != "C":
            return {"official": "WRH_UNIT_MISMATCH"}
        value = D(peak["max_c_whole"])
        label = "wrh_alltimes_c"
    latest_utc = peak.get("latest_utc")
    bundle = {
        "observed_max": value,
        "observed_max_c": D(peak["tmpc"]),
        "obs_temp_c": D(peak["latest_tmpc"]),
        "obs_time": latest_utc,
        "official": label,
    }
    cache[key] = bundle
    return dict(bundle)


def discover_events(today: date, cache_path: Path | None = None) -> list[str]:
    if cache_path and cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text())
            if (
                cached.get("day") == today.isoformat()
                and cached.get("source") == "gamma_tag_highest-temperature"
                and time.time() - float(cached.get("ts") or 0) < 600
            ):
                return list(cached.get("live") or [])
        except Exception:  # noqa: BLE001
            pass
    live = []
    seen = set()
    offset = 0
    while offset < 2000:
        try:
            page = http_json(
                f"{GAMMA}/events?tag_slug=highest-temperature&closed=false&active=true&limit=50&offset={offset}"
            )
        except Exception:  # noqa: BLE001
            break
        if not isinstance(page, list) or not page:
            break
        for ev in page:
            slug = str(ev.get("slug") or "")
            if not slug.startswith("highest-temperature-in-"):
                continue
            if event_date_of(slug) is None:
                continue
            if slug in seen:
                continue
            seen.add(slug)
            live.append(slug)
        if len(page) < 50:
            break
        offset += 50
    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "day": today.isoformat(),
                    "source": "gamma_tag_highest-temperature",
                    "event_n": len(live),
                    "ts": time.time(),
                    "live": live,
                }
            )
        )
    return live


def fetch_books(tokens: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not tokens:
        return out
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(clob_book, token): token for token in tokens}
        for fut in as_completed(futs):
            token = futs[fut]
            try:
                out[token] = fut.result()
            except Exception:  # noqa: BLE001
                out[token] = {"asks": [], "bids": []}
    return out


def no_resolution(no_px: Decimal, *, closed: bool) -> str | None:
    if no_px <= Decimal("0.01"):
        return "lost"
    if not closed:
        return None
    if no_px >= Decimal("0.99"):
        return "won"
    return None


def clob_book_gone(token: str) -> bool:
    try:
        book = clob_book(token)
    except Exception:  # noqa: BLE001
        return True
    return not book.get("bids")


def gamma_no_won(token: str) -> str | None:
    quoted = urllib.parse.quote(token, safe="")
    try:
        rows = http_json(f"{GAMMA}/markets?clob_token_ids={quoted}&limit=5")
    except Exception:  # noqa: BLE001
        return None
    if not rows:
        return None
    market = rows[0]
    prices = market.get("outcomePrices")
    toks = market.get("clobTokenIds")
    if isinstance(prices, str):
        prices = json.loads(prices)
    if isinstance(toks, str):
        toks = json.loads(toks)
    if not prices or not toks or token not in [str(item) for item in toks]:
        return None
    idx = [str(item) for item in toks].index(token)
    return no_resolution(D(prices[idx]), closed=bool(market.get("closed")))


def write_status(store: Store, path: Path, extra: dict):
    snap = store.snapshot()
    snap.update(extra)
    snap["heartbeat_unix"] = time.time()
    snap["heartbeat_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    snap["record_only"] = True
    snap["real_order_submitted"] = False
    snap["poly_live_trading_armed"] = False
    path.write_text(json.dumps(snap, ensure_ascii=False, indent=2))


def skip_once(store: Store, slug: str, reason: str, **extra) -> bool:
    key = extra.pop("once_key")
    if store.seen(key):
        return False
    store.mark(key)
    store.add_ledger(
        ts=time.time(),
        kind="SKIP",
        event=slug,
        reason=reason,
        skip_reason=reason,
        exit_type="SKIP",
        city=extra.get("city") or city_of(slug),
        date=extra.get("date"),
        bin=extra.get("bin"),
        token=extra.get("token"),
        sleeve=extra.get("sleeve"),
        observed_max=extra.get("observed_max"),
        observed_whole=extra.get("observed_whole"),
        coverage_band=extra.get("coverage_band"),
        in_coverage_band=extra.get("in_coverage_band"),
        p_model=extra.get("p_model"),
        p_adverse=extra.get("p_adverse"),
        p_market_devig=extra.get("p_market_devig"),
        p_blend=extra.get("p_blend"),
        ev=extra.get("ev"),
        obs_temp_c=extra.get("obs_temp_c"),
        obs_time_utc=extra.get("obs_time_utc"),
        tail_unknown=extra.get("tail_unknown"),
        nowcast_deltas=extra.get("nowcast_deltas"),
        min_edge=extra.get("min_edge"),
        ensemble_file_used=extra.get("ensemble_file_used"),
        market_no_ask=extra.get("market_no_ask"),
        official=extra.get("official"),
    )
    return True


def planned_shares(
    cfg: dict,
    px: Decimal,
    available: Decimal,
    *,
    sleeve: str,
) -> Decimal | None:
    size_usd = D(cfg.get("size_per_bin_usd") or "100")
    min_sh = D(cfg.get("min_shares") or MIN_SHARES)
    decimals = int(cfg.get("shares_decimals") or 2)
    if px <= 0:
        return None
    shares = quantize_shares(size_usd / px, decimals)
    if available > 0:
        shares = min(shares, quantize_shares(available, decimals))
    if shares < min_sh:
        return None
    return shares


def record_buy(store: Store, cfg: dict, *, slug: str, bucket, row: dict, extra: dict):
    fee_rate = D(row["fee_rate"])
    book = extra.get("book") or {}
    sleeve = "forecast_nowcast"
    ask_max = D(cfg.get("leftover_ask_max") or "0.99")
    min_sh = D(cfg.get("min_shares") or MIN_SHARES)
    decimals = int(cfg.get("shares_decimals") or 2)
    min_edge = D(cfg.get("min_edge") or "0.04")
    size_usd = D(cfg.get("size_per_bin_usd") or "100")
    p_yes = D(row["p_adverse"]) if row.get("p_adverse") not in (None, "") else None
    if p_yes is None:
        skip_once(
            store,
            slug,
            "MISSING_P_ADVERSE",
            once_key=f"{extra['date']}:{bucket.no_token_id}:MISSING_P_ADVERSE",
            city=extra["city"],
            date=extra["date"],
            bin=bucket.title,
            token=bucket.no_token_id,
            sleeve=sleeve,
            observed_max=extra.get("observed_max"),
            official=extra.get("official"),
        )
        return False
    remain = size_usd - store.token_open_cost(bucket.no_token_id)
    remain = min(remain, store.cash())
    clips = walk_ask_clips(
        book,
        budget=remain,
        fee_rate=fee_rate,
        ask_max=ask_max,
        min_sh=min_sh,
        decimals=decimals,
        p_yes=p_yes,
        min_ev=min_edge,
        dead=False,
    )
    if not clips:
        levels = ask_levels(book)
        best = levels[0][0] if levels else None
        reason = "LEFTOVER_NO_EDGE" if best is not None and best >= ask_max else "INSUFFICIENT_SIZE"
        if reason == "LEFTOVER_NO_EDGE":
            skip_once(
                store,
                slug,
                reason,
                once_key=f"{extra['date']}:{bucket.no_token_id}:{reason}",
                city=extra["city"],
                date=extra["date"],
                bin=bucket.title,
                token=bucket.no_token_id,
                sleeve=sleeve,
                observed_max=extra.get("observed_max"),
                market_no_ask=None if best is None else str(best),
                official=extra.get("official"),
            )
        return False
    shares = sum((c["shares"] for c in clips), Decimal("0"))
    cost = sum((c["cost"] for c in clips), Decimal("0"))
    fee = sum((c["fee"] for c in clips), Decimal("0"))
    vwap = sum((c["px"] * c["shares"] for c in clips), Decimal("0")) / shares
    if store.cash() < cost:
        return False
    store.buy(bucket.no_token_id, slug, extra.get("cid") or "", shares, cost)
    asks_out = [{"price": str(px), "size": str(sz)} for px, sz in ask_levels(book)[:24]]
    store.add_ledger(
        ts=time.time(),
        kind="BUY",
        event=slug,
        cid=extra.get("cid"),
        token=bucket.no_token_id,
        shares=str(shares),
        px=str(vwap),
        usdc=str(cost),
        fee=str(fee),
        reason=sleeve,
        official=extra.get("official"),
        book=json.dumps({"asks": asks_out, "clips": [
            {"px": str(c["px"]), "shares": str(c["shares"]), "cost": str(c["cost"])}
            for c in clips
        ]}),
        city=extra["city"],
        date=extra["date"],
        bin=bucket.title,
        unit=bucket.unit,
        sleeve=sleeve,
        observed_max=extra.get("observed_max"),
        observed_whole=extra.get("observed_whole"),
        coverage_band=extra.get("coverage_band"),
        in_coverage_band=str(row.get("in_coverage_band")),
        p_model=row.get("p_model"),
        p_adverse=row.get("p_adverse"),
        p_market_devig=row.get("p_market_devig"),
        p_blend=row.get("p_blend"),
        ev=str(clips[-1]["ev"]),
        obs_temp_c=extra.get("obs_temp_c"),
        obs_time_utc=extra.get("obs_time_utc"),
        tail_unknown=str(row.get("tail_unknown")),
        nowcast_deltas=extra.get("nowcast_deltas"),
        min_edge=extra.get("min_edge"),
        ensemble_file_used=extra.get("ensemble_file_used"),
        n_members=extra.get("n_members"),
        market_no_ask=str(clips[0]["px"]),
        market_yes_ask=None if bucket.yes_ask is None else str(bucket.yes_ask),
        exit_type="OPEN",
    )
    return True


def try_sell(store: Store):
    for token, event, cid, shares, cost in store.open_pos():
        try:
            book = clob_book(token)
        except Exception:  # noqa: BLE001
            continue
        bid = best_bid(book)
        if bid is None or bid < SELL:
            continue
        sh = D(shares)
        proceeds = rest_099_proceeds(sh)
        store.close(token, proceeds)
        store.add_ledger(
            ts=time.time(),
            kind="SELL",
            event=event,
            cid=cid,
            token=token,
            shares=str(sh),
            px=str(SELL),
            usdc=str(proceeds),
            fee="0",
            reason="rest_0.99_bid",
            official="clob_bid",
            book=json.dumps({"bids": (book.get("bids") or [])[:6]}),
            exit_type="SELL",
            exit_price=str(SELL),
            pnl=str(proceeds - D(cost)),
        )


def try_settle(store: Store):
    today = datetime.now(timezone.utc).date()
    for token, event, cid, shares, cost in store.open_pos():
        res = gamma_no_won(token)
        if res is None and event_date_of(event) is not None and event_date_of(event) < today and clob_book_gone(token):
            res = "lost"
        if res is None:
            continue
        sh = D(shares)
        if res == "won":
            fee = sh * taker_fee(rate=FEE, price=Decimal("1"))
            proceeds = sh * Decimal("1") - fee
            kind = "REDEEM"
            px = "1"
        else:
            proceeds = Decimal("0")
            fee = Decimal("0")
            kind = "LOST"
            px = "0"
        store.close(token, proceeds)
        store.add_ledger(
            ts=time.time(),
            kind=kind,
            event=event,
            cid=cid,
            token=token,
            shares=str(sh),
            px=px,
            usdc=str(proceeds),
            fee=str(fee),
            reason=f"gamma_{res}",
            official="gamma_outcomePrices",
            exit_type=kind,
            exit_price=px,
            pnl=str(proceeds - D(cost)),
        )


def process_event(
    store: Store,
    slug: str,
    cfg: dict,
    cities: dict,
    clock: dict[str, dict],
    ens_idx: dict | None,
    ens_rel: str | None,
    wrh_cache: dict,
    now: datetime,
) -> dict:
    stats = {"candidate": 0, "qualify": 0, "buy": 0, "skip": 0, "dead": 0}
    city = city_of(slug)
    ed = event_date_of(slug)
    if ed is None:
        return stats
    today = now.date()
    if ed < today - timedelta(days=1) or ed > today + timedelta(days=1):
        return stats
    stats["candidate"] = 1
    try:
        raw = http_json(f"{GAMMA}/events?slug={slug}")
    except Exception as exc:  # noqa: BLE001
        skip_once(
            store,
            slug,
            f"gamma:{type(exc).__name__}",
            once_key=f"{ed.isoformat()}:{slug}:gamma",
            city=city,
            date=ed.isoformat(),
        )
        stats["skip"] = 1
        return stats
    if not raw:
        return stats
    ev = raw[0] if isinstance(raw, list) else raw
    if ev.get("closed"):
        return stats
    try:
        icao = parse_station_icao(str(ev.get("resolutionSource") or ""))
        buckets = buckets_from_gamma_event(ev)
    except (WeatherModelError, Exception) as exc:  # noqa: BLE001
        skip_once(
            store,
            slug,
            f"event:{exc}",
            once_key=f"{ed.isoformat()}:{slug}:event",
            city=city,
            date=ed.isoformat(),
        )
        stats["skip"] = 1
        return stats
    unit = buckets[0].unit
    tz_name = event_tz(city, icao, cities, clock)
    src = str(ev.get("resolutionSource") or "")
    desc = str(ev.get("description") or "")
    obs = observed_bundle_for(
        icao,
        unit,
        wrh_cache,
        event_date=ed,
        tz_name=tz_name,
        src=src,
        desc=desc,
    )
    official = str(obs.get("official") or "")
    observed = obs.get("observed_max")
    obs_time = obs.get("obs_time")
    if observed is None or obs_time is None:
        durable = official in {
            "NOT_WRH_SETTLEMENT",
            "MISSING_EVENT_TZ",
            "WRH_UNIT_MISMATCH",
        }
        if durable:
            skip_once(
                store,
                slug,
                official,
                once_key=f"{ed.isoformat()}:{slug}:{official}",
                city=city,
                date=ed.isoformat(),
                official=official,
            )
        stats["skip"] = 1
        return stats
    tokens = []
    for bucket in buckets:
        tokens.append(bucket.yes_token_id)
        tokens.append(bucket.no_token_id)
    books = fetch_books(tokens)
    yes_asks = {bucket.yes_token_id: _best_ask(books.get(bucket.yes_token_id) or {}) for bucket in buckets}
    no_asks = {bucket.no_token_id: _best_ask(books.get(bucket.no_token_id) or {}) for bucket in buckets}
    priced = attach_asks(buckets, yes_asks=yes_asks, no_asks=no_asks)
    model_members: dict[str, list[HourlyMember]] = {}
    n_members = 0
    if ens_idx is not None:
        model_members, n_members = hourly_members_for(ens_idx, city)
    extra_base = {
        "city": city,
        "date": ed.isoformat(),
        "cid": str(ev.get("id") or ""),
        "observed_max": str(observed),
        "observed_whole": str(whole_degree(observed)),
        "official": official,
        "ensemble_file_used": ens_rel,
        "n_members": str(n_members),
        "min_edge": str(cfg.get("min_edge") or "0.04"),
        "obs_temp_c": str(obs.get("obs_temp_c")),
        "obs_time_utc": obs_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sleeve": "forecast_nowcast",
    }
    store.set(f"obs:{slug}", str(observed))
    if not model_members:
        skip_once(
            store,
            slug,
            "MISSING_HOURLY_ENSEMBLE",
            once_key=f"{ed.isoformat()}:{slug}:MISSING_HOURLY_ENSEMBLE",
            city=city,
            date=ed.isoformat(),
            observed_max=str(observed),
            official=official,
        )
        stats["skip"] = 1
        return stats
    try:
        receipt = qualify_nowcast_no_books(
            priced,
            observed_max=observed,
            observed_max_c=D(obs["observed_max_c"]),
            obs_temp_c=D(obs["obs_temp_c"]),
            obs_time=obs_time,
            event_date=ed,
            unit=unit,
            model_members=model_members,
            min_edge=D(cfg.get("min_edge") or "0.04"),
            model_weight=D(cfg.get("model_weight") or "0.5"),
            coverage_alpha=D(cfg.get("coverage_alpha") or "0.85"),
        )
        rows = receipt["buckets"]
        extra_base["coverage_band"] = ",".join(receipt.get("coverage_band") or [])
        extra_base["nowcast_deltas"] = json.dumps(receipt.get("nowcast_deltas_c") or {})
    except WeatherModelError as exc:
        skip_once(
            store,
            slug,
            f"qualify:{exc}",
            once_key=f"{ed.isoformat()}:{slug}:qualify:{exc}",
            city=city,
            date=ed.isoformat(),
            observed_max=str(observed),
            official=official,
        )
        stats["skip"] = 1
        return stats
    by_title = {row["title"]: row for row in rows}
    size_usd = D(cfg.get("size_per_bin_usd") or "100")
    for bucket in priced:
        row = by_title[bucket.title]
        if row.get("status") == "dead":
            stats["dead"] += 1
        if store.token_open_cost(bucket.no_token_id) >= size_usd:
            continue
        extra = dict(extra_base)
        extra["book"] = books.get(bucket.no_token_id) or {}
        extra["in_coverage_band"] = str(row.get("in_coverage_band"))
        extra["p_model"] = row.get("p_model")
        extra["p_adverse"] = row.get("p_adverse")
        extra["p_market_devig"] = row.get("p_market_devig")
        extra["p_blend"] = row.get("p_blend")
        extra["ev"] = row.get("ev")
        extra["tail_unknown"] = str(row.get("tail_unknown"))
        extra["bin"] = bucket.title
        extra["token"] = bucket.no_token_id
        if row.get("qualify"):
            stats["qualify"] += 1
            if record_buy(store, cfg, slug=slug, bucket=bucket, row=row, extra=extra):
                stats["buy"] += 1
            continue
        reason = row.get("skip_reason") or "NO_EDGE_BELOW_MIN"
        if skip_once(
            store,
            slug,
            reason,
            once_key=f"{ed.isoformat()}:{bucket.no_token_id}:{reason}",
            **{k: v for k, v in extra.items() if k != "book"},
        ):
            stats["skip"] += 1
    return stats


def one_loop(store: Store, runtime: Path, cfg: dict) -> dict:
    extra = {
        "shadow_name": cfg.get("shadow_name") or "wx-s4-shadow",
        "record_only": True,
        "real_order_submitted": False,
        "poly_live_trading_armed": False,
        "min_edge": str(cfg.get("min_edge") or "0.04"),
        "coverage_alpha": str(cfg.get("coverage_alpha") or "0.85"),
        "loop": "ok",
    }
    now = datetime.now(timezone.utc)
    cities = load_cities(Path(cfg["ensemble_city_config"]))
    clock = load_clock_watch(Path(cfg["clock_watch_path"]))
    ens_path = find_latest_ensemble_file(Path(cfg["ensemble_data_root"]), now)
    extra["ensemble_last_file_seen"] = None if ens_path is None else f"{ens_path.parent.name}/{ens_path.name}"
    ens_idx = None
    if ens_path is not None:
        try:
            ens_idx = index_hourly(json.loads(ens_path.read_text()))
        except Exception as exc:  # noqa: BLE001
            extra["ensemble_read"] = type(exc).__name__
    wrh_cache: dict = {}
    candidate = qualify = buy = skip = dead = 0
    try:
        try_settle(store)
        try_sell(store)
        slugs = discover_events(now.date(), runtime / "events_cache.json")
        extra["event_n"] = len(slugs)
        for slug in slugs:
            st = process_event(
                store,
                slug,
                cfg,
                cities,
                clock,
                ens_idx,
                extra["ensemble_last_file_seen"],
                wrh_cache,
                now,
            )
            candidate += st["candidate"]
            qualify += st["qualify"]
            buy += st["buy"]
            skip += st["skip"]
            dead += st["dead"]
        try_sell(store)
    except Exception as exc:  # noqa: BLE001
        extra["loop"] = f"error:{type(exc).__name__}"
    extra["candidate_city_day_n"] = candidate
    extra["qualify_bin_n"] = qualify
    extra["buy_bin_n"] = buy
    extra["skip_bin_n"] = skip
    extra["dead_bin_seen_n"] = dead
    extra["in_play_n"] = len(store.open_pos())
    extra["clock_icao_n"] = len(clock)
    extra["wrh_ok_n"] = sum(
        1 for key, value in wrh_cache.items() if key != "_fail" and value is not None
    )
    extra["wrh_no_obs_n"] = sum(
        1 for key, value in wrh_cache.items() if key != "_fail" and value is None
    )
    extra["wrh_fail_n"] = int(wrh_cache.get("_fail") or 0)
    return extra


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-dir", required=True)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--config", default="")
    args = parser.parse_args()
    runtime = Path(args.runtime_dir)
    runtime.mkdir(parents=True, exist_ok=True)
    cfg_path = Path(args.config) if args.config else resolve_cfg_path(runtime)
    cfg = load_config(cfg_path)
    aum = D(cfg.get("aum_usd") or "5000")
    store = Store(runtime / "shadow.sqlite", aum)
    if store.get("cash") is None:
        store.set("cash", str(aum))
    store.set("real_order_submitted", "false")
    store.set("record_only", "true")
    store.set("poly_live_trading_armed", "false")
    extra = one_loop(store, runtime, cfg)
    extra["once"] = bool(args.once)
    write_status(store, runtime / "status.json", extra)
    if args.once:
        print((runtime / "status.json").read_text())
        return
    tick = int(cfg.get("tick_s") or 20)
    while True:
        time.sleep(tick)
        cfg = load_config(cfg_path)
        extra = one_loop(store, runtime, cfg)
        write_status(store, runtime / "status.json", extra)


if __name__ == "__main__":
    main()
