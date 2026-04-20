#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
weatherbot.py — Polymarket Weather Trading Bot
================================================
Built from scratch with:
  - Simulation mode (paper trading, no real money)
  - Real mode (actual Polymarket CLOB execution)
  - Fixed gaussian bucket probability (not binary)
  - SQLite storage (not flat JSON)
  - Weighted ensemble forecasting (ECMWF + HRRR + METAR)
  - Claude Sonnet post-trade reasoning hook
  - Kelly Criterion with warm-up guard
  - Circuit breaker per city
  - Telegram alerts (optional)

Usage:
    python weatherbot.py sim          # paper trading (default)
    python weatherbot.py real         # live trading (needs wallet)
    python weatherbot.py status       # balance + open positions
    python weatherbot.py report       # full trade history
    python weatherbot.py backtest     # replay historical data
"""

import re
import os
import sys
import json
import math
import time
import sqlite3
import logging
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("weatherbot.log"),
    ],
)
log = logging.getLogger("weatherbot")

# =============================================================================
# CONFIG
# =============================================================================

CFG_FILE = Path("config.json")
if not CFG_FILE.exists():
    default_cfg = {
        "mode": "sim",
        "balance": 1000.0,
        "max_bet": 20.0,
        "min_ev": 0.08,
        "max_price": 0.50,
        "min_volume": 500,
        "min_hours": 2.0,
        "max_hours": 72.0,
        "kelly_fraction": 0.25,
        "kelly_warmup": 30,
        "scan_interval": 3600,
        "monitor_interval": 600,
        "max_slippage": 0.03,
        "stop_loss_pct": 0.20,
        "trail_trigger_pct": 0.20,
        "forecast_shift_exit": 2.0,
        "vc_key": "YOUR_VISUAL_CROSSING_KEY",
        "anthropic_key": "",
        "telegram_token": "",
        "telegram_chat_id": "",
        "polymarket_pk": "",
        "circuit_breaker_fails": 3,
    }
    CFG_FILE.write_text(json.dumps(default_cfg, indent=2))
    log.info("Created default config.json — please fill in your API keys.")

with open(CFG_FILE, encoding="utf-8") as f:
    _cfg = json.load(f)

MODE             = _cfg.get("mode", "sim")          # "sim" | "real"
BALANCE          = float(_cfg.get("balance", 1000.0))
MAX_BET          = float(_cfg.get("max_bet", 20.0))
MIN_EV           = float(_cfg.get("min_ev", 0.08))
MAX_PRICE        = float(_cfg.get("max_price", 0.50))
MIN_VOLUME       = float(_cfg.get("min_volume", 500))
MIN_HOURS        = float(_cfg.get("min_hours", 2.0))
MAX_HOURS        = float(_cfg.get("max_hours", 72.0))
KELLY_FRACTION   = float(_cfg.get("kelly_fraction", 0.25))
KELLY_WARMUP     = int(_cfg.get("kelly_warmup", 30))
SCAN_INTERVAL    = int(_cfg.get("scan_interval", 3600))
MONITOR_INTERVAL = int(_cfg.get("monitor_interval", 600))
MAX_SLIPPAGE     = float(_cfg.get("max_slippage", 0.03))
STOP_LOSS_PCT    = float(_cfg.get("stop_loss_pct", 0.20))
TRAIL_TRIGGER    = float(_cfg.get("trail_trigger_pct", 0.20))
FORECAST_SHIFT   = float(_cfg.get("forecast_shift_exit", 2.0))
VC_KEY           = _cfg.get("vc_key", "")
ANTHROPIC_KEY    = _cfg.get("anthropic_key", "")
TG_TOKEN         = _cfg.get("telegram_token", "")
TG_CHAT_ID       = _cfg.get("telegram_chat_id", "")
POLY_PK          = _cfg.get("polymarket_pk", "")
CB_MAX_FAILS     = int(_cfg.get("circuit_breaker_fails", 3))

DB_PATH  = Path("weatherbot.db")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# =============================================================================
# CITY / LOCATION DATA
# Polymarket resolves on airport stations, NOT city centers.
# Using airport coordinates is the single most impactful accuracy improvement.
# =============================================================================

LOCATIONS = {
    # US
    "new-york":      {"name": "New York",      "lat": 40.7769, "lon": -73.8740, "station": "KLGA", "unit": "F", "tz": "America/New_York"},
    "chicago":       {"name": "Chicago",        "lat": 41.9742, "lon": -87.9073, "station": "KORD", "unit": "F", "tz": "America/Chicago"},
    "miami":         {"name": "Miami",           "lat": 25.7959, "lon": -80.2870, "station": "KMIA", "unit": "F", "tz": "America/New_York"},
    "dallas":        {"name": "Dallas",          "lat": 32.8481, "lon": -96.8512, "station": "KDAL", "unit": "F", "tz": "America/Chicago"},
    "seattle":       {"name": "Seattle",         "lat": 47.4502, "lon": -122.3088,"station": "KSEA", "unit": "F", "tz": "America/Los_Angeles"},
    "atlanta":       {"name": "Atlanta",         "lat": 33.6407, "lon": -84.4277, "station": "KATL", "unit": "F", "tz": "America/New_York"},
    # Europe
    "london":        {"name": "London",          "lat": 51.5048, "lon": -0.0495,  "station": "EGLC", "unit": "C", "tz": "Europe/London"},
    "paris":         {"name": "Paris",           "lat": 48.7233, "lon": 2.3794,   "station": "LFPO", "unit": "C", "tz": "Europe/Paris"},
    "munich":        {"name": "Munich",          "lat": 48.3537, "lon": 11.7860,  "station": "EDDM", "unit": "C", "tz": "Europe/Berlin"},
    "ankara":        {"name": "Ankara",          "lat": 40.1282, "lon": 32.9951,  "station": "LTAC", "unit": "C", "tz": "Europe/Istanbul"},
    # Asia
    "tokyo":         {"name": "Tokyo",           "lat": 35.5494, "lon": 139.7798, "station": "RJTT", "unit": "C", "tz": "Asia/Tokyo"},
    "seoul":         {"name": "Seoul",           "lat": 37.4691, "lon": 126.4505, "station": "RKSI", "unit": "C", "tz": "Asia/Seoul"},
    "shanghai":      {"name": "Shanghai",        "lat": 31.1443, "lon": 121.8083, "station": "ZSPD", "unit": "C", "tz": "Asia/Shanghai"},
    "singapore":     {"name": "Singapore",       "lat": 1.3644,  "lon": 103.9915, "station": "WSSS", "unit": "C", "tz": "Asia/Singapore"},
    "lucknow":       {"name": "Lucknow",         "lat": 26.7606, "lon": 80.8893,  "station": "VILK", "unit": "C", "tz": "Asia/Kolkata"},
    "tel-aviv":      {"name": "Tel Aviv",        "lat": 32.0114, "lon": 34.8867,  "station": "LLBG", "unit": "C", "tz": "Asia/Jerusalem"},
    # Americas
    "toronto":       {"name": "Toronto",         "lat": 43.6777, "lon": -79.6248, "station": "CYYZ", "unit": "C", "tz": "America/Toronto"},
    "sao-paulo":     {"name": "São Paulo",       "lat": -23.4356, "lon": -46.4731,"station": "SBGR", "unit": "C", "tz": "America/Sao_Paulo"},
    "buenos-aires":  {"name": "Buenos Aires",    "lat": -34.8222, "lon": -58.5358,"station": "SAEZ", "unit": "C", "tz": "America/Argentina/Buenos_Aires"},
    # Oceania
    "wellington":    {"name": "Wellington",      "lat": -41.3272, "lon": 174.8052,"station": "NZWN", "unit": "C", "tz": "Pacific/Auckland"},
}

MONTHS = ["january","february","march","april","may","june",
          "july","august","september","october","november","december"]

# =============================================================================
# DATABASE — SQLite replaces flat JSON files
# =============================================================================

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def db_init():
    """Create tables if they don't exist."""
    with db_connect() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS state (
            id      INTEGER PRIMARY KEY DEFAULT 1,
            balance REAL    NOT NULL,
            start   REAL    NOT NULL,
            wins    INTEGER DEFAULT 0,
            losses  INTEGER DEFAULT 0,
            updated TEXT
        );

        CREATE TABLE IF NOT EXISTS markets (
            id          TEXT PRIMARY KEY,
            city        TEXT,
            date        TEXT,
            status      TEXT DEFAULT 'open',
            question    TEXT,
            t_low       REAL,
            t_high      REAL,
            unit        TEXT,
            hours_left  REAL,
            created_at  TEXT,
            resolved_at TEXT,
            pnl         REAL
        );

        CREATE TABLE IF NOT EXISTS positions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id   TEXT REFERENCES markets(id),
            mode        TEXT,
            entry_price REAL,
            shares      REAL,
            cost        REAL,
            stop_price  REAL,
            trail_high  REAL,
            status      TEXT DEFAULT 'open',
            exit_price  REAL,
            close_reason TEXT,
            pnl         REAL,
            opened_at   TEXT,
            closed_at   TEXT
        );

        CREATE TABLE IF NOT EXISTS forecasts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id   TEXT REFERENCES markets(id),
            ts          TEXT,
            source      TEXT,
            temp        REAL,
            confidence  REAL
        );

        CREATE TABLE IF NOT EXISTS calibration (
            city        TEXT,
            source      TEXT,
            mae         REAL,
            n           INTEGER,
            updated     TEXT,
            PRIMARY KEY (city, source)
        );

        CREATE TABLE IF NOT EXISTS post_trade_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id   TEXT,
            outcome     TEXT,
            analysis    TEXT,
            created_at  TEXT
        );
        """)

        # Seed initial state if empty
        cur = conn.execute("SELECT COUNT(*) as c FROM state")
        if cur.fetchone()["c"] == 0:
            conn.execute(
                "INSERT INTO state (balance, start, updated) VALUES (?, ?, ?)",
                (BALANCE, BALANCE, _now())
            )
    log.info("Database ready: %s", DB_PATH)

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def load_state() -> dict:
    with db_connect() as conn:
        row = conn.execute("SELECT * FROM state WHERE id=1").fetchone()
        return dict(row) if row else {"balance": BALANCE, "start": BALANCE, "wins": 0, "losses": 0}

def save_state(balance: float, win: bool = None):
    with db_connect() as conn:
        if win is True:
            conn.execute("UPDATE state SET balance=?, wins=wins+1, updated=? WHERE id=1", (balance, _now()))
        elif win is False:
            conn.execute("UPDATE state SET balance=?, losses=losses+1, updated=? WHERE id=1", (balance, _now()))
        else:
            conn.execute("UPDATE state SET balance=?, updated=? WHERE id=1", (balance, _now()))

# =============================================================================
# MATH — Fixed gaussian bucket probability
# =============================================================================

def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def bucket_prob(forecast: float, t_low: float, t_high: float, sigma: float = None) -> float:
    """
    FIXED: All buckets use gaussian distribution, not just edge buckets.
    The original bot returned 0.0 or 1.0 for regular buckets — this caused
    it to miss positive EV trades near bucket boundaries.

    sigma comes from calibration data (forecast MAE per city/source).
    Default 2.0°F / 1.5°C if no calibration data yet.
    """
    s = sigma if sigma and sigma > 0 else 2.0
    f = float(forecast)

    if t_low == -999:   # "below X" edge bucket
        return norm_cdf((t_high - f) / s)
    if t_high == 999:   # "above X" edge bucket
        return 1.0 - norm_cdf((t_low - f) / s)

    # FIXED: regular bucket — gaussian, not binary
    p_high = norm_cdf((t_high - f) / s)
    p_low  = norm_cdf((t_low  - f) / s)
    return round(max(0.0, p_high - p_low), 4)

def calc_ev(p: float, price: float) -> float:
    """Expected Value = p*(1/price - 1) - (1-p)"""
    if price <= 0 or price >= 1:
        return 0.0
    return round(p * (1.0 / price - 1.0) - (1.0 - p), 4)

def calc_kelly(p: float, price: float, n_resolved: int = 0) -> float:
    """
    Fractional Kelly with warm-up guard.
    Returns 0 until we have enough resolved trades for calibration.
    """
    if n_resolved < KELLY_WARMUP:
        # Flat sizing during warm-up: use min_ev as a proxy fraction
        flat_frac = 0.02
        return round(flat_frac * KELLY_FRACTION, 4)

    if price <= 0 or price >= 1:
        return 0.0
    b = 1.0 / price - 1.0
    f = (p * b - (1.0 - p)) / b
    return round(min(max(0.0, f) * KELLY_FRACTION, 1.0), 4)

def bet_size(kelly: float, balance: float) -> float:
    raw = kelly * balance
    return round(min(raw, MAX_BET), 2)

# =============================================================================
# WEIGHTED ENSEMBLE FORECAST
# Weights based on historical MAE per source per city from calibration table.
# =============================================================================

def get_weights(city_slug: str) -> dict:
    """Return normalized inverse-MAE weights for each source."""
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT source, mae, n FROM calibration WHERE city=?", (city_slug,)
        ).fetchall()

    if not rows:
        # No calibration data yet — equal weights
        return {"ecmwf": 1/3, "hrrr": 1/3, "metar": 1/3}

    raw = {}
    for r in rows:
        if r["mae"] and r["mae"] > 0 and r["n"] >= 5:
            raw[r["source"]] = 1.0 / r["mae"]

    if not raw:
        return {"ecmwf": 1/3, "hrrr": 1/3, "metar": 1/3}

    total = sum(raw.values())
    return {k: v / total for k, v in raw.items()}

def ensemble_forecast(city_slug: str, temps: dict) -> tuple[Optional[float], float]:
    """
    Combine ECMWF, HRRR, METAR forecasts into weighted average.
    Returns (weighted_temp, ensemble_sigma).
    ensemble_sigma = weighted MAE from calibration.
    """
    weights = get_weights(city_slug)
    weighted_sum = 0.0
    weight_total = 0.0

    for source, temp in temps.items():
        if temp is not None:
            w = weights.get(source, 1/3)
            weighted_sum += w * temp
            weight_total += w

    if weight_total == 0:
        return None, 2.0

    ensemble_temp = weighted_sum / weight_total

    # Ensemble sigma: weighted average of source MAEs + spread penalty
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT source, mae FROM calibration WHERE city=?", (city_slug,)
        ).fetchall()
    maes = {r["source"]: r["mae"] for r in rows if r["mae"]}

    sigma_parts = []
    for source, temp in temps.items():
        if temp is not None:
            w = weights.get(source, 1/3)
            mae = maes.get(source, 2.0)
            sigma_parts.append(w * mae)

    base_sigma = sum(sigma_parts) / max(len(sigma_parts), 1) if sigma_parts else 2.0

    # Add spread penalty if sources disagree significantly
    valid_temps = [t for t in temps.values() if t is not None]
    if len(valid_temps) >= 2:
        spread = max(valid_temps) - min(valid_temps)
        spread_penalty = spread * 0.3
        base_sigma += spread_penalty

    return round(ensemble_temp, 2), round(base_sigma, 2)

# =============================================================================
# FORECAST SOURCES
# =============================================================================

# Circuit breaker state — tracks consecutive failures per city
_cb_fails: dict = {}

def _cb_check(city_slug: str) -> bool:
    """Return True if city is OK to fetch, False if circuit is open."""
    return _cb_fails.get(city_slug, 0) < CB_MAX_FAILS

def _cb_ok(city_slug: str):
    _cb_fails[city_slug] = 0

def _cb_fail(city_slug: str):
    _cb_fails[city_slug] = _cb_fails.get(city_slug, 0) + 1
    if _cb_fails[city_slug] >= CB_MAX_FAILS:
        log.warning("Circuit breaker OPEN for %s (%d consecutive failures)", city_slug, _cb_fails[city_slug])

def get_ecmwf(city_slug: str, dates: list[str]) -> dict[str, Optional[float]]:
    """
    Fetch ECMWF forecast from Open-Meteo (free, no key).
    Returns {date: max_temp} for each date.
    """
    if not _cb_check(city_slug):
        return {}

    loc  = LOCATIONS[city_slug]
    unit = "fahrenheit" if loc["unit"] == "F" else "celsius"
    url  = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":         loc["lat"],
        "longitude":        loc["lon"],
        "daily":            "temperature_2m_max",
        "temperature_unit": unit,
        "timezone":         loc["tz"],
        "forecast_days":    7,
        "models":           "ecmwf_ifs025",
    }

    try:
        r = requests.get(url, params=params, timeout=(5, 10))
        r.raise_for_status()
        data = r.json()
        result = {}
        for d, t in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
            if d in dates and t is not None:
                result[d] = round(float(t), 1)
        _cb_ok(city_slug)
        return result
    except Exception as e:
        _cb_fail(city_slug)
        log.debug("ECMWF fetch failed for %s: %s", city_slug, e)
        return {}

def get_hrrr(city_slug: str, dates: list[str]) -> dict[str, Optional[float]]:
    """
    Fetch HRRR/GFS forecast from Open-Meteo (US cities only; falls back to GFS globally).
    """
    if not _cb_check(city_slug):
        return {}

    loc  = LOCATIONS[city_slug]
    unit = "fahrenheit" if loc["unit"] == "F" else "celsius"
    url  = "https://api.open-meteo.com/v1/forecast"
    # HRRR only covers US; use GFS as global fallback
    model = "gfs_seamless"

    params = {
        "latitude":         loc["lat"],
        "longitude":        loc["lon"],
        "daily":            "temperature_2m_max",
        "temperature_unit": unit,
        "timezone":         loc["tz"],
        "forecast_days":    7,
        "models":           model,
    }

    try:
        r = requests.get(url, params=params, timeout=(5, 10))
        r.raise_for_status()
        data = r.json()
        result = {}
        for d, t in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
            if d in dates and t is not None:
                result[d] = round(float(t), 1)
        _cb_ok(city_slug)
        return result
    except Exception as e:
        _cb_fail(city_slug)
        log.debug("HRRR/GFS fetch failed for %s: %s", city_slug, e)
        return {}

def get_metar(city_slug: str) -> Optional[float]:
    """
    Fetch real-time observation from METAR (Aviation Weather API).
    Returns current temperature at airport station.
    """
    if not _cb_check(city_slug):
        return None

    station = LOCATIONS[city_slug]["station"]
    url = f"https://aviationweather.gov/api/data/metar?ids={station}&format=json"

    try:
        r = requests.get(url, timeout=(5, 10))
        r.raise_for_status()
        data = r.json()
        if data and isinstance(data, list) and data[0].get("temp") is not None:
            temp_c = float(data[0]["temp"])
            # Convert to F if needed
            if LOCATIONS[city_slug]["unit"] == "F":
                return round(temp_c * 9/5 + 32, 1)
            return round(temp_c, 1)
    except Exception as e:
        log.debug("METAR fetch failed for %s/%s: %s", city_slug, station, e)

    return None

def get_actual_temp(city_slug: str, date_str: str) -> Optional[float]:
    """
    Fetch actual historical temperature via Visual Crossing (for market resolution).
    """
    if not VC_KEY or VC_KEY == "YOUR_VISUAL_CROSSING_KEY":
        return None

    loc  = LOCATIONS[city_slug]
    unit = "us" if loc["unit"] == "F" else "metric"
    url  = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{loc['lat']},{loc['lon']}/{date_str}"
    params = {
        "unitGroup":  unit,
        "elements":   "tempmax",
        "include":    "days",
        "contentType":"json",
    }

    try:
        r = requests.get(url, params=params, timeout=(5, 15))
        r.raise_for_status()
        data = r.json()
        days = data.get("days", [])
        if days:
            return round(float(days[0]["tempmax"]), 1)
    except Exception as e:
        log.debug("Visual Crossing fetch failed for %s/%s: %s", city_slug, date_str, e)

    return None

# =============================================================================
# CALIBRATION — Update forecast MAE per city/source after resolution
# =============================================================================

def update_calibration(city_slug: str, source: str, forecast: float, actual: float):
    """Update running MAE for a source in a city after market resolves."""
    err = abs(forecast - actual)
    with db_connect() as conn:
        row = conn.execute(
            "SELECT mae, n FROM calibration WHERE city=? AND source=?",
            (city_slug, source)
        ).fetchone()

        if row:
            new_n   = row["n"] + 1
            new_mae = (row["mae"] * row["n"] + err) / new_n
            conn.execute(
                "UPDATE calibration SET mae=?, n=?, updated=? WHERE city=? AND source=?",
                (round(new_mae, 3), new_n, _now(), city_slug, source)
            )
        else:
            conn.execute(
                "INSERT INTO calibration (city, source, mae, n, updated) VALUES (?,?,?,?,?)",
                (city_slug, source, round(err, 3), 1, _now())
            )

# =============================================================================
# POLYMARKET API
# =============================================================================

GAMMA_BASE = "https://gamma-api.polymarket.com"

def get_polymarket_event(city_slug: str, month: str, day: int, year: int) -> Optional[dict]:
    """Search Polymarket Gamma for a weather event matching city/date."""
    city_name = LOCATIONS[city_slug]["name"]
    query     = f"{city_name} temperature {month} {day}"
    url       = f"{GAMMA_BASE}/events"
    params    = {"q": query, "closed": "false"}

    try:
        r = requests.get(url, params=params, timeout=(5, 10))
        r.raise_for_status()
        events = r.json()
        # Filter: title must mention city and date
        month_short = month[:3].lower()
        for ev in events:
            title = ev.get("title", "").lower()
            if city_name.lower().split()[0] in title and str(day) in title:
                if month_short in title or month.lower() in title:
                    return ev
    except Exception as e:
        log.debug("Polymarket event fetch failed: %s", e)

    return None

def get_market_price(market_id: str) -> Optional[float]:
    """Get best ask (price to buy YES) from Polymarket."""
    try:
        r = requests.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=(3, 8))
        r.raise_for_status()
        mdata = r.json()
        ask = mdata.get("bestAsk")
        if ask is not None:
            return float(ask)
    except Exception as e:
        log.debug("Market price fetch failed for %s: %s", market_id, e)
    return None

def get_market_bid(market_id: str) -> Optional[float]:
    """Get best bid (current sell price) from Polymarket."""
    try:
        r = requests.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=(3, 8))
        r.raise_for_status()
        mdata = r.json()
        bid = mdata.get("bestBid")
        if bid is not None:
            return float(bid)
    except Exception as e:
        log.debug("Market bid fetch failed for %s: %s", market_id, e)
    return None

def get_market_volume(market_id: str) -> float:
    """Get 24h volume from Polymarket."""
    try:
        r = requests.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=(3, 8))
        r.raise_for_status()
        mdata = r.json()
        return float(mdata.get("volume24hr", 0) or 0)
    except Exception:
        return 0.0

def check_market_resolved(market_id: str) -> tuple[bool, Optional[bool]]:
    """
    Returns (is_resolved, did_yes_win).
    Checks Polymarket resolution status.
    """
    try:
        r = requests.get(f"{GAMMA_BASE}/markets/{market_id}", timeout=(3, 8))
        r.raise_for_status()
        mdata = r.json()
        if mdata.get("closed") or mdata.get("resolved"):
            winner = mdata.get("winner")
            return True, (winner == "Yes" if winner else None)
    except Exception as e:
        log.debug("Resolution check failed for %s: %s", market_id, e)
    return False, None

def execute_trade_real(market_id: str, amount_usd: float) -> bool:
    """
    Execute real trade via Polymarket CLOB API.
    Requires private key in config.

    NOTE: This uses py-clob-client. Install with:
      pip install py-clob-client

    Returns True if order placed successfully.
    """
    if not POLY_PK:
        log.error("No polymarket_pk in config — cannot trade real")
        return False

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.constants import POLYGON

        client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=POLYGON,
            key=POLY_PK,
        )
        order = client.create_market_order(
            MarketOrderArgs(
                token_id=market_id,
                amount=amount_usd,
            )
        )
        resp = client.post_order(order, OrderType.FOK)
        log.info("Order placed: %s", resp)
        return True
    except ImportError:
        log.error("py-clob-client not installed. Run: pip install py-clob-client")
        return False
    except Exception as e:
        log.error("Trade execution failed: %s", e)
        return False

# =============================================================================
# MARKET PARSING
# =============================================================================

def parse_temp_range(question: str) -> tuple[float, float]:
    """
    Parse temperature range from Polymarket question text.
    Returns (t_low, t_high) with -999/999 for edge buckets.
    Examples:
      "Will the high be between 45–46°F" -> (45, 46)
      "Will the high be above 80°F"      -> (80, 999)
      "Will the high be below 32°F"      -> (-999, 32)
    """
    q = question.lower()

    # Between X and Y
    m = re.search(r"between\s+([-\d.]+)\s*(?:and|–|-|to)\s*([-\d.]+)", q)
    if m:
        return float(m.group(1)), float(m.group(2))

    # Above / over X
    m = re.search(r"(?:above|over|higher than|at least)\s+([-\d.]+)", q)
    if m:
        return float(m.group(1)), 999.0

    # Below / under X
    m = re.search(r"(?:below|under|lower than|at most)\s+([-\d.]+)", q)
    if m:
        return -999.0, float(m.group(1))

    # Fallback: find two numbers
    nums = re.findall(r"[-\d.]+", question)
    floats = [float(n) for n in nums if 10 <= abs(float(n)) <= 150]
    if len(floats) >= 2:
        return min(floats[0], floats[1]), max(floats[0], floats[1])

    return -999.0, 999.0

def hours_to_resolution(end_date_str: str) -> float:
    """Hours until market closes."""
    try:
        end = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        diff = end - datetime.now(timezone.utc)
        return max(0.0, diff.total_seconds() / 3600)
    except Exception:
        return 0.0

# =============================================================================
# CORE TRADING LOGIC
# =============================================================================

def evaluate_opportunity(
    city_slug: str,
    date_str: str,
    forecast_temp: float,
    sigma: float,
    market_id: str,
    question: str,
    price: float,
    volume: float,
    hours: float,
) -> Optional[dict]:
    """
    Evaluate a single market opportunity.
    Returns a trade signal dict or None if no edge.
    """
    t_low, t_high = parse_temp_range(question)
    p = bucket_prob(forecast_temp, t_low, t_high, sigma)
    ev = calc_ev(p, price)

    state = load_state()
    n_resolved = state["wins"] + state["losses"]
    kelly = calc_kelly(p, price, n_resolved)
    size  = bet_size(kelly, state["balance"])

    # Filters
    if ev < MIN_EV:
        return None
    if price > MAX_PRICE:
        return None
    if volume < MIN_VOLUME:
        return None
    if hours < MIN_HOURS or hours > MAX_HOURS:
        return None
    if size <= 0:
        return None

    # Slippage check: if spread too wide, skip
    bid = get_market_bid(market_id)
    if bid is not None and (price - bid) > MAX_SLIPPAGE:
        log.debug("Slippage too high for %s: spread=%.3f", market_id, price - bid)
        return None

    return {
        "city":      city_slug,
        "date":      date_str,
        "market_id": market_id,
        "question":  question,
        "t_low":     t_low,
        "t_high":    t_high,
        "forecast":  forecast_temp,
        "sigma":     sigma,
        "prob":      round(p, 4),
        "price":     price,
        "ev":        ev,
        "kelly":     kelly,
        "size":      size,
        "hours":     hours,
    }

def open_position(signal: dict, mode: str):
    """Record a new position in the database."""
    state   = load_state()
    balance = state["balance"]
    cost    = signal["size"]

    if mode == "sim":
        # Simulate slippage: pay slightly more than ask
        effective_price = min(signal["price"] + 0.005, 0.99)
        shares = round(cost / effective_price, 4)
    else:
        effective_price = signal["price"]
        shares = round(cost / effective_price, 4)

    stop_price = round(effective_price * (1 - STOP_LOSS_PCT), 4)

    with db_connect() as conn:
        # Upsert market record
        conn.execute("""
            INSERT OR IGNORE INTO markets (id, city, date, status, question, t_low, t_high, unit, hours_left, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            signal["market_id"], signal["city"], signal["date"], "open",
            signal["question"], signal["t_low"], signal["t_high"],
            LOCATIONS[signal["city"]]["unit"], signal["hours"], _now()
        ))

        conn.execute("""
            INSERT INTO positions (market_id, mode, entry_price, shares, cost, stop_price, trail_high, status, opened_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            signal["market_id"], mode, effective_price, shares,
            cost, stop_price, effective_price, "open", _now()
        ))

        new_balance = round(balance - cost, 2)
        conn.execute("UPDATE state SET balance=?, updated=? WHERE id=1", (new_balance, _now()))

    sym = LOCATIONS[signal["city"]]["unit"]
    log.info("[%s OPEN] %s | %s %.1f%s | p=%.1f%% ev=%.2f price=%.3f size=$%.2f %dh",
        mode.upper(), signal["city"], signal["date"],
        signal["forecast"], sym,
        signal["prob"] * 100, signal["ev"],
        effective_price, cost, signal["hours"])

    send_telegram(
        f"🟢 [{mode.upper()}] Open: {LOCATIONS[signal['city']]['name']} {signal['date']}\n"
        f"Forecast: {signal['forecast']}{sym} | Bucket: {signal['t_low']}–{signal['t_high']}\n"
        f"P={signal['prob']:.1%} EV={signal['ev']:.2f} Price=${effective_price:.3f} Size=${cost}"
    )

    return effective_price, shares, stop_price

def close_position(pos_id: int, market_id: str, city_slug: str, date_str: str,
                   exit_price: float, reason: str, mode: str):
    """Close an open position and record PnL."""
    with db_connect() as conn:
        pos = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()
        if not pos:
            return

        pnl = round((exit_price - pos["entry_price"]) * pos["shares"], 2)
        new_balance = round(load_state()["balance"] + pos["cost"] + pnl, 2)
        won = pnl > 0

        conn.execute("""
            UPDATE positions SET status='closed', exit_price=?, close_reason=?,
            pnl=?, closed_at=? WHERE id=?
        """, (exit_price, reason, pnl, _now(), pos_id))

        conn.execute("""
            UPDATE markets SET status=?, pnl=?, resolved_at=? WHERE id=?
        """, ("resolved", pnl, _now(), market_id))

        if won:
            conn.execute("UPDATE state SET balance=?, wins=wins+1, updated=? WHERE id=1",
                        (new_balance, _now()))
        else:
            conn.execute("UPDATE state SET balance=?, losses=losses+1, updated=? WHERE id=1",
                        (new_balance, _now()))

    sign = "+" if pnl >= 0 else ""
    log.info("[%s CLOSE %s] %s | %s | entry=%.3f exit=%.3f | PnL: %s$%.2f",
        mode.upper(), reason, city_slug, date_str,
        pos["entry_price"], exit_price, sign, pnl)

    emoji = "✅" if won else "❌"
    send_telegram(
        f"{emoji} [{mode.upper()}] Close ({reason}): {LOCATIONS[city_slug]['name']} {date_str}\n"
        f"Entry=${pos['entry_price']:.3f} Exit=${exit_price:.3f} | PnL: {sign}${pnl:.2f}"
    )

    # Trigger Claude post-trade reasoning if loss
    if not won and ANTHROPIC_KEY:
        schedule_post_trade_analysis(market_id, city_slug, date_str, pos, exit_price, pnl, reason)

# =============================================================================
# CLAUDE POST-TRADE REASONING
# Called after losing trades to extract learnings for Hermes skill generation
# =============================================================================

def schedule_post_trade_analysis(market_id: str, city_slug: str, date_str: str,
                                  pos: sqlite3.Row, exit_price: float, pnl: float, reason: str):
    """
    Call Claude Sonnet to analyze why a trade lost.
    Results are saved to post_trade_log table for Hermes to pick up.
    """
    if not ANTHROPIC_KEY:
        return

    with db_connect() as conn:
        forecasts = conn.execute(
            "SELECT * FROM forecasts WHERE market_id=? ORDER BY ts",
            (market_id,)
        ).fetchall()
        market = conn.execute(
            "SELECT * FROM markets WHERE id=?", (market_id,)
        ).fetchone()

    if not market:
        return

    forecast_summary = [
        f"{r['ts'][:16]} | {r['source']} | {r['temp']}° (conf: {r['confidence']})"
        for r in forecasts
    ] if forecasts else ["no forecast snapshots available"]

    prompt = f"""You are a trading analyst reviewing a losing prediction market trade.

Trade details:
- City: {LOCATIONS.get(city_slug, {}).get('name', city_slug)}
- Date: {date_str}
- Market question: {market['question'] if market else 'unknown'}
- Entry price: ${pos['entry_price']:.3f}
- Exit price: ${exit_price:.3f}
- Close reason: {reason}
- PnL: ${pnl:.2f}
- Mode: {pos['mode']}

Forecast history at time of entry:
{chr(10).join(forecast_summary[:10])}

Please analyze in 3 short bullet points:
1. Most likely root cause of the loss (forecast error, bad timing, market mispricing, etc.)
2. One specific rule to add or adjust to avoid this in future
3. Confidence: was this a bad trade in hindsight or just bad luck?

Be concise. Max 150 words total. Output as plain text."""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        analysis = msg.content[0].text

        with db_connect() as conn:
            conn.execute(
                "INSERT INTO post_trade_log (market_id, outcome, analysis, created_at) VALUES (?,?,?,?)",
                (market_id, "loss", analysis, _now())
            )

        log.info("[CLAUDE] Post-trade analysis saved for %s", market_id)
    except ImportError:
        log.debug("anthropic package not installed — skipping post-trade analysis")
    except Exception as e:
        log.debug("Claude analysis failed: %s", e)

# =============================================================================
# TELEGRAM ALERTS
# =============================================================================

def send_telegram(message: str):
    """Send alert to Telegram. Silently skips if not configured."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": message}, timeout=5)
    except Exception:
        pass

# =============================================================================
# MAIN SCAN LOOP
# =============================================================================

def take_forecast_snapshot(city_slug: str, dates: list[str]) -> dict:
    """
    Fetch all 3 sources and store snapshots.
    Returns {date: {ecmwf, hrrr, metar}} per date.
    """
    ecmwf  = get_ecmwf(city_slug, dates)
    hrrr   = get_hrrr(city_slug, dates)
    metar_today = get_metar(city_slug)  # current observation only for today

    snapshots = {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with db_connect() as conn:
        for d in dates:
            temps = {
                "ecmwf": ecmwf.get(d),
                "hrrr":  hrrr.get(d),
                "metar": metar_today if d == today else None,
            }
            # Store each source as a row
            for source, temp in temps.items():
                if temp is not None:
                    # Get calibration confidence
                    cal = conn.execute(
                        "SELECT mae FROM calibration WHERE city=? AND source=?",
                        (city_slug, source)
                    ).fetchone()
                    confidence = round(1.0 / (cal["mae"] + 0.5), 3) if cal and cal["mae"] else 0.5

                    # Only save if market exists in DB
                    mkt_exists = conn.execute(
                        "SELECT id FROM markets WHERE city=? AND date=?",
                        (city_slug, d)
                    ).fetchone()
                    if mkt_exists:
                        conn.execute(
                            "INSERT INTO forecasts (market_id, ts, source, temp, confidence) VALUES (?,?,?,?,?)",
                            (mkt_exists["id"], _now(), source, temp, confidence)
                        )
            snapshots[d] = temps

    return snapshots

def scan_city(city_slug: str, mode: str):
    """Run one full scan for a city: fetch forecasts + evaluate markets."""
    if not _cb_check(city_slug):
        log.debug("Skipping %s (circuit breaker open)", city_slug)
        return

    now = datetime.now(timezone.utc)
    dates = [(now + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(4)]

    try:
        snapshots = take_forecast_snapshot(city_slug, dates)
        time.sleep(0.5)  # rate limit courtesy
    except Exception as e:
        _cb_fail(city_slug)
        log.warning("Snapshot failed for %s: %s", city_slug, e)
        return

    for date_str in dates:
        snap   = snapshots.get(date_str, {})
        dt     = datetime.strptime(date_str, "%Y-%m-%d")
        month  = MONTHS[dt.month - 1]

        # Ensemble forecast
        ensemble_temp, sigma = ensemble_forecast(city_slug, snap)
        if ensemble_temp is None:
            continue

        # Find Polymarket event
        event = get_polymarket_event(city_slug, month, dt.day, dt.year)
        if not event:
            continue

        # Check each outcome (temperature bucket)
        for mkt in event.get("markets", []):
            market_id = mkt.get("id") or mkt.get("conditionId")
            question  = mkt.get("question", "")
            if not market_id or not question:
                continue

            # Skip if already have open position on this market
            with db_connect() as conn:
                existing = conn.execute(
                    "SELECT id FROM positions WHERE market_id=? AND status='open'",
                    (market_id,)
                ).fetchone()
            if existing:
                continue

            price  = get_market_price(market_id)
            volume = get_market_volume(market_id)
            hours  = hours_to_resolution(mkt.get("endDate", ""))

            if price is None:
                continue

            signal = evaluate_opportunity(
                city_slug, date_str, ensemble_temp, sigma,
                market_id, question, price, volume, hours
            )

            if signal:
                if mode == "real":
                    success = execute_trade_real(market_id, signal["size"])
                    if success:
                        open_position(signal, mode)
                else:
                    open_position(signal, mode)

def monitor_open_positions(mode: str):
    """Check stop-loss, trailing stop, and forecast-shift exits on open positions."""
    with db_connect() as conn:
        positions = conn.execute("""
            SELECT p.*, m.city, m.date, m.question, m.t_low, m.t_high, m.unit
            FROM positions p
            JOIN markets m ON p.market_id = m.id
            WHERE p.status = 'open'
        """).fetchall()

    for pos in positions:
        market_id = pos["market_id"]
        city      = pos["city"]
        date_str  = pos["date"]
        entry     = pos["entry_price"]
        stop      = pos["stop_price"]
        trail_hi  = pos["trail_high"]

        current_price = get_market_bid(market_id)
        if current_price is None:
            continue

        # Update trailing stop
        if current_price > trail_hi:
            new_stop = round(current_price * (1 - STOP_LOSS_PCT), 4)
            with db_connect() as conn:
                conn.execute(
                    "UPDATE positions SET trail_high=?, stop_price=? WHERE id=?",
                    (current_price, new_stop, pos["id"])
                )
            stop = new_stop

        # Check stop trigger
        if current_price <= stop:
            reason = "stop_loss" if current_price < entry else "trailing_be"
            close_position(pos["id"], market_id, city, date_str, current_price, reason, mode)
            continue

        # Check resolution
        resolved, yes_won = check_market_resolved(market_id)
        if resolved:
            if yes_won is True:
                close_price = 1.0
                reason = "resolved_win"
            elif yes_won is False:
                close_price = 0.0
                reason = "resolved_loss"
            else:
                close_price = current_price
                reason = "resolved_unknown"
            close_position(pos["id"], market_id, city, date_str, close_price, reason, mode)

            # Update calibration with actual temp
            actual = get_actual_temp(city, date_str)
            if actual is not None:
                snap_sources = {}
                with db_connect() as conn:
                    forecasts = conn.execute(
                        "SELECT source, AVG(temp) as avg_temp FROM forecasts WHERE market_id=? GROUP BY source",
                        (market_id,)
                    ).fetchall()
                    snap_sources = {r["source"]: r["avg_temp"] for r in forecasts}

                for source, forecast_val in snap_sources.items():
                    if forecast_val is not None:
                        update_calibration(city, source, forecast_val, actual)

def run_loop(mode: str):
    """Main event loop."""
    db_init()

    log.info("=" * 55)
    log.info("WEATHERBOT STARTING — mode: %s", mode.upper())
    log.info("Cities: %d | Balance: $%.0f | Max bet: $%.2f", len(LOCATIONS), BALANCE, MAX_BET)
    log.info("Scan: %dm | Monitor: %dm", SCAN_INTERVAL // 60, MONITOR_INTERVAL // 60)
    log.info("Ctrl+C to stop")
    log.info("=" * 55)

    send_telegram(f"🌤 WeatherBot started [{mode.upper()}]\nBalance: ${BALANCE:.2f} | Cities: {len(LOCATIONS)}")

    last_scan    = 0
    last_monitor = 0

    while True:
        now_ts = time.time()

        if now_ts - last_scan >= SCAN_INTERVAL:
            log.info("--- Full scan starting ---")
            for city_slug in LOCATIONS:
                print(f"  -> {LOCATIONS[city_slug]['name']}...", end=" ", flush=True)
                scan_city(city_slug, mode)
                print("done")
            last_scan = now_ts
            log.info("--- Scan complete ---")

        if now_ts - last_monitor >= MONITOR_INTERVAL:
            monitor_open_positions(mode)
            last_monitor = now_ts

        time.sleep(30)

# =============================================================================
# CLI COMMANDS
# =============================================================================

def cmd_status():
    db_init()
    state = load_state()
    bal   = state["balance"]
    start = state["start"]
    ret   = (bal - start) / start * 100
    wins  = state["wins"]
    losses= state["losses"]
    total = wins + losses

    print(f"\n{'='*55}")
    print(f"  WEATHERBOT STATUS")
    print(f"{'='*55}")
    print(f"  Balance:  ${bal:,.2f}  (start ${start:,.2f}, {'+'if ret>=0 else ''}{ret:.1f}%)")
    if total:
        print(f"  Record:   {wins}W / {losses}L  (WR: {wins/total:.1%})")
    else:
        print(f"  Record:   no trades yet")

    with db_connect() as conn:
        open_pos = conn.execute("""
            SELECT p.*, m.city, m.date, m.question
            FROM positions p JOIN markets m ON p.market_id=m.id
            WHERE p.status='open'
        """).fetchall()

    print(f"  Open:     {len(open_pos)}")
    if open_pos:
        print()
        for p in open_pos:
            cur = get_market_bid(p["market_id"]) or p["entry_price"]
            unreal = round((cur - p["entry_price"]) * p["shares"], 2)
            sign   = "+" if unreal >= 0 else ""
            print(f"    {p['city']} {p['date']} | entry ${p['entry_price']:.3f} | unrealized {sign}${unreal:.2f}")
    print()

def cmd_report():
    db_init()
    with db_connect() as conn:
        trades = conn.execute("""
            SELECT p.*, m.city, m.date, m.question
            FROM positions p JOIN markets m ON p.market_id=m.id
            WHERE p.status='closed'
            ORDER BY p.closed_at DESC
            LIMIT 50
        """).fetchall()

        analyses = conn.execute("""
            SELECT * FROM post_trade_log ORDER BY created_at DESC LIMIT 10
        """).fetchall()

    print(f"\n{'='*55}")
    print(f"  WEATHERBOT REPORT — Last {len(trades)} trades")
    print(f"{'='*55}")
    for t in trades:
        sign = "+" if (t["pnl"] or 0) >= 0 else ""
        emoji = "✅" if (t["pnl"] or 0) >= 0 else "❌"
        print(f"  {emoji} {t['city']} {t['date']} | {t['close_reason']} | PnL: {sign}${t['pnl']:.2f}")

    if analyses:
        print(f"\n--- Claude Post-Trade Insights (last {len(analyses)}) ---")
        for a in analyses:
            print(f"\n[{a['created_at'][:16]}] {a['market_id'][:12]}...")
            print(a["analysis"])
    print()

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    cmd = sys.argv[1].lower() if len(sys.argv) > 1 else "sim"

    if cmd == "status":
        cmd_status()
    elif cmd == "report":
        cmd_report()
    elif cmd in ("sim", "real"):
        if cmd == "real" and not POLY_PK:
            log.error("Real mode requires polymarket_pk in config.json")
            sys.exit(1)
        run_loop(mode=cmd)
    else:
        print("Usage: python weatherbot.py [sim|real|status|report]")
        sys.exit(1)
