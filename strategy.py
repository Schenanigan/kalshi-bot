"""
strategy.py — Trading strategies for expiring and weather markets.

Each strategy takes a CandidateMarket + orderbook and returns an OrderIntent (or None).

Bot.py imports: BaseStrategy, FairValueStrategy, ExpiryMomentumStrategy, OrderIntent
"""

import json
import logging
import os
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from math import erf, sqrt
from typing import Optional
from zoneinfo import ZoneInfo

import requests

import math

import config
from scanner import CandidateMarket


# Euler-Mascheroni constant — needed to align Gumbel mode with arithmetic mean
_EULER_GAMMA = 0.5772156649015329


def gumbel_cdf_max(x: float, mean: float, sigma: float) -> float:
    """CDF of the Gumbel-max distribution at x, parameterized so that the
    distribution has the given arithmetic mean and standard deviation σ.

    Daily maxima follow a Gumbel (Type-I extreme value) distribution, not a
    Gaussian — using a normal CDF biases tail probabilities by 5–15% in
    exactly the region partition-arb edge lives.

    Standard parameterization: F(x) = exp(-exp(-(x - μ) / β))
    with β = σ * √6 / π and μ = mean - γ * β.
    """
    if sigma <= 0:
        return 1.0 if x >= mean else 0.0
    beta = sigma * math.sqrt(6) / math.pi
    mu = mean - _EULER_GAMMA * beta
    z = (x - mu) / beta
    # Clamp to avoid math overflow on extreme z; F saturates at 0/1 anyway.
    if z > 50:
        return 1.0
    if z < -50:
        return 0.0
    return math.exp(-math.exp(-z))


def gumbel_cdf_min(x: float, mean: float, sigma: float) -> float:
    """CDF of the Gumbel-min distribution at x. Daily minima are extremes of
    a negated process, so F_min(x) = 1 - F_max(-x; -mean, σ)."""
    return 1.0 - gumbel_cdf_max(-x, -mean, sigma)


def kalshi_fee(contracts: int, price_cents: int, rate: float = config.TAKER_FEE_RATE) -> float:
    """Compute Kalshi fee in dollars: ceil(rate × C × P × (1−P)).

    Args:
        contracts: number of contracts
        price_cents: price per contract in cents (1-99)
        rate: fee rate (default: taker)

    Returns:
        Fee in dollars, rounded up to nearest cent.
    """
    p = price_cents / 100
    raw = rate * contracts * p * (1 - p)
    return math.ceil(raw * 100) / 100  # round up to nearest cent

log = logging.getLogger(__name__)

# ── City coordinates for dynamic NWS grid resolution ─────────────────────────

CITY_COORDS: dict[str, tuple[float, float]] = {
    "San Francisco": (37.7749, -122.4194),
    "New York":      (40.7128, -74.0060),
    "Chicago":       (41.8781, -87.6298),
    "Los Angeles":   (34.0522, -118.2437),
    "Seattle":       (47.6062, -122.3321),
    "Miami":         (25.7617, -80.1918),
    "Denver":        (39.7392, -104.9903),
    "Houston":       (29.7604, -95.3698),
    "Phoenix":       (33.4484, -112.0740),
    "Philadelphia":  (39.9526, -75.1652),
    "Dallas":        (32.7767, -96.7970),
    "Atlanta":       (33.7490, -84.3880),
    "Boston":        (42.3601, -71.0589),
    "Detroit":       (42.3314, -83.0458),
    "Minneapolis":   (44.9778, -93.2650),
    "Las Vegas":     (36.1699, -115.1398),
    "Portland":      (45.5152, -122.6784),
    "Charlotte":     (35.2271, -80.8431),
    "Nashville":     (36.1627, -86.7816),
    "Austin":        (30.2672, -97.7431),
    "Oklahoma City": (35.4676, -97.5164),
    "Washington DC": (38.9072, -77.0369),
    "New Orleans":   (29.9511, -90.0715),
    "San Antonio":   (29.4241, -98.4936),
}

# Series ticker → city. Real Kalshi weather markets often omit the city
# from the per-market title (e.g. KXHIGHTLV markets are titled "Will the
# maximum temperature be <75°…" with no city word at all). The city only
# lives in the series name, so this map is the only reliable resolver.
# Extend as new city series go live.
SERIES_CITY_MAP: dict[str, str] = {
    # New York
    "KXHIGHNY":      "New York",
    "KXLOWNY":       "New York",
    "KXLOWTNYC":     "New York",
    # Las Vegas
    "KXHIGHTLV":     "Las Vegas",
    "KXLOWTLV":      "Las Vegas",
    # Oklahoma City
    "KXHIGHTOKC":    "Oklahoma City",
    "KXLOWTOKC":     "Oklahoma City",
    # Denver
    "KXHIGHTEMPDEN": "Denver",
    "KXHIGHDEN":     "Denver",
    "KXLOWDEN":      "Denver",
    "KXLOWTDEN":     "Denver",
    # Chicago
    "KXHIGHCHI":     "Chicago",
    "KXLOWCHI":      "Chicago",
    "KXLOWTCHI":     "Chicago",
    # Los Angeles
    "KXHIGHLA":      "Los Angeles",
    "KXHIGHLAX":     "Los Angeles",
    "KXLOWLA":       "Los Angeles",
    "KXLOWTLAX":     "Los Angeles",
    # Miami
    "KXHIGHMIA":     "Miami",
    "KXLOWMIA":      "Miami",
    "KXLOWTMIA":     "Miami",
    # Houston
    "KXHIGHHOU":     "Houston",
    "KXHIGHTHOU":    "Houston",
    "KXLOWHOU":      "Houston",
    "KXLOWTHOU":     "Houston",
    # Phoenix
    "KXHIGHPHX":     "Phoenix",
    "KXHIGHTPHX":    "Phoenix",
    "KXLOWPHX":      "Phoenix",
    "KXLOWTPHX":     "Phoenix",
    # Atlanta
    "KXHIGHATL":     "Atlanta",
    "KXHIGHTATL":    "Atlanta",
    "KXLOWATL":      "Atlanta",
    "KXLOWTATL":     "Atlanta",
    # Boston
    "KXHIGHBOS":     "Boston",
    "KXHIGHTBOS":    "Boston",
    "KXLOWBOS":      "Boston",
    "KXLOWTBOS":     "Boston",
    # Austin
    "KXHIGHAUS":     "Austin",
    "KXLOWTAUS":     "Austin",
    # Philadelphia
    "KXHIGHPHIL":    "Philadelphia",
    "KXLOWTPHIL":    "Philadelphia",
    # Dallas
    "KXHIGHTDAL":    "Dallas",
    "KXLOWTDAL":     "Dallas",
    # Washington DC
    "KXHIGHTDC":     "Washington DC",
    "KXLOWTDC":      "Washington DC",
    # Minneapolis
    "KXHIGHTMIN":    "Minneapolis",
    "KXLOWTMIN":     "Minneapolis",
    # New Orleans
    "KXHIGHTNOLA":   "New Orleans",
    "KXLOWTNOLA":    "New Orleans",
    # San Antonio
    "KXHIGHTSATX":   "San Antonio",
    "KXLOWTSATX":    "San Antonio",
    # Seattle
    "KXHIGHTSEA":    "Seattle",
    "KXLOWTSEA":     "Seattle",
    # San Francisco
    "KXHIGHTSFO":    "San Francisco",
    "KXLOWTSFO":     "San Francisco",
}

# Aliases / abbreviations that may appear in real Kalshi titles
# (e.g. "high temp in NYC", "Vegas Max Daily Temperature").
# Order matters — longer aliases first so they win in title matching.
CITY_ALIASES: list[tuple[str, str]] = [
    ("oklahoma city", "Oklahoma City"),
    ("san francisco", "San Francisco"),
    ("los angeles",   "Los Angeles"),
    ("philadelphia",  "Philadelphia"),
    ("minneapolis",   "Minneapolis"),
    ("las vegas",     "Las Vegas"),
    ("new york",      "New York"),
    ("nashville",     "Nashville"),
    ("charlotte",     "Charlotte"),
    ("portland",      "Portland"),
    ("houston",       "Houston"),
    ("chicago",       "Chicago"),
    ("phoenix",       "Phoenix"),
    ("seattle",       "Seattle"),
    ("atlanta",       "Atlanta"),
    ("detroit",       "Detroit"),
    ("denver",        "Denver"),
    ("boston",        "Boston"),
    ("austin",        "Austin"),
    ("dallas",        "Dallas"),
    ("miami",         "Miami"),
    ("vegas",         "Las Vegas"),
    ("washington",    "Washington DC"),
    ("new orleans",   "New Orleans"),
    ("san antonio",   "San Antonio"),
    ("nola",          "New Orleans"),
    ("nyc",           "New York"),
    ("okc",           "Oklahoma City"),
    ("sfo",           "San Francisco"),
]

# Map Kalshi ticker city codes to city names
TICKER_CITY_MAP: dict[str, str] = {
    "SFO": "San Francisco",
    "SF":  "San Francisco",
    "NYC": "New York",
    "CHI": "Chicago",
    "LAX": "Los Angeles",
    "LA":  "Los Angeles",
    "SEA": "Seattle",
    "MIA": "Miami",
    "DEN": "Denver",
    "HOU": "Houston",
    "PHX": "Phoenix",
    "PHL": "Philadelphia",
    "DFW": "Dallas",
    "DAL": "Dallas",
    "ATL": "Atlanta",
    "BOS": "Boston",
    "DTW": "Detroit",
    "DET": "Detroit",
    "MSP": "Minneapolis",
    "MIN": "Minneapolis",
    "LAS": "Las Vegas",
    "PDX": "Portland",
    "CLT": "Charlotte",
    "BNA": "Nashville",
    "AUS": "Austin",
}

# ── ICAO airport codes for METAR observation data ─────────────────────────

CITY_ICAO: dict[str, str] = {
    "San Francisco": "KSFO",
    "New York":      "KJFK",
    "Chicago":       "KORD",
    "Los Angeles":   "KLAX",
    "Seattle":       "KSEA",
    "Miami":         "KMIA",
    "Denver":        "KDEN",
    "Houston":       "KIAH",
    "Phoenix":       "KPHX",
    "Philadelphia":  "KPHL",
    "Dallas":        "KDFW",
    "Atlanta":       "KATL",
    "Boston":        "KBOS",
    "Detroit":       "KDTW",
    "Minneapolis":   "KMSP",
    "Las Vegas":     "KLAS",
    "Portland":      "KPDX",
    "Charlotte":     "KCLT",
    "Nashville":     "KBNA",
    "Austin":        "KAUS",
    "Oklahoma City": "KOKC",
    "Washington DC": "KDCA",
    "San Antonio":   "KSAT",
    "New Orleans":   "KMSY",
}

# Local timezone for each city — required so the "observed daily extreme"
# filter rolls over at local midnight, not UTC. A 24h rolling window would
# leak yesterday's max into today's KXHIGH market until the old reading aged
# out 24h later, which can mark a leg "dead" based on weather that no longer
# counts toward the settle.
CITY_TZ: dict[str, str] = {
    "San Francisco": "America/Los_Angeles",
    "Los Angeles":   "America/Los_Angeles",
    "Seattle":       "America/Los_Angeles",
    "Portland":      "America/Los_Angeles",
    "Las Vegas":     "America/Los_Angeles",
    "Phoenix":       "America/Phoenix",
    "Denver":        "America/Denver",
    "Chicago":       "America/Chicago",
    "Houston":       "America/Chicago",
    "Dallas":        "America/Chicago",
    "Austin":        "America/Chicago",
    "San Antonio":   "America/Chicago",
    "Oklahoma City": "America/Chicago",
    "Minneapolis":   "America/Chicago",
    "Nashville":     "America/Chicago",
    "New Orleans":   "America/Chicago",
    "Atlanta":       "America/New_York",
    "Charlotte":     "America/New_York",
    "Detroit":       "America/New_York",
    "Miami":         "America/New_York",
    "Washington DC": "America/New_York",
    "Philadelphia":  "America/New_York",
    "New York":      "America/New_York",
    "Boston":        "America/New_York",
}

METAR_API = "https://aviationweather.gov/api/data/metar"
METAR_CACHE_SECONDS = 900  # 15 min — METARs update hourly

# Open-Meteo GEFS ensemble — free, no key, mirrors NOAA GEFS 31-member output.
# Used to derive a *measured* forecast σ from member spread, replacing the
# hardcoded NWS_SIGMA_DEGREES constant. Ensemble runs update every 6h, so a
# 30-min cache is generous.
ENSEMBLE_API = "https://ensemble-api.open-meteo.com/v1/ensemble"
ENSEMBLE_CACHE_SECONDS = 1800
ENSEMBLE_MIN_SIGMA_F = 1.5  # floor: real forecast uncertainty rarely drops
                            # below this even with ensemble agreement
ENSEMBLE_MAX_SIGMA_F = 10.0 # cap: pathological spread → fail closed by clamping
ENSEMBLE_FAILURE_TTL = 600  # back off from Open-Meteo for 10 min after a
                            # transient failure; one failed lookup poisons
                            # the forecast for that lead-time anyway, no
                            # point hammering it every 30s
ENSEMBLE_MAX_CONSECUTIVE_FAILURES = 3  # before tripping the TTL backoff


def diurnal_sigma_factor(close_dt, tz_name: Optional[str], is_high: bool) -> float:
    """Return σ multiplier in [0.3, 1.0] based on time of day.

    Daily highs typically peak 13:00-17:00 local; lows 04:00-08:00 local.
    Once the relevant peak window has passed (and we're approaching close),
    the day's extreme is essentially locked — σ should collapse rather than
    stay flat at the time-to-close formula. Without this, end-of-day temp
    markets price in spurious uncertainty.

    Returns 1.0 when the peak window is still ahead, 0.3 once well past.
    """
    if not tz_name or close_dt is None:
        return 1.0
    try:
        local_now = datetime.now(ZoneInfo(tz_name))
    except Exception:
        return 1.0
    hour = local_now.hour + local_now.minute / 60.0
    if is_high:
        peak_end = 17.0  # by 5pm the high is usually in
    else:
        peak_end = 8.0   # by 8am the low is usually past
    if hour <= peak_end:
        return 1.0
    # Linearly shrink σ from 1.0 at peak_end to 0.3 by 4 hours later.
    hours_past = hour - peak_end
    factor = max(0.3, 1.0 - 0.175 * hours_past)
    return factor


def lead_time_sigma_floor(hours_to_close: float) -> float:
    """Minimum plausible forecast σ as a function of lead time.

    Even when an ensemble's members happen to cluster, real-world forecast
    error grows with lead time. Floor σ at this level so a momentarily-tight
    ensemble can't push us into over-confident bets multiple days out.

    Empirical: NWS skill scores roughly imply ~1°F at +24h, ~2°F at +72h,
    ~3°F at +120h. Linear-in-hours approximation, capped.
    """
    h = max(0.0, hours_to_close)
    # 0.025°F per hour of lead time, plus a 1°F base floor
    return min(4.0, 1.0 + 0.025 * h)


PREDICTIONS_LOG_DIR = os.environ.get(
    "KALSHI_PREDICTIONS_DIR", ".",
)

# Online σ recalibration: sigma multiplier learned from recent ECE.
# Persisted to disk by recompute_sigma_multiplier(); read at boot.
_SIGMA_MULTIPLIER_PATH = os.environ.get(
    "KALSHI_SIGMA_MULT_PATH", "sigma_multiplier.json",
)
_SIGMA_MULTIPLIER: float = 1.0


def _load_sigma_multiplier() -> None:
    global _SIGMA_MULTIPLIER
    try:
        if os.path.exists(_SIGMA_MULTIPLIER_PATH):
            with open(_SIGMA_MULTIPLIER_PATH) as f:
                d = json.load(f) or {}
            v = float(d.get("multiplier", 1.0))
            _SIGMA_MULTIPLIER = max(0.5, min(2.0, v))
    except Exception:
        _SIGMA_MULTIPLIER = 1.0


def recompute_sigma_multiplier() -> Optional[dict]:
    """Read calibration logs, derive a σ-multiplier nudge, persist it.

    If recent confident-bin predictions are over-confident on average
    (predicted - actual > 0), bump σ up by 10%. Under-confident → down.
    Bounded to [0.6, 1.5] and changes by at most 10% per recompute.
    Called at bot start; safe to skip silently when no data.
    """
    global _SIGMA_MULTIPLIER
    try:
        import sys as _sys
        _sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))
        from calibration import load_rotated, reconcile
        from pathlib import Path as _Path
        preds = load_rotated(_Path("."), "predictions")
        outs = load_rotated(_Path("."), "outcomes")
        pairs = reconcile(preds, outs)
        if len(pairs) < 30:  # need a meaningful sample
            return None
        # Average gap (predicted - actual) on confident bins (P>0.7 or <0.3).
        confident = [(p, y) for p, y, _ in pairs if p > 0.7 or p < 0.3]
        if len(confident) < 15:
            return None
        # For YES-leaning predictions (p>0.7): gap = avg_p - avg_y.
        # Positive gap → over-confident YES → σ should widen.
        gap_high = (
            sum(p - y for p, y in confident if p > 0.7) /
            max(1, sum(1 for p, _ in confident if p > 0.7))
        )
        # For NO-leaning predictions (p<0.3): mirror — under-prediction → σ widens.
        gap_low = (
            sum(y - p for p, y in confident if p < 0.3) /
            max(1, sum(1 for p, _ in confident if p < 0.3))
        )
        gap = (gap_high + gap_low) / 2
        # Map gap to multiplier change (clamped). gap ≈ 0.05 → +10% σ.
        delta = max(-0.10, min(0.10, gap * 2.0))
        new = max(0.6, min(1.5, _SIGMA_MULTIPLIER + delta))
        _SIGMA_MULTIPLIER = new
        with open(_SIGMA_MULTIPLIER_PATH, "w") as f:
            json.dump({"multiplier": new, "n_pairs": len(pairs), "gap": gap}, f)
        return {"multiplier": new, "n_pairs": len(pairs), "gap": gap}
    except Exception as e:
        log.debug("sigma recompute failed: %s", e)
        return None


_load_sigma_multiplier()


def _log_prediction(record: dict) -> None:
    """Append a prediction record for offline calibration analysis.

    Rotates daily — file path is ``predictions-YYYY-MM-DD.jsonl`` so the
    log doesn't grow unbounded. Calibration script globs the directory.
    Schema: ticker, side, fair_prob, mkt_mid, source, ts (UTC iso).
    """
    try:
        from datetime import datetime as _dt, timezone as _tz
        day = _dt.now(_tz.utc).strftime("%Y-%m-%d")
        path = os.path.join(PREDICTIONS_LOG_DIR, f"predictions-{day}.jsonl")
        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")
    except Exception as e:
        log.debug("predictions log append failed: %s", e)


def _metar_get(params: dict, attempts: int = 2, timeout: int = 10):
    """GET aviationweather.gov/metar with one retry on timeout/network err.

    The single-shot 10s timeout was producing ~18 silent failures/day, each
    of which downgraded dead-leg detection to wider NWS-forecast sigma and
    cost real arb fires. One quick retry recovers most transient blips.
    """
    last_exc: Optional[Exception] = None
    for i in range(attempts):
        try:
            resp = requests.get(
                METAR_API, params=params, timeout=timeout,
                headers={"User-Agent": "kalshi-bot/1.0"},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_exc = e
            if i + 1 < attempts:
                time.sleep(0.5)
    if last_exc is not None:
        raise last_exc
    return None


# ── OrderIntent ───────────────────────────────────────────────────────────────

@dataclass
class OrderIntent:
    """What the strategy wants to do. Passed to risk manager then executor."""
    ticker: str
    side: str           # "yes" or "no"
    count: int          # number of contracts
    limit_price: int    # cents (1–99)
    reason: str         # human-readable rationale
    action: str = "buy" # "buy" for entries, "sell" for exits
    basket_id: str = "" # shared across legs of a partition-arb basket so
                        # the executor can reconcile partial fills


# ── Base ──────────────────────────────────────────────────────────────────────

@dataclass
class PositionInfo:
    """Minimal position data needed for exit evaluation."""
    ticker: str
    side: str           # "yes" or "no"
    count: int          # contracts held
    entry_price: int    # average entry price in cents


class BaseStrategy(ABC):
    name: str = "base"

    @abstractmethod
    def evaluate(self, market: CandidateMarket, orderbook: dict) -> Optional[OrderIntent]:
        """Return an OrderIntent if there's an edge, else None."""

    def evaluate_exit(self, position: PositionInfo, market: CandidateMarket, orderbook: dict) -> Optional[OrderIntent]:
        """Return an exit OrderIntent if the position should be closed, else None.
        Default: no exit logic. Override in subclasses."""
        return None

    def evaluate_batch(self, candidates: list[CandidateMarket]) -> list[OrderIntent]:
        """Evaluate multiple markets together for cross-market signals.
        Default: no batch logic. Override in subclasses."""
        return []


# ── FairValueStrategy (weather markets) ───────────────────────────────────────

class FairValueStrategy(BaseStrategy):
    """
    For weather markets: compare an external fair value estimate against the
    market price. If the market is mispriced by more than the edge threshold,
    trade into it.

    Data hierarchy:
      1. METAR (aviation weather observations) — actual readings, tightest edge
      2. NWS forecast — used as fallback when METAR is insufficient
    """
    name = "fair_value"

    NWS_BASE = "https://api.weather.gov"

    # METAR (current observations) is only valid for markets that measure
    # TODAY's weather. For markets resolving tomorrow+, today's temp is
    # irrelevant. 18h is generous enough for markets that close at midnight
    # (same-day) but excludes next-day markets (~24-48h out).
    METAR_MAX_HOURS = 18

    # How long to suppress retries after an NWS forecast fetch fails (seconds).
    # NWS grid coords are stable; a 404 usually means a stale/bad grid cache,
    # and hammering it every 15s produces hundreds of ERROR log lines/hour.
    NWS_FAILURE_TTL = 600

    def __init__(self):
        self._forecast_cache: dict[str, dict] = {}
        self._grid_cache: dict[str, tuple[str, int, int]] = {}
        self._metar_cache: dict[str, dict] = {}
        self._forecast_failures: dict[str, float] = {}
        # GEFS ensemble cache: (city, target_date_iso, is_high) -> (mu_f, sigma_f, ts)
        self._ensemble_cache: dict[tuple, tuple[float, float, float]] = {}
        # Ensemble source-health: consecutive failures + last-fail timestamp
        # so a flapping Open-Meteo doesn't generate hundreds of timeout
        # warnings per loop. Resets on first success.
        self._ensemble_consecutive_failures: int = 0
        self._ensemble_failure_until: float = 0.0
        # NWS observations cache (METAR fallback): icao -> (data, ts)
        self._nws_obs_cache: dict[str, tuple[dict, float]] = {}
        # Freshest-obs cache: icao -> (data, fetch_ts). Populated from the
        # ASOS-aware /observations?limit=N endpoint and consulted only when
        # standard METAR is materially stale (>15 min old).
        self._freshest_obs_cache: dict[str, tuple[dict, float]] = {}
        self._client = None
        self._metrics = None
        # event_ticker -> (open_market_count, timestamp)
        self._event_market_count_cache: dict[str, tuple[list, float]] = {}
        # event_ticker -> unix_ts of last basket-build attempt (for cooldown)
        self._event_attempt_ts: dict[str, float] = {}
        # Tickers we already hold a position in — used to dedupe cert-winner
        # arb across bot restarts (event cooldown lives in memory only).
        self._open_tickers: set[str] = set()
        # Rolling kill-switch state: log of recent settled basket P&Ls
        # and the day-key we're tracking, so consecutive losses can pause
        # new basket emission until the next local day.
        self._basket_outcomes: list[float] = []
        self._kill_switch_day: Optional[str] = None
        self._kill_switch_tripped: bool = False
        # event_ticker -> edge_cents at most-recent basket fire. Used by
        # record_basket_outcome to push a (edge, realized_pnl) pair into
        # the metrics edge-realized dashboard.
        self._basket_edge_at_fire: dict[str, float] = {}
        # Rolling edge-compression tracker: list of (ts, edge_cents) for the
        # last N partition-arb fires. Periodically log the mean so a slow
        # decay (more bots → tighter spreads) is visible without scraping
        # metrics. Capped to avoid unbounded growth across long sessions.
        self._recent_edge_fires: list[tuple[float, float]] = []
        self._recent_edge_last_log_ts: float = 0.0
        # Market-price-guard refusal counter for the NWS dead-leg trim.
        # Increments whenever the trim *would* have killed a leg but the
        # market guard refused. Periodic log surfaces persistent guard
        # firing — could mean our σ/threshold tuning is too aggressive.
        self._nws_trim_market_guard_count: int = 0
        self._nws_trim_guard_last_log_ts: float = 0.0
        self.load_state()

    STATE_PATH = os.environ.get("KALSHI_ARB_STATE_PATH", "arb_state.json")

    def save_state(self) -> None:
        """Persist analytics + kill-switch state across restarts.

        Keeps `_recent_edge_fires`, kill-switch state, and `_basket_outcomes`
        — these reset to empty on cold start otherwise, defeating the
        compression watch and the rolling-loss kill switch.
        """
        try:
            payload = {
                "recent_edge_fires": self._recent_edge_fires[-200:],
                "basket_outcomes": self._basket_outcomes[-50:],
                "kill_switch_day": self._kill_switch_day,
                "kill_switch_tripped": self._kill_switch_tripped,
                "basket_edge_at_fire": self._basket_edge_at_fire,
            }
            tmp = self.STATE_PATH + ".tmp"
            with open(tmp, "w") as f:
                json.dump(payload, f)
            os.replace(tmp, self.STATE_PATH)
        except Exception as e:
            log.debug("arb state save failed: %s", e)

    def load_state(self) -> None:
        try:
            if not os.path.exists(self.STATE_PATH):
                return
            with open(self.STATE_PATH) as f:
                payload = json.load(f) or {}
            self._recent_edge_fires = [
                tuple(x) for x in payload.get("recent_edge_fires", [])
            ]
            self._basket_outcomes = list(payload.get("basket_outcomes", []))
            self._kill_switch_day = payload.get("kill_switch_day")
            self._kill_switch_tripped = bool(payload.get("kill_switch_tripped", False))
            self._basket_edge_at_fire = dict(payload.get("basket_edge_at_fire", {}))
            log.info(
                "Arb state restored: %d recent edges, %d outcomes, kill_switch=%s",
                len(self._recent_edge_fires), len(self._basket_outcomes),
                self._kill_switch_tripped,
            )
        except Exception as e:
            log.warning("arb state load failed: %s", e)

    def set_client(self, client):
        """Inject the Kalshi client so partition completeness can be
        verified against the authoritative event→markets mapping."""
        self._client = client

    def set_metrics(self, metrics):
        """Inject metrics server so strategy can report basket/cert-winner
        attempt + fire counters separately from raw order counts."""
        self._metrics = metrics

    def set_open_tickers(self, tickers):
        """Inject the current set of open position tickers so cert-winner
        arb can skip legs we already hold — the event cooldown is in-memory
        and wouldn't survive a mid-day restart."""
        self._open_tickers = set(tickers or [])

    def record_basket_outcome(self, event_ticker: str, pnl: float) -> None:
        """Feed a settled basket's realized P&L into the kill-switch.

        Called from paper.resolve_expired (and the live-settlement path
        once wired). N consecutive losses > BASKET_KILL_SWITCH_MIN_LOSS
        trips the switch until the next local day rollover.

        Also pushes (edge_cents_at_fire, pnl) into the metrics edge-
        realized dashboard so we can see whether thin-edge baskets
        actually clear after fees + uncovered-bucket losses.
        """
        day = datetime.now().date().isoformat()
        if day != self._kill_switch_day:
            self._kill_switch_day = day
            self._basket_outcomes = []
            self._kill_switch_tripped = False
        self._basket_outcomes.append(pnl)
        edge_cents = self._basket_edge_at_fire.pop(event_ticker, None)
        if edge_cents is not None and self._metrics is not None:
            try:
                self._metrics.push_basket_outcome(edge_cents=edge_cents, pnl=pnl)
            except Exception:
                pass
        recent = self._basket_outcomes[-config.BASKET_KILL_SWITCH_N:]
        if (
            len(recent) >= config.BASKET_KILL_SWITCH_N
            and all(p <= -config.BASKET_KILL_SWITCH_MIN_LOSS for p in recent)
            and not self._kill_switch_tripped
        ):
            self._kill_switch_tripped = True
            log.error(
                "KILL SWITCH TRIPPED: %d consecutive basket losses (%s) — "
                "pausing basket/cert-winner until next day",
                len(recent), ", ".join(f"${p:+.2f}" for p in recent),
            )
        self.save_state()

    def evaluate(self, market: CandidateMarket, orderbook: dict) -> Optional[OrderIntent]:
        # Single-leg fair-value entries are disabled: backtest showed a
        # consistent small bleed (-$6 realized + -$0.00 unrealized across
        # ~20 positions). Partition-arb (evaluate_batch) remains active,
        # and evaluate_exit still handles take-profit / stop-loss on any
        # legacy FV positions still open.
        return None
        if "weather" in market.tags:
            return self._evaluate_weather(market)
        else:
            return self._evaluate_mean_reversion(market)

    def evaluate_exit(self, position: PositionInfo, market: CandidateMarket, orderbook: dict) -> Optional[OrderIntent]:
        """Exit weather positions: take-profit on deep winners, then fair-value flip."""
        if "weather" not in market.tags:
            return None

        # Cert-loser early-out: METAR proves this leg cannot win. Recover
        # whatever salvage value the book still bids rather than holding
        # to $0 settle. Symmetric to the cert-winner take-profit path.
        loser_intent = self._cert_loser_exit_intent(position, market)
        if loser_intent is not None:
            return loser_intent

        tp_intent = self._take_profit_intent(position, market)
        if tp_intent is not None:
            return tp_intent

        series = market.series.upper()
        fair_prob = None
        source = ""
        if series.startswith("KXHIGH") or series.startswith("KXLOW"):
            if market.strike_type == "between":
                fair_prob, source = self._get_temp_range_probability(market)
            else:
                fair_prob, source = self._get_temp_probability(market)
        elif series.startswith("KXRAIN"):
            fair_prob, source = self._get_rain_probability(market)
        elif series.startswith("KXWIND"):
            fair_prob, source = self._get_wind_probability(market)
        elif series.startswith("KXSNOW"):
            fair_prob, source = self._get_snow_probability(market)

        if fair_prob is None:
            return None

        entry_price_frac = position.entry_price / 100

        # Only exit if fair value has dropped meaningfully below entry
        # (not just 1-2% noise). Require at least 10% drop from entry to trigger.
        # Also account for the sell-side taker fee — don't exit if the fee
        # would make the exit worse than holding.
        min_exit_drop = 0.10

        if position.side == "yes":
            sell_price = max(int(market.yes_bid * 100), 1)
            if sell_price < config.MIN_EXIT_PRICE_CENTS:
                return None
            exit_fee = kalshi_fee(position.count, sell_price) / position.count
            if fair_prob < entry_price_frac - min_exit_drop - exit_fee:
                return OrderIntent(
                    ticker=position.ticker, side="yes",
                    count=position.count, limit_price=sell_price,
                    reason=f"EXIT {source}: {fair_prob:.0%} below entry {entry_price_frac:.0%} (fee {exit_fee:.1%})",
                    action="sell",
                )
        else:
            no_fair = 1.0 - fair_prob
            no_bid = 1.0 - market.yes_ask
            sell_price = max(int(no_bid * 100), 1)
            if sell_price < config.MIN_EXIT_PRICE_CENTS:
                return None
            exit_fee = kalshi_fee(position.count, sell_price) / position.count
            if no_fair < entry_price_frac - min_exit_drop - exit_fee:
                return OrderIntent(
                    ticker=position.ticker, side="no",
                    count=position.count, limit_price=sell_price,
                    reason=f"EXIT {source}: NO fair {no_fair:.0%} below entry {entry_price_frac:.0%} (fee {exit_fee:.1%})",
                    action="sell",
                )

        return None

    def _take_profit_intent(self, position: PositionInfo, market: CandidateMarket) -> Optional[OrderIntent]:
        """Close a winning leg when its bid reaches the take-profit threshold.

        Locks in most of the expected payout without holding through
        settlement, where last-minute dislocations and missed fills
        erase gains (see APR16 exits that expired unfilled at 1¢).

        If METAR proves the leg is already a certain winner, we drop the
        threshold all the way down to "any positive net-of-fee gain" —
        the leg's payout is locked, so freeing the capital now to roll
        into the next basket is strictly +EV vs holding to settle.
        """
        if position.side == "yes":
            bid_cents = max(int(market.yes_bid * 100), 1)
        else:
            bid_cents = max(int((1.0 - market.yes_ask) * 100), 1)

        is_cert = self._is_position_certain_winner(position, market)

        if is_cert:
            # Net-of-fee gain check. Any positive number is a recycle win.
            fee_per = kalshi_fee(position.count, bid_cents) / max(position.count, 1)
            net_gain_cents = (bid_cents - position.entry_price) - fee_per
            if net_gain_cents <= 0:
                return None
            reason = (
                f"CERT TAKE PROFIT {position.side.upper()}: bid {bid_cents}c "
                f"(entry {position.entry_price}c, +{net_gain_cents:.1f}¢ net) "
                f"— METAR-locked winner, recycling capital"
            )
        else:
            if bid_cents < config.WEATHER_TAKE_PROFIT_BID_CENTS:
                return None
            gain_cents = bid_cents - position.entry_price
            if gain_cents < config.WEATHER_TAKE_PROFIT_MIN_GAIN_CENTS:
                return None
            reason = (
                f"TAKE PROFIT {position.side.upper()}: bid {bid_cents}c ≥ "
                f"{config.WEATHER_TAKE_PROFIT_BID_CENTS}c "
                f"(entry {position.entry_price}c, +{gain_cents}c)"
            )

        return OrderIntent(
            ticker=position.ticker, side=position.side,
            count=position.count, limit_price=bid_cents,
            reason=reason,
            action="sell",
        )

    def _is_position_certain_winner(
        self, position: PositionInfo, market: CandidateMarket,
    ) -> bool:
        """True if METAR proves this held YES leg will settle at $1.

        Mirror of the cert-winner entry filter: KXHIGH more_than with
        floor < observed_max, KXLOW less_than with cap > observed_min.
        Only YES positions can be cert winners — NO would be cert
        loser, handled separately.
        """
        if position.side != "yes":
            return False
        series = market.series.upper()
        is_high = series.startswith("KXHIGH")
        is_low = series.startswith("KXLOW")
        if not (is_high or is_low):
            return False
        if market.minutes_to_close / 60 > self.METAR_MAX_HOURS:
            return False
        city = self._resolve_city(market)
        if not city:
            return False
        observed = self._get_observed_extreme(city, is_high=is_high)
        if observed is None:
            return False
        st = market.strike_type
        floor = market.raw.get("floor_strike")
        cap = market.raw.get("cap_strike")
        # Symmetric band: only declare cert-winner when observation clears the
        # threshold by more than the ASOS rounding band (otherwise we might
        # take profit on a leg that ultimately settles at $0).
        band = config.METAR_OBSERVATION_BAND_DEGREES
        if is_high and st in ("more_than", "more", "greater", "greater_than"):
            return floor is not None and observed - band > floor
        if is_low and st in ("less_than", "less"):
            return cap is not None and observed + band < cap
        return False

    def _is_position_certain_loser(
        self, position: PositionInfo, market: CandidateMarket,
    ) -> bool:
        """True if METAR proves this held YES leg will settle at $0.

        Inverse of _is_position_certain_winner. Used to trigger early
        salvage exits — once the basket's eventual winner is locked, the
        losing legs still bid 3-10c on liquid books, and that capital is
        better recycled than left to expire at zero.
        """
        if position.side != "yes":
            return False
        series = market.series.upper()
        is_high = series.startswith("KXHIGH")
        is_low = series.startswith("KXLOW")
        if not (is_high or is_low):
            return False
        if market.minutes_to_close / 60 > self.METAR_MAX_HOURS:
            return False
        city = self._resolve_city(market)
        if not city:
            return False
        observed = self._get_observed_extreme(city, is_high=is_high)
        if observed is None:
            return False
        st = market.strike_type
        floor = market.raw.get("floor_strike")
        cap = market.raw.get("cap_strike")
        band = config.METAR_OBSERVATION_BAND_DEGREES
        if is_high:
            if st in ("less_than", "less") and cap is not None and observed - band > cap:
                return True
            if st == "between" and cap is not None and observed - band > cap:
                return True
        else:
            if (
                st in ("more_than", "more", "greater", "greater_than")
                and floor is not None and observed + band < floor
            ):
                return True
            if st == "between" and floor is not None and observed + band < floor:
                return True
        return False

    def _cert_loser_exit_intent(
        self, position: PositionInfo, market: CandidateMarket,
    ) -> Optional[OrderIntent]:
        """Sell a held leg whose outcome is now impossible at any decent bid.

        Floor at MIN_EXIT_PRICE_CENTS — below that fees + zombie-rest risk
        outweigh the salvage value. We accept any net-of-fee positive
        outcome down to a 1¢ realized loss vs holding to $0 (the fee on
        a tiny exit can otherwise make the salvage worse than nothing).
        """
        if not self._is_position_certain_loser(position, market):
            return None
        if position.side != "yes":
            return None
        bid_cents = max(int(market.yes_bid * 100), 0)
        if bid_cents < config.MIN_EXIT_PRICE_CENTS:
            return None
        fee_per = kalshi_fee(position.count, max(1, bid_cents)) / max(position.count, 1)
        if bid_cents - fee_per <= 0:
            return None
        reason = (
            f"CERT LOSER {position.ticker}: METAR rules out, "
            f"salvaging {bid_cents}c bid (fee {fee_per:.1f}¢)"
        )
        log.info(reason)
        return OrderIntent(
            ticker=position.ticker, side=position.side,
            count=position.count, limit_price=bid_cents,
            reason=reason, action="sell",
        )

    def _evaluate_mean_reversion(self, market: CandidateMarket) -> Optional[OrderIntent]:
        """No model = no edge = no trade. Refuse to gamble on unknown markets."""
        log.debug("%s — no fair value model, skipping (no-edge-no-trade policy)", market.ticker)
        return None

    def _evaluate_weather(self, market: CandidateMarket) -> Optional[OrderIntent]:
        """Compare METAR/NWS fair value against Kalshi implied probability.

        Tries METAR observations first (actual readings), falls back to NWS
        forecasts. Covers KXHIGH, KXLOW, KXRAIN, KXWIND, KXSNOW.

        Temperature markets come in two shapes:
          - `more_than`/`less_than`: single threshold (>X or <X) — handled
            by _get_temp_probability
          - `between`: range bucket [a, b] — handled by
            _get_temp_range_probability (models P(final extreme ∈ [a,b]))
        """
        series = market.series.upper()

        fair_prob = None
        source = ""
        if series.startswith("KXHIGH") or series.startswith("KXLOW"):
            if market.strike_type == "between":
                fair_prob, source = self._get_temp_range_probability(market)
            else:
                fair_prob, source = self._get_temp_probability(market)
        elif series.startswith("KXRAIN"):
            fair_prob, source = self._get_rain_probability(market)
        elif series.startswith("KXWIND"):
            fair_prob, source = self._get_wind_probability(market)
        elif series.startswith("KXSNOW"):
            fair_prob, source = self._get_snow_probability(market)
        else:
            return None

        if fair_prob is None:
            log.debug("%s — no signal from METAR or NWS", market.ticker)
            return None

        # Reject uncertain model estimates. If our model says 40-60%,
        # we're basically guessing — the edge is noise, not signal.
        # Only trade when the model is directionally confident.
        if 0.20 < fair_prob < 0.80 and source == "METAR":
            log.debug("%s — METAR P=%.0f%% in uncertain zone (20-80%%), skipping",
                      market.ticker, fair_prob * 100)
            return None

        mid = market.mid
        edge = fair_prob - mid

        # Tiered confidence: METAR is direct observation (higher confidence),
        # NWS is a public forecast on a thin market (lower confidence, need
        # bigger edge to overcome model uncertainty). Late in the day the
        # signal is more reliable so the thresholds relax.
        late = market.minutes_to_close <= config.WEATHER_LATE_WINDOW_MINUTES
        if source == "NWS":
            threshold = (config.WEATHER_NWS_EDGE_THRESHOLD_LATE if late
                         else config.WEATHER_NWS_EDGE_THRESHOLD)
        else:
            threshold = (config.WEATHER_EDGE_THRESHOLD_LATE if late
                         else config.WEATHER_EDGE_THRESHOLD)

        # Compute the price we'd pay and reject if too expensive
        if edge > 0:
            price_cents = min(int(market.yes_ask * 100) + 1, 99)
        else:
            no_ask = 1.0 - market.yes_bid
            price_cents = min(int(no_ask * 100) + 1, 99)

        # Deduct estimated taker fee from edge before threshold check
        fee_dollars = kalshi_fee(1, price_cents)
        net_edge = abs(edge) - fee_dollars
        if net_edge < threshold:
            log.debug("%s — weather net edge %.1f¢ (%.1f¢ raw - %.1f¢ fee) below %s threshold %.0f¢",
                      market.ticker, net_edge * 100, abs(edge) * 100, fee_dollars * 100,
                      source, threshold * 100)
            return None

        # Between buckets are narrow 2°F ranges — model uncertainty is highest.
        # Use a lower price cap to avoid expensive bets on tight ranges. Near
        # close, observed temp has mostly crystallized so we raise the cap.
        if market.strike_type == "between":
            late = market.minutes_to_close <= config.WEATHER_LATE_WINDOW_MINUTES
            max_price = (config.WEATHER_MAX_BETWEEN_PRICE_LATE_CENTS if late
                         else config.WEATHER_MAX_BETWEEN_PRICE_CENTS)
        else:
            max_price = config.WEATHER_MAX_PRICE_CENTS
        if price_cents > max_price:
            log.debug("%s — price %dc exceeds max %dc (%s), skipping",
                     market.ticker, price_cents, max_price, market.strike_type)
            return None

        # Size proportional to edge, but conservative.
        # NWS signals get half the base size — lower confidence.
        # Weather bets are model-dependent (not arbs), so cap at DEFAULT_ORDER_SIZE.
        base = config.DEFAULT_ORDER_SIZE if source != "NWS" else max(1, config.DEFAULT_ORDER_SIZE // 2)
        size = max(config.MIN_ORDER_SIZE, min(
            int(base * sqrt(net_edge / threshold)),
            config.DEFAULT_ORDER_SIZE,  # hard cap — weather bets aren't arbs
        ))

        side = "yes" if edge > 0 else "no"
        _log_prediction({
            "ts": datetime.now(timezone.utc).isoformat(),
            "ticker": market.ticker,
            "series": market.series,
            "side": side,
            "fair_prob": round(fair_prob, 4),
            "mkt_mid": round(mid, 4),
            "price_cents": price_cents,
            "size": size,
            "source": source,
            "close_time": market.close_time.isoformat() if market.close_time else None,
        })
        if side == "yes":
            return OrderIntent(
                ticker=market.ticker, side="yes",
                count=size,
                limit_price=price_cents,
                reason=f"Weather {source}: P={fair_prob:.0%} vs mkt {mid:.0%} (edge {abs(edge):.1%})",
            )
        else:
            return OrderIntent(
                ticker=market.ticker, side="no",
                count=size,
                limit_price=price_cents,
                reason=f"Weather {source}: P={fair_prob:.0%} vs mkt {mid:.0%} (fade YES, edge {abs(edge):.1%})",
            )

    # ── NWS helpers ──────────────────────────────────────────────────────────

    def _resolve_city(self, market: CandidateMarket) -> Optional[str]:
        """Extract city name from series, ticker, or title.

        Real Kalshi market titles for KXHIGHTLV / KXHIGHTOKC / KXHIGHTEMPDEN
        omit the city entirely ("Will the maximum temperature be <75°…"),
        so the series prefix is the only reliable resolver. We try in order:
          1. SERIES_CITY_MAP keyed on the series ticker
          2. Legacy ticker-split (for fake markets that put city as a dash segment)
          3. Title-alias word-boundary match (for KXHIGHNY-style "high temp in NYC")
        """
        # 1. Series-prefix lookup (the only thing that works for real
        # KXHIGHTLV/KXHIGHTOKC/etc. markets)
        series_upper = market.series.upper()
        if series_upper in SERIES_CITY_MAP:
            return SERIES_CITY_MAP[series_upper]

        # 2. Legacy fake-ticker path: city as its own dash segment
        parts = market.ticker.upper().split("-")
        for part in parts:
            if part in TICKER_CITY_MAP:
                return TICKER_CITY_MAP[part]

        # 3. Title aliases (covers "NYC", "Vegas", full names, …).
        # Use word-boundary regex to avoid e.g. "NY" matching inside "Albany".
        title_lower = market.title.lower()
        for alias, canonical in CITY_ALIASES:
            if re.search(rf"\b{re.escape(alias)}\b", title_lower):
                return canonical

        return None

    def _resolve_nws_grid(self, city: str) -> Optional[tuple[str, int, int]]:
        """Look up NWS grid (office, gridX, gridY) for a city. Results are cached."""
        if city in self._grid_cache:
            return self._grid_cache[city]

        coords = CITY_COORDS.get(city)
        if not coords:
            log.warning("No coordinates for city '%s'", city)
            return None

        lat, lon = coords
        url = f"{self.NWS_BASE}/points/{lat},{lon}"
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "kalshi-bot/1.0"})
            resp.raise_for_status()
            props = resp.json().get("properties", {})
            office = props.get("gridId")
            grid_x = props.get("gridX")
            grid_y = props.get("gridY")
            if not office or grid_x is None or grid_y is None:
                log.warning("NWS /points response missing grid data for %s", city)
                return None
            result = (office, int(grid_x), int(grid_y))
            self._grid_cache[city] = result
            log.info("Resolved NWS grid for %s: %s %d,%d", city, *result)
            return result
        except Exception as e:
            log.error("NWS grid resolution failed for %s: %s", city, e)
            return None

    def _resolve_forecast_sigma(
        self,
        city: Optional[str],
        target_date,
        is_high: bool,
        nws_mu: float,
        hours_to_close: float,
    ) -> tuple[Optional[float], str, Optional[float]]:
        """Pick σ for the (μ_NWS, target_date, city) tuple.

        Combines three signals, in priority order:
          1. GEFS ensemble σ (measured from member spread)
          2. Disagreement penalty: if GEFS μ disagrees with NWS μ by more
             than the measured σ, inflate σ via quadrature with the gap.
             High forecast disagreement → wider σ → fewer entries → fail-closed.
          3. Lead-time floor: even an over-tight ensemble can't credibly
             claim σ below ~1°F + 0.025°F/hour-out.

        Returns (sigma_used, source_label, ens_mu_or_None).
        """
        ens_mu, ens_sigma = (None, None)
        if city is not None and target_date is not None:
            ens_mu, ens_sigma = self._get_ensemble_sigma(city, target_date, is_high)

        if ens_sigma is not None:
            sigma = ens_sigma
            source = "NWS+GEFS"
            if ens_mu is not None:
                gap = abs(float(ens_mu) - float(nws_mu))
                if gap > config.MAX_FORECAST_DISAGREEMENT_F:
                    log.warning(
                        "Forecast disagreement %s exceeds veto threshold: "
                        "NWS μ=%.1f, GEFS μ=%.1f, gap=%.1f°F > %.1f°F — skipping",
                        city, nws_mu, ens_mu, gap, config.MAX_FORECAST_DISAGREEMENT_F,
                    )
                    return None, "VETO", ens_mu
                if gap > sigma:
                    inflated = math.sqrt(sigma * sigma + gap * gap)
                    log.info(
                        "Forecast disagreement %s: NWS μ=%.1f, GEFS μ=%.1f, gap=%.1f → σ %.1f→%.1f",
                        city, nws_mu, ens_mu, gap, sigma, inflated,
                    )
                    sigma = inflated
                    source = "NWS+GEFS+DISAGREE"
        else:
            sigma = config.NWS_SIGMA_DEGREES
            source = "NWS"

        floor = lead_time_sigma_floor(hours_to_close)
        if sigma < floor:
            sigma = floor
            source = source + "+FLOOR"
        # Apply online recalibration multiplier (persisted across runs)
        if _SIGMA_MULTIPLIER != 1.0:
            sigma *= _SIGMA_MULTIPLIER
        return sigma, source, ens_mu

    def _get_ensemble_sigma(
        self, city: str, target_date, is_high: bool,
    ) -> tuple[Optional[float], Optional[float]]:
        """Return (mu_f, sigma_f) in °F derived from GEFS member spread.

        Replaces the hardcoded NWS_SIGMA_DEGREES with a *measured* spread
        per city per lead-time. 31 GEFS members at 0.25° resolution, free
        via Open-Meteo's NOAA mirror — no API key, no GRIB parsing.

        Returns (None, None) on any failure; callers must fall back to the
        constant σ. Fail-closed: if we can't measure σ, don't pretend.
        """
        coords = CITY_COORDS.get(city)
        if not coords:
            return None, None
        try:
            target_iso = target_date.isoformat() if hasattr(target_date, "isoformat") else str(target_date)
        except Exception:
            return None, None
        cache_key = (city, target_iso, is_high)
        now = time.time()
        cached = self._ensemble_cache.get(cache_key)
        if cached and now - cached[2] < ENSEMBLE_CACHE_SECONDS:
            return cached[0], cached[1]
        # Source-health throttle: after repeated failures, stop hitting
        # Open-Meteo until the cooldown elapses. Fail-closed — callers will
        # use NWS_SIGMA_DEGREES, which is wider than typical ensemble σ.
        if now < self._ensemble_failure_until:
            return None, None
        try:
            lat, lon = coords
            field = "temperature_2m_max" if is_high else "temperature_2m_min"
            resp = requests.get(
                ENSEMBLE_API,
                params={
                    "latitude": lat, "longitude": lon,
                    "daily": field,
                    "models": "gfs025",
                    "temperature_unit": "fahrenheit",
                    "forecast_days": 10,
                    "timezone": "auto",
                },
                timeout=10,
                headers={"User-Agent": "kalshi-bot/1.0"},
            )
            resp.raise_for_status()
            data = resp.json()
            daily = data.get("daily", {})
            times = daily.get("time", [])
            if target_iso not in times:
                return None, None
            idx = times.index(target_iso)
            members: list[float] = []
            for k, v in daily.items():
                if not k.startswith(field):
                    continue
                if not isinstance(v, list) or len(v) <= idx:
                    continue
                val = v[idx]
                if val is None:
                    continue
                members.append(float(val))
            if len(members) < 5:
                return None, None
            mu = sum(members) / len(members)
            var = sum((x - mu) ** 2 for x in members) / len(members)
            sigma = math.sqrt(var)
            sigma = max(ENSEMBLE_MIN_SIGMA_F, min(sigma, ENSEMBLE_MAX_SIGMA_F))
            self._ensemble_cache[cache_key] = (mu, sigma, now)
            self._ensemble_consecutive_failures = 0
            log.info(
                "GEFS %s %s %s: μ=%.1f°F σ=%.1f°F (n=%d members)",
                city, "max" if is_high else "min", target_iso,
                mu, sigma, len(members),
            )
            return mu, sigma
        except Exception as e:
            self._ensemble_consecutive_failures += 1
            if self._ensemble_consecutive_failures >= ENSEMBLE_MAX_CONSECUTIVE_FAILURES:
                self._ensemble_failure_until = now + ENSEMBLE_FAILURE_TTL
                log.warning(
                    "Ensemble fetch failed %dx in a row — backing off for %ds",
                    self._ensemble_consecutive_failures, ENSEMBLE_FAILURE_TTL,
                )
            else:
                log.warning("Ensemble fetch failed for %s: %s", city, e)
            return None, None

    def _get_freshest_obs(self, city: str) -> Optional[dict]:
        """Fetch the most recent station observation regardless of source.

        ASOS stations report SPECI (special) observations on 5-min cadence
        when conditions warrant; standard METAR is hourly. NWS's
        observations endpoint exposes both — pulling the most recent N and
        picking the freshest valid temp gets us 5-15 min freshness vs
        ~60 min from aviationweather.gov.

        Returns a METAR-shaped dict (`temp` °C, `_obs_time_utc` epoch sec)
        or None on failure. Caller should compare timestamp against the
        cached METAR's obsTime and prefer this when materially newer.
        """
        icao = CITY_ICAO.get(city)
        if not icao:
            return None
        now = time.time()
        # Use the same NWS-obs cache window — freshest obs and "latest"
        # endpoint queries share the rate-limit budget.
        cached = self._freshest_obs_cache.get(icao)
        if cached and now - cached[1] < METAR_CACHE_SECONDS / 2:  # tighter cache
            return cached[0]
        try:
            url = f"{self.NWS_BASE}/stations/{icao}/observations?limit=12"
            resp = requests.get(
                url, timeout=10,
                headers={"User-Agent": "kalshi-bot/1.0", "Accept": "application/geo+json"},
            )
            resp.raise_for_status()
            features = resp.json().get("features") or []
            best = None
            best_ts = 0.0
            for feat in features:
                props = (feat or {}).get("properties") or {}
                temp = (props.get("temperature") or {}).get("value")
                obs_time = props.get("timestamp")
                if temp is None or not obs_time:
                    continue
                try:
                    ts = datetime.fromisoformat(obs_time.replace("Z", "+00:00")).timestamp()
                except Exception:
                    continue
                if ts > best_ts:
                    best_ts = ts
                    best = {"temp": temp, "_obs_time_utc": ts, "_source": "NWS_FRESH"}
            if best is None:
                return None
            self._freshest_obs_cache[icao] = (best, now)
            return best
        except Exception as e:
            log.debug("Freshest-obs fetch failed for %s: %s", icao, e)
            return None

    def _get_nws_observation(self, city: str) -> Optional[dict]:
        """Fetch the latest NWS station observation for a city.

        Used as a METAR fallback when aviationweather.gov is timing out.
        Returns a dict shaped like METAR (`temp` in °C, `maxT`/`minT` in °C
        when the 24h fields are populated) so callers can treat it
        interchangeably.
        """
        icao = CITY_ICAO.get(city)
        if not icao:
            return None
        now = time.time()
        cached = self._nws_obs_cache.get(icao)
        if cached and now - cached[1] < METAR_CACHE_SECONDS:
            return cached[0]
        try:
            url = f"{self.NWS_BASE}/stations/{icao}/observations/latest"
            resp = requests.get(
                url, timeout=10,
                headers={"User-Agent": "kalshi-bot/1.0", "Accept": "application/geo+json"},
            )
            resp.raise_for_status()
            data = resp.json()
            props = data.get("properties", {}) or {}
            temp = (props.get("temperature") or {}).get("value")
            max24 = (props.get("maxTemperatureLast24Hours") or {}).get("value")
            min24 = (props.get("minTemperatureLast24Hours") or {}).get("value")
            if temp is None and max24 is None and min24 is None:
                return None
            obs = {
                "temp": temp,           # °C
                "maxT": max24,          # °C, may be None
                "minT": min24,          # °C, may be None
                "_source": "NWS_OBS",
                "_fetched_at": now,
            }
            self._nws_obs_cache[icao] = (obs, now)
            return obs
        except Exception as e:
            log.warning("NWS observation fetch failed for %s: %s", icao, e)
            return None

    def _get_forecast(self, market: CandidateMarket) -> Optional[dict]:
        """Fetch NWS forecast for the city in a market. Dynamically resolves the grid."""
        city = self._resolve_city(market)
        if not city:
            log.debug("Could not determine city for %s", market.ticker)
            return None

        cache_key = city
        if cache_key in self._forecast_cache:
            return self._forecast_cache[cache_key]

        now = time.time()
        last_fail = self._forecast_failures.get(cache_key)
        if last_fail is not None and now - last_fail < self.NWS_FAILURE_TTL:
            return None

        grid = self._resolve_nws_grid(city)
        if not grid:
            self._forecast_failures[cache_key] = now
            return None

        office, grid_x, grid_y = grid
        url = f"{self.NWS_BASE}/gridpoints/{office}/{grid_x},{grid_y}/forecast"
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "kalshi-bot/1.0"})
            resp.raise_for_status()
            data = resp.json()
            self._forecast_cache[cache_key] = data
            self._forecast_failures.pop(cache_key, None)
            return data
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 404:
                # Bad grid cache — drop it so next attempt re-resolves
                self._grid_cache.pop(city, None)
            self._forecast_failures[cache_key] = now
            log.error("NWS forecast fetch failed for %s: %s", city, e)
            return None
        except Exception as e:
            self._forecast_failures[cache_key] = now
            log.error("NWS forecast fetch failed for %s: %s", city, e)
            return None

    # ── METAR helpers ─────────────────────────────────────────────────────────

    def _get_metar(self, city: str) -> Optional[dict]:
        """Fetch latest METAR observation for a city's airport."""
        icao = CITY_ICAO.get(city)
        if not icao:
            return None

        now = time.time()
        cached = self._metar_cache.get(icao)
        if cached and (now - cached["_fetched_at"]) < METAR_CACHE_SECONDS:
            return cached

        try:
            data = _metar_get({"ids": icao, "format": "json"})
            if not data or not isinstance(data, list):
                log.warning("METAR empty response for %s — trying NWS observation fallback", icao)
                fallback = self._get_nws_observation(city)
                if fallback is not None:
                    self._metar_cache[icao] = fallback
                return fallback
            metar = data[0]
            metar["_fetched_at"] = now
            # Prefer ASOS 5-min freshest obs when METAR observation is
            # >15 min stale. The KXHIGH/KXLOW markets close on local-day
            # boundaries, so the last 30 min before close is exactly when
            # this matters most. Override the temperature only — keep
            # METAR's maxT/minT/wind/wx fields, which the latest obs lacks.
            obs_time_str = metar.get("obsTime") or metar.get("reportTime")
            metar_obs_ts = None
            if obs_time_str:
                try:
                    metar_obs_ts = datetime.fromisoformat(
                        str(obs_time_str).replace("Z", "+00:00")
                    ).timestamp()
                except Exception:
                    pass
            metar_age_sec = (now - metar_obs_ts) if metar_obs_ts else 0
            if metar_age_sec > 15 * 60:
                fresh = self._get_freshest_obs(city)
                if fresh and fresh.get("_obs_time_utc", 0) > (metar_obs_ts or 0) + 60:
                    log.info(
                        "%s: METAR temp %.1fC age %.0fmin → using fresher obs %.1fC",
                        icao, metar.get("temp", 0), metar_age_sec / 60, fresh["temp"],
                    )
                    metar["temp"] = fresh["temp"]
                    metar["_temp_source"] = "NWS_FRESH"
            self._metar_cache[icao] = metar
            temp_f = metar.get("temp", 0) * 9 / 5 + 32
            log.info(
                "METAR %s: %.1f°F (%.1f°C) wind %skt wx=%s",
                icao, temp_f, metar.get("temp", 0),
                metar.get("wspd", 0),
                metar.get("wxString", "clear"),
            )
            # Cross-check against NWS observation. _get_nws_observation is
            # cache-gated so this is a no-op within METAR_CACHE_SECONDS, but
            # it ensures the cross-check actually fires (otherwise NWS-obs
            # only loads as a fallback and the comparison stays dead).
            self._get_nws_observation(city)
            cached_obs = self._nws_obs_cache.get(icao)
            if cached_obs and (now - cached_obs[1]) < METAR_CACHE_SECONDS:
                obs_temp_c = cached_obs[0].get("temp")
                metar_temp_c = metar.get("temp")
                if obs_temp_c is not None and metar_temp_c is not None:
                    obs_temp_f = obs_temp_c * 9 / 5 + 32
                    metar_temp_f = metar_temp_c * 9 / 5 + 32
                    delta = abs(metar_temp_f - obs_temp_f)
                    if delta > config.METAR_OBSERVATION_BAND_DEGREES * 2:
                        log.warning(
                            "METAR↔NWS-obs disagree for %s: METAR=%.1f°F NWS=%.1f°F Δ=%.1f°F",
                            icao, metar_temp_f, obs_temp_f, delta,
                        )
            return metar
        except Exception as e:
            log.warning("METAR fetch failed for %s: %s — trying NWS observation fallback", icao, e)
            fallback = self._get_nws_observation(city)
            if fallback is not None:
                self._metar_cache[icao] = fallback
            return fallback

    def _get_observed_extreme(self, city: str, is_high: bool) -> Optional[float]:
        """Return observed daily max (is_high=True) or min (is_high=False) in °F
        from the past ~24h of METARs at the city's airport. Cached 15min.

        Used to identify partition legs whose outcome is already impossible:
        for a KXHIGH market, once observed_max exceeds a bucket's upper bound,
        that bucket can never win (max is monotonically non-decreasing). Same
        logic inverted for KXLOW.
        """
        icao = CITY_ICAO.get(city)
        tz_name = CITY_TZ.get(city)
        if not icao or not tz_name:
            return None
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            return None
        today_local = datetime.now(tz).date()
        # Cache key includes the local date so entries expire at local midnight
        # even if the 15min time-based cache window straddles it.
        cache_key = (icao, "max" if is_high else "min", today_local.isoformat())
        now = time.time()
        if not hasattr(self, "_extreme_cache"):
            self._extreme_cache: dict = {}
        cached = self._extreme_cache.get(cache_key)
        if cached and (now - cached[1]) < METAR_CACHE_SECONDS:
            return cached[0]
        try:
            data = _metar_get({"ids": icao, "format": "json", "hours": 30})
            temps_f: list[float] = []
            for m in data:
                obs_ts = m.get("obsTime")
                if obs_ts is None:
                    continue
                try:
                    obs_local = datetime.fromtimestamp(
                        int(obs_ts), tz=timezone.utc,
                    ).astimezone(tz)
                except (OSError, ValueError, OverflowError):
                    continue
                if obs_local.date() != today_local:
                    continue
                if m.get("temp") is not None:
                    temps_f.append(m["temp"] * 9 / 5 + 32)
                # ASOS reports 6-hour max/min in °C. The official daily high
                # NWS uses comes from 1-min averages — the 5-min METAR temp
                # alone systematically under-reports peaks. The 6h max field,
                # when present, is the closest real-time signal we have to
                # the actual high. Mix it into the candidate set.
                if is_high and m.get("maxT") is not None:
                    temps_f.append(m["maxT"] * 9 / 5 + 32)
                if (not is_high) and m.get("minT") is not None:
                    temps_f.append(m["minT"] * 9 / 5 + 32)
            if not temps_f:
                return None
            val = max(temps_f) if is_high else min(temps_f)
            self._extreme_cache[cache_key] = (val, now)
            return val
        except Exception as e:
            log.warning("METAR local-day fetch failed for %s: %s", icao, e)
            return None

    def _observably_dead_legs(
        self, event_ticker: str, legs: list[CandidateMarket],
    ) -> set[str]:
        """Return tickers whose YES payout is already impossible per METAR.

        KXHIGH (daily max): dead if leg's upper bound < observed_max.
        KXLOW  (daily min): dead if leg's lower bound > observed_min.
        Both are monotonic intra-day (max only rises, min only falls), so a
        leg that's dead now stays dead through settle.
        """
        if not legs or not config.PARTITION_ARB_SKIP_DEAD_LEGS:
            return set()
        sample = legs[0]
        series = sample.series.upper()
        is_high = series.startswith("KXHIGH")
        is_low = series.startswith("KXLOW")
        if not (is_high or is_low):
            return set()
        # Today's METAR is only informative for markets settling today.
        if sample.minutes_to_close / 60 > self.METAR_MAX_HOURS:
            return set()
        city = self._resolve_city(sample)
        if not city:
            return set()
        observed = self._get_observed_extreme(city, is_high=is_high)
        if observed is None:
            return set()
        # ASOS rounding gives observed up to ~1°F of error. Only kill a leg
        # when the observation clears its bucket bound by more than the band —
        # otherwise we risk dropping a leg that's actually the eventual winner
        # (e.g. METAR=96.0 from 35°C round-trip when truth is 95.4°F).
        band = config.METAR_OBSERVATION_BAND_DEGREES
        dead: set[str] = set()
        for m in legs:
            st = m.strike_type
            floor = m.raw.get("floor_strike")
            cap = m.raw.get("cap_strike")
            if is_high:
                # less_than / between: upper bound is `cap`. Dead only if
                # observed_max - band > cap (true high definitely above cap).
                if st in ("less_than", "less") and cap is not None and observed - band > cap:
                    dead.add(m.ticker)
                elif st == "between" and cap is not None and observed - band > cap:
                    dead.add(m.ticker)
            else:
                # more_than / between: lower bound is `floor`. Dead only if
                # observed_min + band < floor (true low definitely below floor).
                if (
                    st in ("more_than", "more", "greater", "greater_than")
                    and floor is not None and observed + band < floor
                ):
                    dead.add(m.ticker)
                elif st == "between" and floor is not None and observed + band < floor:
                    dead.add(m.ticker)
        if dead:
            log.info(
                "%s: METAR %s=%.1f°F kills %d leg(s): %s",
                event_ticker, "max" if is_high else "min", observed,
                len(dead), ",".join(sorted(dead)),
            )
        return dead

    def _probably_dead_legs_nws(
        self, event_ticker: str, legs: list[CandidateMarket],
    ) -> set[str]:
        """Return tickers whose NWS-implied P(win) is below the dead threshold.

        Extends `_observably_dead_legs` past the METAR_MAX_HOURS horizon
        using the same probabilistic model the FairValue strategy already
        uses for fair-value pricing. NWS sigma at 24-48h is ~5°F, so a
        leg whose midpoint sits 2σ outside the forecast already prices
        below 5%. Trimming those legs from the basket is conservative —
        we still hold every leg with non-trivial win probability — but
        cuts cost meaningfully on big-tail partitions.

        Skipped when METAR is in-range (the observation-based filter is
        strictly stronger), and when hours-to-close exceeds the NWS
        horizon (forecast sigma blows up beyond 48h).
        """
        if not legs:
            return set()
        sample = legs[0]
        series = sample.series.upper()
        if not (series.startswith("KXHIGH") or series.startswith("KXLOW")):
            return set()
        hours_left = sample.minutes_to_close / 60
        # Inside the METAR window, observably-dead is strictly better;
        # don't double-trim with a probabilistic guess.
        if hours_left <= self.METAR_MAX_HOURS:
            return set()
        if hours_left > config.PARTITION_ARB_NWS_DEAD_LEG_HOURS:
            return set()

        threshold = config.PARTITION_ARB_NWS_DEAD_LEG_PROB
        max_mkt = config.PARTITION_ARB_NWS_DEAD_LEG_MAX_MKT_PRICE
        dead: set[str] = set()
        for m in legs:
            st = m.strike_type
            if st == "between":
                p, source = self._get_temp_range_probability(m)
            else:
                # Tail markets — _get_temp_probability returns
                # P(temp >= threshold). For more_than that's the win
                # probability directly; for less_than the win prob is
                # 1 − that.
                p, source = self._get_temp_probability(m)
                if p is not None and st in ("less_than", "less"):
                    p = 1.0 - p
            # Source must be a forecast-derived label (NWS, NWS+GEFS, …).
            # The disagreement-veto path returns ("", None) which we skip.
            if p is None or not source.startswith("NWS"):
                continue
            if p >= threshold:
                continue
            # Market-price sanity check: if the orderbook is pricing this
            # leg above max_mkt, the market disagrees with our "dead"
            # claim. Trust the market and refuse to trim — fail closed.
            mkt_price = m.yes_ask if m.yes_ask is not None else m.mid
            if mkt_price is not None and mkt_price > max_mkt:
                log.info(
                    "%s: NWS-trim refused %s — model P=%.1f%% but mkt ask=%.0f¢ > %.0f¢ guard",
                    event_ticker, m.ticker, p * 100,
                    mkt_price * 100, max_mkt * 100,
                )
                self._nws_trim_market_guard_count += 1
                now_g = time.time()
                if now_g - self._nws_trim_guard_last_log_ts > 3600:
                    self._nws_trim_guard_last_log_ts = now_g
                    log.warning(
                        "NWS-trim market guard fired %d times since last hourly summary",
                        self._nws_trim_market_guard_count,
                    )
                    self._nws_trim_market_guard_count = 0
                continue
            dead.add(m.ticker)
        if dead:
            log.info(
                "%s: NWS forecast kills %d leg(s) at %.1fh out: %s",
                event_ticker, len(dead), hours_left, ",".join(sorted(dead)),
            )
        return dead

    # ── Probability helpers ───────────────────────────────────────────────────

    def _get_temp_probability(
        self, market: CandidateMarket,
    ) -> tuple[Optional[float], str]:
        """P(actual temp exceeds threshold). Tries METAR first, NWS fallback.

        For KXHIGH: if observed temp already >= threshold → P=0.99 (settled).
        For KXLOW:  if observed temp already <= threshold → P=0.01 (settled).
        Otherwise uses normal CDF with time-based uncertainty (METAR: ±1-3°F,
        NWS: ±3°F).
        """
        threshold = self._extract_temp_threshold(market.title)
        if threshold is None:
            return None, ""

        is_high = market.series.upper().startswith("KXHIGH")
        city = self._resolve_city(market)
        hours_left = market.minutes_to_close / 60

        # --- METAR: actual observation, tightest uncertainty ---
        # Only valid when the market measures TODAY's weather. For tomorrow+,
        # today's METAR is irrelevant — fall through to NWS forecast.
        if city and hours_left <= self.METAR_MAX_HOURS:
            metar = self._get_metar(city)
            if metar and metar.get("temp") is not None:
                temp_c = metar["temp"]
                temp_f = temp_c * 9 / 5 + 32

                if is_high:
                    max_c = metar.get("maxT")
                    max_f = (
                        max_c * 9 / 5 + 32
                        if max_c is not None else temp_f
                    )
                    observed_high = max(temp_f, max_f)
                    if observed_high >= threshold:
                        log.info(
                            "%s — METAR: high %.1f°F ≥ %.0f°F → P=99%%",
                            market.ticker, observed_high, threshold,
                        )
                        return 0.99, "METAR"
                else:
                    min_c = metar.get("minT")
                    min_f = (
                        min_c * 9 / 5 + 32
                        if min_c is not None else temp_f
                    )
                    observed_low = min(temp_f, min_f)
                    if observed_low <= threshold:
                        log.info(
                            "%s — METAR: low %.1f°F ≤ %.0f°F → P=1%%",
                            market.ticker, observed_low, threshold,
                        )
                        return 0.01, "METAR"

                # Not yet settled — METAR temp + time-based sigma. Daily
                # extremes are Gumbel-distributed (not Gaussian); using
                # Gumbel keeps tail mass in the right shape, especially
                # for the >threshold question on KXHIGH.
                sigma = max(3.0, min(hours_left * 0.5, 5.0))
                sigma = max(sigma, lead_time_sigma_floor(hours_left))
                sigma *= diurnal_sigma_factor(
                    market.close_time, CITY_TZ.get(city), is_high,
                )
                sigma = max(sigma, 0.5)
                if is_high:
                    prob = 1.0 - gumbel_cdf_max(threshold, temp_f, sigma)
                else:
                    prob = gumbel_cdf_min(threshold, temp_f, sigma)
                log.info(
                    "%s — METAR: %.1f°F vs %.0f°F σ=%.1f P=%.0f%% (Gumbel)",
                    market.ticker, temp_f, threshold, sigma,
                    prob * 100,
                )
                return prob, "METAR"

        # --- NWS forecast fallback (used for tomorrow+ and when METAR is unavailable) ---
        forecast = self._get_forecast(market)
        if not forecast:
            return None, ""

        nws_temp = self._extract_nws_temp(
            forecast, market.close_time, is_high=is_high,
        )
        if nws_temp is None:
            return None, ""

        try:
            target_date = market.close_time.date()
        except Exception:
            target_date = None
        sigma, source, _ = self._resolve_forecast_sigma(
            city, target_date, is_high, float(nws_temp), hours_left,
        )
        if sigma is None:
            return None, ""
        if is_high:
            prob = 1.0 - gumbel_cdf_max(threshold, float(nws_temp), sigma)
        else:
            prob = gumbel_cdf_min(threshold, float(nws_temp), sigma)
        return prob, source

    def _get_temp_range_probability(
        self, market: CandidateMarket,
    ) -> tuple[Optional[float], str]:
        """P(daily extreme ∈ [a, b]) for range / between markets.

        Uses Kalshi's authoritative floor_strike/cap_strike to define the
        bucket bounds, with fallback to title regex for legacy fake markets.

        Model:
          - μ = current METAR temp (center of the remaining-day distribution)
          - σ = time-based uncertainty (3–5°F, shrinks as market approaches close)
          - P = Φ((b − μ)/σ) − Φ((a − μ)/σ) for the general case
          - Early-elimination shortcuts when observed extreme already rules
            out the bucket (METAR maxT/minT)
        """
        # --- Parse bucket bounds ---
        a, b = self._extract_range_bounds(market)
        if a is None or b is None:
            return None, ""

        is_high = market.series.upper().startswith("KXHIGH")
        city = self._resolve_city(market)
        hours_left = market.minutes_to_close / 60

        # --- METAR primary (same-day markets only) ---
        if city and hours_left <= self.METAR_MAX_HOURS:
            metar = self._get_metar(city)
            if metar and metar.get("temp") is not None:
                temp_c = metar["temp"]
                temp_f = temp_c * 9 / 5 + 32
                sigma = max(3.0, min(hours_left * 0.5, 5.0))
                sigma = max(sigma, lead_time_sigma_floor(hours_left))
                sigma *= diurnal_sigma_factor(
                    market.close_time, CITY_TZ.get(city), is_high,
                )
                sigma = max(sigma, 0.5)

                if is_high:
                    max_c = metar.get("maxT")
                    max_f = (max_c * 9 / 5 + 32) if max_c is not None else temp_f
                    observed = max(temp_f, max_f)

                    if observed > b:
                        # Day's high already exceeds top of bucket → bucket
                        # can never be the final answer (high only goes up)
                        log.info(
                            "%s — METAR range: high %.1f°F > %.0f°F → P≈0",
                            market.ticker, observed, b,
                        )
                        return 0.01, "METAR"

                    if observed >= a:
                        # Currently in-bucket: final high stays here only if
                        # no future reading exceeds b. P(future_max ≤ b)
                        # under Gumbel-max anchored at current temp.
                        prob = gumbel_cdf_max(b, temp_f, sigma)
                    else:
                        # Below bucket: future could reach [a, b]
                        prob = max(
                            0.0,
                            gumbel_cdf_max(b, temp_f, sigma)
                            - gumbel_cdf_max(a, temp_f, sigma),
                        )
                else:
                    # KXLOW — daily low can only go down or stay
                    min_c = metar.get("minT")
                    min_f = (min_c * 9 / 5 + 32) if min_c is not None else temp_f
                    observed = min(temp_f, min_f)

                    if observed < a:
                        # Day's low already below bucket floor → impossible
                        log.info(
                            "%s — METAR range: low %.1f°F < %.0f°F → P≈0",
                            market.ticker, observed, a,
                        )
                        return 0.01, "METAR"

                    if observed <= b:
                        # In-bucket: stays here if no future temp dips below a.
                        # P(future_min ≥ a) = 1 - F_min(a)
                        prob = 1.0 - gumbel_cdf_min(a, temp_f, sigma)
                    else:
                        # Above bucket: future could dip into [a, b].
                        # P(a ≤ future_min ≤ b) = F_min(b) - F_min(a)
                        prob = max(
                            0.0,
                            gumbel_cdf_min(b, temp_f, sigma)
                            - gumbel_cdf_min(a, temp_f, sigma),
                        )

                # Clamp to [0.01, 0.99] — avoid certainties the model can't
                # justify (METAR updates only hourly; surprises happen)
                prob = max(0.01, min(0.99, prob))
                log.info(
                    "%s — METAR range: obs=%.1f°F temp=%.1f°F [%.0f-%.0f] σ=%.1f P=%.0f%%",
                    market.ticker, observed, temp_f, a, b, sigma, prob * 100,
                )
                return prob, "METAR"

        # --- NWS forecast fallback ---
        forecast = self._get_forecast(market)
        if not forecast:
            return None, ""
        nws_temp = self._extract_nws_temp(
            forecast, market.close_time, is_high=is_high,
        )
        if nws_temp is None:
            return None, ""

        mu = float(nws_temp)
        try:
            target_date = market.close_time.date()
        except Exception:
            target_date = None
        sigma, source, _ = self._resolve_forecast_sigma(
            city, target_date, is_high, mu, hours_left,
        )
        if sigma is None:
            return None, ""
        if is_high:
            prob = gumbel_cdf_max(b, mu, sigma) - gumbel_cdf_max(a, mu, sigma)
        else:
            prob = gumbel_cdf_min(b, mu, sigma) - gumbel_cdf_min(a, mu, sigma)
        prob = max(0.01, min(0.99, prob))
        return prob, source

    @staticmethod
    def _extract_range_bounds(market: CandidateMarket) -> tuple[Optional[float], Optional[float]]:
        """Extract bucket bounds [a, b] from Kalshi floor_strike/cap_strike
        (preferred) or title regex fallback (for legacy fake markets)."""
        floor = market.raw.get("floor_strike")
        cap = market.raw.get("cap_strike")
        if floor is not None and cap is not None:
            try:
                a, b = float(floor), float(cap)
                if b < a:
                    a, b = b, a
                return a, b
            except (TypeError, ValueError):
                pass
        # Fallback: parse "61-62°" or "61 to 62°" from title
        m = re.search(
            r"(\d+(?:\.\d+)?)\s*[°º]?\s*(?:to|-)\s*(\d+(?:\.\d+)?)\s*[°º]?",
            market.title,
        )
        if m:
            a, b = float(m.group(1)), float(m.group(2))
            if b < a:
                a, b = b, a
            return a, b
        return None, None

    def _get_rain_probability(
        self, market: CandidateMarket,
    ) -> tuple[Optional[float], str]:
        """P(rain today). Checks METAR for active precip first."""
        city = self._resolve_city(market)
        if city:
            metar = self._get_metar(city)
            if metar:
                wx = (metar.get("wxString") or "").upper()
                precip_codes = ("RA", "DZ", "SH", "TS", "UP")
                if any(code in wx for code in precip_codes):
                    log.info(
                        "%s — METAR precip active: %s",
                        market.ticker, wx,
                    )
                    return 0.97, "METAR"

        # NWS PoP fallback
        forecast = self._get_forecast(market)
        if not forecast:
            return None, ""

        periods = forecast.get("properties", {}).get("periods", [])
        target_day = market.close_time.date()
        for period in periods:
            start = period.get("startTime", "")
            try:
                period_date = datetime.fromisoformat(start).date()
            except ValueError:
                continue
            if period_date == target_day and period.get("isDaytime"):
                pop = period.get("probabilityOfPrecipitation", {})
                val = pop.get("value") if isinstance(pop, dict) else pop
                if val is not None:
                    return float(val) / 100, "NWS"
        return None, ""

    def _extract_nws_temp(self, forecast: dict, target_date, is_high: bool) -> Optional[float]:
        periods = forecast.get("properties", {}).get("periods", [])
        target_day = target_date.date()
        for period in periods:
            start = period.get("startTime", "")
            try:
                period_date = datetime.fromisoformat(start).date()
            except ValueError:
                continue
            if period_date == target_day:
                temp = period.get("temperature")
                if is_high and period.get("isDaytime") and temp is not None:
                    return float(temp)
                elif not is_high and not period.get("isDaytime") and temp is not None:
                    return float(temp)
        return None

    @staticmethod
    def _extract_temp_threshold(title: str) -> Optional[float]:
        match = re.search(r"(\d+(?:\.\d+)?)\s*[°º]?\s*F", title, re.IGNORECASE)
        if match:
            return float(match.group(1))
        return None

    @staticmethod
    def _extract_wind_threshold(title: str) -> Optional[float]:
        """Extract mph threshold from market title."""
        match = re.search(
            r"(\d+(?:\.\d+)?)\s*mph", title, re.IGNORECASE,
        )
        if match:
            return float(match.group(1))
        return None

    def _get_wind_probability(
        self, market: CandidateMarket,
    ) -> tuple[Optional[float], str]:
        """P(wind exceeds threshold). Uses METAR wind observations."""
        city = self._resolve_city(market)
        if not city:
            return None, ""

        metar = self._get_metar(city)
        if not metar:
            return None, ""

        wspd_kt = metar.get("wspd", 0) or 0
        wgst_kt = metar.get("wgst") or 0
        max_wind_mph = max(wspd_kt, wgst_kt) * 1.15078

        threshold = self._extract_wind_threshold(market.title)
        if threshold is None:
            return None, ""

        if max_wind_mph >= threshold:
            log.info(
                "%s — METAR: wind %.0f mph ≥ %.0f mph → P=97%%",
                market.ticker, max_wind_mph, threshold,
            )
            return 0.97, "METAR"

        hours_left = market.minutes_to_close / 60
        sigma = max(2.0, min(hours_left * 1.0, 5.0))
        z = (max_wind_mph - threshold) / (sigma * sqrt(2))
        prob = 0.5 * (1 + erf(z))
        log.info(
            "%s — METAR: wind %.0f mph vs %.0f σ=%.1f P=%.0f%%",
            market.ticker, max_wind_mph, threshold, sigma,
            prob * 100,
        )
        return prob, "METAR"

    def _get_snow_probability(
        self, market: CandidateMarket,
    ) -> tuple[Optional[float], str]:
        """P(snow today). Checks METAR for active snowfall and temp."""
        city = self._resolve_city(market)
        if not city:
            return None, ""

        metar = self._get_metar(city)
        if not metar:
            return None, ""

        wx = (metar.get("wxString") or "").upper()
        snow_codes = ("SN", "GS", "PL", "IC")
        if any(code in wx for code in snow_codes):
            log.info(
                "%s — METAR snow/ice active: %s",
                market.ticker, wx,
            )
            return 0.95, "METAR"

        # Above freezing and no snow → very unlikely near close
        temp_c = metar.get("temp")
        if temp_c is not None and temp_c > 3.0:
            hours_left = market.minutes_to_close / 60
            if hours_left < 6:
                log.info(
                    "%s — METAR: %.1f°C (>37°F) no snow, P=5%%",
                    market.ticker, temp_c,
                )
                return 0.05, "METAR"

        return None, ""

    # ── Partition-sum arbitrage (cross-market) ────────────────────────────────

    # Series prefixes whose events are partitioned into mutually-exclusive,
    # exhaustive markets — i.e. one tail (<X), N ranges (a-b), one tail (>Y).
    # Other weather series (KXRAIN/KXWIND/KXSNOW) are typically single binary
    # markets and don't form partitions, so they're excluded.
    PARTITION_SERIES_PREFIXES = ("KXHIGH", "KXLOW")

    def evaluate_batch(self, candidates: list[CandidateMarket]) -> list[OrderIntent]:
        """Detect partition-sum arbitrage across mutually-exclusive markets.

        For each Kalshi event whose markets form a partition (e.g.
        KXHIGHNY-26APR11 = {<61, 61-62, 63-64, 65-66, 67-68, >68}),
        the YES outcomes are mutually exclusive and exhaustive, so:

            Σ P(yes_i) = 1   for all i in event

        Therefore:
          - If Σ yes_ask < 1 − fees: buying YES at ask on every leg is
            risk-free — exactly one leg pays $1, total cost < $1.
          - Equivalently if Σ no_ask < (N − 1) − fees: buying NO at ask
            on every leg yields exactly $(N − 1) for cost < (N − 1).

        We emit one intent per leg, sized to the smallest available
        ask depth so the basket actually fills evenly.
        """
        # Group by event_ticker, only for partition-shaped series.
        groups: dict[str, list[CandidateMarket]] = {}
        for c in candidates:
            if not c.event_ticker:
                continue
            if not any(c.series.upper().startswith(p) for p in self.PARTITION_SERIES_PREFIXES):
                continue
            groups.setdefault(c.event_ticker, []).append(c)

        intents: list[OrderIntent] = []
        # Day-roll check: clear the kill-switch on local-day change so a
        # fresh session isn't still paused from yesterday's losses.
        today = datetime.now().date().isoformat()
        if today != self._kill_switch_day:
            self._kill_switch_day = today
            self._basket_outcomes = []
            self._kill_switch_tripped = False
        if self._kill_switch_tripped:
            return intents
        now = time.time()
        cooldown = config.PARTITION_ARB_EVENT_COOLDOWN_S
        for event_ticker, legs in groups.items():
            if len(legs) < config.PARTITION_ARB_MIN_LEGS:
                continue
            # Event-level cooldown: after any basket-build attempt we suppress
            # rebuilds on the same event for COOLDOWN_S so a slow/partial fill
            # doesn't cause the same arb to keep re-firing every loop.
            last_attempt = self._event_attempt_ts.get(event_ticker, 0.0)
            age = now - last_attempt
            if age < cooldown:
                log.debug("Cooldown skip %s: %.0fs < %ds", event_ticker, age, cooldown)
                continue
            # Pull in any partition legs the scanner dropped before validating
            # — the completeness check should run against the ACTUAL event,
            # not the scanner-filtered subset.
            legs = self._augment_with_event_markets(event_ticker, legs)
            is_valid, missing = self._is_complete_partition(event_ticker, legs)
            new_intents: list[OrderIntent] = []
            if is_valid:
                new_intents = self._find_partition_arb(event_ticker, legs, missing=missing)
            # Cert-winner runs in PARALLEL with partition arb — it's the
            # cleanest single trade we have (METAR proves the leg settles
            # at $1) and there's no reason to skip it just because a
            # partition basket already fired on the event. Dedupe by
            # ticker so we don't double-buy a leg that's already part of
            # the basket.
            if config.CERT_WINNER_ENABLED:
                basket_tickers = {i.ticker for i in new_intents}
                cert_intents = self._find_certain_winners(event_ticker, legs)
                new_intents.extend(
                    i for i in cert_intents if i.ticker not in basket_tickers
                )
            if new_intents:
                self._event_attempt_ts[event_ticker] = now
                log.info("Cooldown start %s: next attempt in %ds", event_ticker, cooldown)
            intents.extend(new_intents)
        return intents

    def _open_event_markets(self, event_ticker: str) -> list[dict]:
        """Return the authoritative list of active markets for this event,
        cached 60s. Used both for the completeness count check and for
        filling in partition legs the scanner dropped (low volume / wide
        spread filters can hide buckets that still price meaningfully)."""
        import time as _time
        now = _time.time()
        cached = self._event_market_count_cache.get(event_ticker)
        if cached and now - cached[1] < 60:
            return cached[0]
        try:
            markets = self._client.get_markets_for_event(event_ticker)
            active = [m for m in markets if m.get("status") == "active"]
            if not active:
                active = list(markets)
            self._event_market_count_cache[event_ticker] = (active, now)
            return active
        except Exception as e:
            log.warning("Failed to fetch markets for %s: %s", event_ticker, e)
            return []

    def _open_market_count(self, event_ticker: str) -> int:
        """Backwards-compat shim — count of active markets in the event."""
        return len(self._open_event_markets(event_ticker))

    def _augment_with_event_markets(
        self, event_ticker: str, legs: list[CandidateMarket],
    ) -> list[CandidateMarket]:
        """Fill in legs the scanner dropped using the authoritative event
        list. The scanner filters by volume/spread, which legitimately
        skips illiquid buckets — but for partition completeness we need
        ALL legs, even if some have zero displayed depth (those will
        cause _build_basket to abort the basket later, which is correct).

        This is the single biggest +EV change: yesterday's losses came
        from partitions where the scanner returned 4 of 6 legs, the
        completeness check accepted it, the dead-leg trim narrowed
        further, and the modal outcome lived on a leg we never owned.
        Fetching the full list first eliminates that whole failure mode.
        """
        if self._client is None:
            return legs
        all_markets = self._open_event_markets(event_ticker)
        if not all_markets:
            return legs
        from scanner import _parse
        existing = {m.ticker for m in legs}
        now_dt = datetime.now(timezone.utc)
        added = 0
        merged = list(legs)
        for raw in all_markets:
            tkr = raw.get("ticker", "")
            if not tkr or tkr in existing:
                continue
            parsed = _parse(raw, now_dt)
            if parsed is None:
                continue
            parsed.tags = ["weather"]
            merged.append(parsed)
            added += 1
        if added:
            log.info(
                "Partition %s: augmented with %d scanner-dropped leg(s) "
                "(scanner=%d, event=%d, merged=%d)",
                event_ticker, added, len(legs), len(all_markets), len(merged),
            )
        return merged

    def _is_complete_partition(self, event_ticker: str, legs: list[CandidateMarket]) -> tuple[bool, int]:
        """Verify the legs form a (near-)complete partition.

        Returns (is_valid, missing_count) — missing_count is 0 for a full
        partition or 1 if we're one leg short and PARTITION_ARB_ALLOW_MISSING_LEG
        is enabled. Callers add PARTITION_ARB_MISSING_LEG_PHANTOM_CENTS to the
        basket cost per missing leg so only clearly-profitable arbs qualify.

        A valid Kalshi weather partition needs ≥1 between range. The tails
        (less_than/more_than) are allowed to be missing one (but not both),
        and with the scanner-vs-event cross-check we also allow one between
        bucket to be scanner-filtered.
        """
        LESS_TYPES = ("less_than", "less")
        MORE_TYPES = ("more_than", "more", "greater", "greater_than")
        types = [m.strike_type for m in legs]
        n_less = sum(1 for t in types if t in LESS_TYPES)
        n_more = sum(1 for t in types if t in MORE_TYPES)
        n_between = types.count("between")

        if n_between < 1:
            log.info("Partition %s: no between legs — not a real partition", event_ticker)
            return False, 0

        allow_missing = config.PARTITION_ARB_ALLOW_MISSING_LEG

        # Tail check: require both tails in strict mode, allow one missing
        # otherwise. If both are missing, bail — too much uncovered range.
        tails_missing = (0 if n_less == 1 else 1) + (0 if n_more == 1 else 1)
        if tails_missing > 1 or (tails_missing == 1 and not allow_missing):
            log.info(
                "Partition %s: incomplete — %d less_than, %d more_than, %d between",
                event_ticker, n_less, n_more, n_between,
            )
            return False, 0

        sum_ask = sum(m.yes_ask for m in legs)
        # A real complete partition has Σ yes_ask ≈ 1.0 (slightly under due
        # to spread). Anything below ~0.85 means missing buckets — the
        # scanner dropped legs the market still prices meaningfully. Was
        # 0.50, but in live runs that admitted partitions like 6×~8c where
        # the surviving 4 covered just 13% of the probability mass; the
        # "edge" was illusory because the modal outcome lived on legs we
        # never bought.
        if sum_ask < 0.85:
            log.info(
                "Partition %s: sum_ask=%.2f below 0.85 for %d legs — "
                "missing buckets, not a real partition",
                event_ticker, sum_ask, len(legs),
            )
            return False, 0

        # Cross-check against Kalshi's authoritative event→markets list to
        # catch buckets the scanner filtered out for spread/volume. Accept
        # up to 1 missing bucket in total (tail or between combined).
        scanner_missing = 0
        if self._client is not None:
            total = self._open_market_count(event_ticker)
            if total > 0:
                scanner_missing = max(0, total - len(legs) - tails_missing)

        total_missing = tails_missing + scanner_missing
        max_allowed = 1 if allow_missing else 0
        if total_missing > max_allowed:
            log.info(
                "Partition %s: %d legs vs event %d (tails_missing=%d, scanner_missing=%d) — skipping",
                event_ticker, len(legs),
                len(self._event_market_count_cache.get(event_ticker, ([], 0))[0]),
                tails_missing, scanner_missing,
            )
            return False, 0

        return True, total_missing

    @staticmethod
    def _partition_is_contiguous(legs: list[CandidateMarket]) -> bool:
        """Verify the legs form a contiguous partition with no interior holes.

        Tails (less_than/more_than) sit at the ends; between-buckets in the
        middle. A valid partition: less_than.cap == first_between.floor,
        consecutive between-buckets share boundaries, last_between.cap ==
        more_than.floor. Tail absence is allowed (one-tailed event), but
        interior gaps mean a leg was killed mid-partition — refuse to fire.
        """
        less = [m for m in legs if m.strike_type in ("less_than", "less")]
        more = [m for m in legs if m.strike_type in ("more_than", "more", "greater", "greater_than")]
        between = sorted(
            [m for m in legs if m.strike_type == "between"],
            key=lambda m: m.raw.get("floor_strike", 0),
        )
        if len(less) > 1 or len(more) > 1:
            return False
        if not between:
            return True  # only tails — degenerate but contiguous
        # Tail-to-first-between
        if less and abs(float(less[0].raw.get("cap_strike", 0)) -
                       float(between[0].raw.get("floor_strike", 0))) > 0.01:
            return False
        # Between-to-between
        for i in range(len(between) - 1):
            cap_i = float(between[i].raw.get("cap_strike", 0))
            floor_next = float(between[i + 1].raw.get("floor_strike", 0))
            if abs(cap_i - floor_next) > 0.01:
                return False
        # Last-between-to-tail
        if more and abs(float(between[-1].raw.get("cap_strike", 0)) -
                       float(more[0].raw.get("floor_strike", 0))) > 0.01:
            return False
        return True

    def _track_edge_fire(self, edge_cents: float) -> None:
        """Record a partition-arb fire and periodically log rolling mean.

        Edge decay is real and silent — without an explicit observability
        hook the operator only notices once net P&L flatlines. Logging the
        mean of the last 50 fires every ~30 min surfaces compression early.
        """
        now = time.time()
        self._recent_edge_fires.append((now, edge_cents))
        cutoff = now - 7 * 24 * 3600  # 7-day window
        self._recent_edge_fires = [
            (t, e) for t, e in self._recent_edge_fires if t >= cutoff
        ][-500:]
        self.save_state()
        if now - self._recent_edge_last_log_ts < 1800:
            return
        self._recent_edge_last_log_ts = now
        recent = self._recent_edge_fires[-50:]
        if len(recent) >= 5:
            avg = sum(e for _, e in recent) / len(recent)
            threshold_cents = config.PARTITION_ARB_MIN_EDGE * 100
            level = log.warning if avg < threshold_cents * 1.2 else log.info
            level(
                "Edge-compression watch: last %d fires avg %.2f¢ (latest %.2f¢, threshold %.0f¢)%s",
                len(recent), avg, edge_cents, threshold_cents,
                " — compression: avg < 1.2× threshold, consider tightening MIN_EDGE"
                if avg < threshold_cents * 1.2 else "",
            )

    def _find_partition_arb(
        self, event_ticker: str, legs: list[CandidateMarket],
        missing: int = 0,
    ) -> list[OrderIntent]:
        """Generate one intent per leg if the YES-ask sum (or NO-ask sum)
        is below the no-arb bound by at least PARTITION_ARB_MIN_EDGE.

        `missing` is the count of partition buckets not present in `legs`
        (0 for a complete partition, 1 when one leg is absent and
        PARTITION_ARB_ALLOW_MISSING_LEG is on). For each missing leg we
        subtract PARTITION_ARB_MISSING_LEG_PHANTOM_CENTS from the edge as
        a conservative assumption of what the missing bucket would have
        cost to cover, so only comfortably-profitable arbs qualify.
        """
        # METAR-aware filter: drop legs whose outcome is already impossible.
        # Exactly one of the surviving legs must still win (dead legs can't),
        # so the partition guarantee on the shrunken basket is unchanged.
        # Beyond the METAR window, fall back to NWS-implied P(win) < ε.
        dead = self._observably_dead_legs(event_ticker, legs)
        if not dead:
            dead = self._probably_dead_legs_nws(event_ticker, legs)
        if dead:
            pre_trim_sum = sum(m.yes_ask for m in legs)
            dead_sum = sum(m.yes_ask for m in legs if m.ticker in dead)
            legs = [m for m in legs if m.ticker not in dead]
            if len(legs) < 2:
                log.info(
                    "%s: only %d live leg(s) after METAR filter — skipping",
                    event_ticker, len(legs),
                )
                return []
            # Dead legs SHOULD be priced near 0¢ (market agrees they can't
            # win). If killing them dropped Σ yes_ask by more than 25¢,
            # the market disagrees with our dead-leg claim — likely the
            # market knows about further temp swings ahead. Bail rather
            # than buy a basket whose "edge" came from a stale METAR snap.
            surviving_sum = pre_trim_sum - dead_sum
            # Tightened from dead_sum>0.25 / surviving_sum<0.70 after the
            # SATX loss — any non-trivial dead Σ now triggers a skip
            # (defense-in-depth alongside the per-leg market-price guard).
            if dead_sum > 0.10 or surviving_sum < 0.85:
                log.info(
                    "%s: trim dropped Σ from %.2f → %.2f (dead Σ=%.2f) — "
                    "market disagrees with dead-leg claim, skipping",
                    event_ticker, pre_trim_sum, surviving_sum, dead_sum,
                )
                return []
            # Contiguity check: after trim, the surviving partition must
            # still cover [pre_min_floor, pre_max_cap] with no holes. A
            # killed interior bucket whose neighbors are still alive is a
            # red flag — the killed bucket has live neighbors the market
            # is pricing, so our model overconfidence is exposed. METAR
            # trim is naturally tail-only; NWS-trim can hit interior buckets.
            if not self._partition_is_contiguous(legs):
                log.warning(
                    "%s: trim broke partition contiguity — refusing fire",
                    event_ticker,
                )
                return []

        n = len(legs)
        sum_yes_ask = sum(m.yes_ask for m in legs)
        sum_yes_bid = sum(m.yes_bid for m in legs)
        # NO ask = 1 − YES bid; sum of NO asks = N − Σ yes_bid
        sum_no_ask = n - sum_yes_bid

        yes_edge_raw = 1.0 - sum_yes_ask          # profit per basket before fees
        no_edge_raw  = (n - 1) - sum_no_ask        # equivalent on the NO side

        # Estimate total taker fees for a 1-contract basket on each side.
        # Fee per leg = ceil(rate × 1 × P × (1−P)), sum across all legs.
        basket_size = config.DEFAULT_ORDER_SIZE  # estimate; _build_basket may adjust
        yes_fees = sum(
            kalshi_fee(basket_size, max(1, min(int(round(m.yes_ask * 100)), 99)))
            for m in legs
        )
        no_fees = sum(
            kalshi_fee(basket_size, max(1, min(int(round((1.0 - m.yes_bid) * 100)), 99)))
            for m in legs
        )
        # Per-contract fees (divide by basket_size to get edge per contract)
        yes_fee_per = yes_fees / basket_size
        no_fee_per = no_fees / basket_size

        # Buffer cost: we pay BUFFER¢ above the displayed ask on every leg so
        # thin-depth limits actually cross. That's n_legs × buffer cents of
        # extra cost per basket, which must come out of the edge.
        buffer_cost = (config.PARTITION_ARB_PRICE_BUFFER_CENTS / 100.0) * n
        # Phantom cost for each missing leg — we don't know what the missing
        # bucket would have cost, so assume PHANTOM_CENTS and require that
        # much extra edge before trading an incomplete basket.
        phantom_cost = (config.PARTITION_ARB_MISSING_LEG_PHANTOM_CENTS / 100.0) * missing

        yes_edge = yes_edge_raw - yes_fee_per - buffer_cost - phantom_cost
        no_edge  = no_edge_raw - no_fee_per - buffer_cost - phantom_cost

        log.debug(
            "Partition %s: N=%d Σyes_ask=%.3f Σno_ask=%.3f yes_edge_raw=%+.3f yes_fees=$%.2f yes_edge_net=%+.3f no_edge_net=%+.3f",
            event_ticker, n,
            sum_yes_ask, sum_no_ask, yes_edge_raw, yes_fees, yes_edge, no_edge,
        )

        if yes_edge >= config.PARTITION_ARB_MIN_EDGE:
            intents = self._build_basket(event_ticker, legs, side="yes", edge=yes_edge)
            if self._metrics is not None:
                self._metrics.push_basket(edge_cents=yes_edge * 100, submitted=bool(intents))
            if intents:
                self._basket_edge_at_fire[event_ticker] = round(yes_edge * 100, 2)
                self._track_edge_fire(yes_edge * 100)
            return intents
        # Diagnostic: when YES path didn't clear but NO is positive, log it.
        # Helps confirm whether NO-side arbs simply never appear in our
        # universe vs. a logic bug suppressing them.
        if yes_edge < config.PARTITION_ARB_MIN_EDGE and no_edge > 0:
            log.debug(
                "Partition %s NO-edge %+.3f (YES %+.3f, threshold %.3f)",
                event_ticker, no_edge, yes_edge, config.PARTITION_ARB_MIN_EDGE,
            )
        if no_edge >= config.PARTITION_ARB_MIN_EDGE:
            intents = self._build_basket(event_ticker, legs, side="no", edge=no_edge)
            if self._metrics is not None:
                self._metrics.push_basket(edge_cents=no_edge * 100, submitted=bool(intents))
            if intents:
                self._basket_edge_at_fire[event_ticker] = round(no_edge * 100, 2)
                self._track_edge_fire(no_edge * 100)
            return intents
        return []

    def _build_basket(
        self,
        event_ticker: str,
        legs: list[CandidateMarket],
        side: str,
        edge: float,
    ) -> list[OrderIntent]:
        """Construct one OrderIntent per leg, sized to the basket's
        smallest displayed ask depth so all legs can actually fill.

        A basket is all-or-nothing: if any leg lacks depth or prices
        above the leg-price cap, the entire basket is aborted. Submitting
        a partial partition turns an arb into an unhedged directional bet.
        """
        # Per-leg depth on the side we're crossing. A leg with 0 depth
        # cannot fill — we must abort the whole basket rather than
        # submit a partial partition.
        # Late-window relax: a 92c near-cert leg paired with cheap tails is
        # a real arb close to settle. The default 90c cap was disqualifying
        # those baskets entirely.
        min_minutes = min((m.minutes_to_close for m in legs), default=10**9)
        is_late = min_minutes <= config.PARTITION_ARB_LATE_WINDOW_MINUTES
        max_leg_price = (
            config.PARTITION_ARB_MAX_LEG_PRICE_LATE if is_late
            else config.PARTITION_ARB_MAX_LEG_PRICE
        )
        leg_specs: list[tuple[object, int, int]] = []  # (market, price_cents, depth)
        for m in legs:
            if side == "yes":
                price_cents = max(1, min(int(round(m.yes_ask * 100)), 99))
                depth = m.yes_ask_size
            else:
                no_ask_dollars = 1.0 - m.yes_bid
                price_cents = max(1, min(int(round(no_ask_dollars * 100)), 99))
                depth = m.yes_bid_size

            if price_cents > max_leg_price:
                log.info(
                    "PARTITION ARB %s ABORTED — leg %s priced %dc > cap %dc "
                    "(skipping partial basket would break partition guarantee)",
                    event_ticker, m.ticker, price_cents, max_leg_price,
                )
                return []

            if depth <= 0:
                log.info(
                    "PARTITION ARB %s ABORTED — leg %s has no displayed %s depth "
                    "(cannot guarantee fill)",
                    event_ticker, m.ticker, "ask" if side == "yes" else "bid",
                )
                return []

            leg_specs.append((m, price_cents, depth))

        min_depth = min(spec[2] for spec in leg_specs)

        # Scale target size with edge: a 40¢-edge basket is worth a much
        # bigger bet than an 8¢ one. Capped by depth and a hard max so we
        # never overcommit relative to what's actually displayed.
        target = int(edge * config.PARTITION_ARB_SIZE_PER_EDGE)
        target = max(config.MIN_ORDER_SIZE,
                     min(target, config.PARTITION_ARB_MAX_BASKET_SIZE))
        # Volatility-aware shrink: thin-book baskets are the most likely to
        # have stale or about-to-move quotes, so scale target by the ratio
        # of min_depth to the typical depth we expect on a healthy book.
        # depth_factor is clamped to [0,1] — a book deeper than typical
        # doesn't grow our bet beyond `target`.
        typical = max(1, config.PARTITION_ARB_TYPICAL_DEPTH)
        depth_factor = min(1.0, min_depth / typical)
        scaled = max(config.MIN_ORDER_SIZE, int(target * depth_factor))
        basket_size = max(config.MIN_ORDER_SIZE, min(scaled, min_depth))

        log.info(
            "PARTITION ARB %s side=%s edge=%.1f¢ size=%d × %d legs",
            event_ticker, side.upper(), edge * 100, basket_size, len(legs),
        )

        reason = f"Partition arb {event_ticker}: {side.upper()} basket edge {edge:.1%}"
        basket_id = f"{event_ticker}:{side}:{int(time.time())}"
        # Liquid books don't need a 2c crossing buffer; 1c is enough and
        # recovers ~5% of edge on 5-leg baskets. Thin books still pay the
        # full buffer because their best ask is most likely to move.
        if min_depth >= config.PARTITION_ARB_LIQUID_DEPTH:
            buffer_c = config.PARTITION_ARB_PRICE_BUFFER_CENTS_LIQUID
        else:
            buffer_c = config.PARTITION_ARB_PRICE_BUFFER_CENTS
        return [
            OrderIntent(
                ticker=m.ticker, side=side,
                count=basket_size,
                # Cross the spread by buffer_c cents so the order actually
                # fills when depth at the best ask is thin. Never exceed 99c.
                limit_price=min(99, price_cents + buffer_c),
                reason=reason, basket_id=basket_id,
            )
            for m, price_cents, _ in leg_specs
        ]

    def _find_certain_winners(
        self, event_ticker: str, legs: list[CandidateMarket],
    ) -> list[OrderIntent]:
        """Single-leg arb: buy YES on any leg METAR proves will settle true.

        Mirrors the dead-leg filter used for partitions. For KXHIGH a
        ``more_than`` leg with floor < observed_max is already won (max is
        monotonic up). For KXLOW a ``less_than`` leg with cap > observed_min
        is already won (min is monotonic down). We only fire when there's
        real edge after fees + buffer and price is below the cap so we're
        never buying a tiny tail for a penny of edge.
        """
        if not legs:
            return []
        sample = legs[0]
        series = sample.series.upper()
        is_high = series.startswith("KXHIGH")
        is_low = series.startswith("KXLOW")
        if not (is_high or is_low):
            return []
        if sample.minutes_to_close / 60 > self.METAR_MAX_HOURS:
            return []
        city = self._resolve_city(sample)
        if not city:
            return []
        observed = self._get_observed_extreme(city, is_high=is_high)
        if observed is None:
            return []

        buffer_c = config.PARTITION_ARB_PRICE_BUFFER_CENTS
        max_price = config.CERT_WINNER_MAX_PRICE_CENTS
        min_edge = config.CERT_WINNER_MIN_EDGE_CENTS

        intents: list[OrderIntent] = []
        for m in legs:
            st = m.strike_type
            floor = m.raw.get("floor_strike")
            cap = m.raw.get("cap_strike")
            certain = False
            band = config.METAR_OBSERVATION_BAND_DEGREES
            if is_high and st in ("more_than", "more", "greater", "greater_than"):
                if floor is not None and observed - band > floor:
                    certain = True
            elif is_low and st in ("less_than", "less"):
                if cap is not None and observed + band < cap:
                    certain = True
            if not certain:
                continue

            if m.ticker in self._open_tickers:
                # Already holding this cert-winner leg — don't stack.
                continue

            if m.yes_ask_size <= 0:
                continue
            price_cents = max(1, min(int(round(m.yes_ask * 100)) + buffer_c, 99))
            if price_cents > max_price:
                continue

            # Cap by displayed depth, per-basket hard cap, and the
            # per-trade dollar limit — risk.approve would otherwise reject
            # an oversized intent outright rather than shrink it, so a
            # cheap-leg cert-winner at high depth would fail silently.
            dollar_cap = int((config.DEFAULT_MAX_TRADE_DOLLARS * 100) // price_cents)
            size = min(
                m.yes_ask_size,
                config.PARTITION_ARB_MAX_BASKET_SIZE,
                max(config.MIN_ORDER_SIZE, dollar_cap),
            )
            size = max(config.MIN_ORDER_SIZE, size)
            fee_per = kalshi_fee(size, price_cents) / size
            edge_cents = 100 - price_cents - fee_per
            if edge_cents < min_edge:
                if self._metrics is not None:
                    self._metrics.push_cert_winner(edge_cents=edge_cents, submitted=False)
                continue

            reason = (
                f"Cert winner {m.ticker}: certain via METAR "
                f"{'max' if is_high else 'min'}={observed:.1f}°F"
            )
            log.info(
                "CERT WINNER %s price=%dc edge=%.1f¢ size=%d (METAR %s=%.1f°F)",
                m.ticker, price_cents, edge_cents, size,
                "max" if is_high else "min", observed,
            )
            intents.append(OrderIntent(
                ticker=m.ticker, side="yes", count=size,
                limit_price=price_cents, reason=reason,
            ))
            if self._metrics is not None:
                self._metrics.push_cert_winner(edge_cents=edge_cents, submitted=True)
        return intents


# ── ExpiryMomentumStrategy (markets about to close) ─────────────────────────

class ExpiryMomentumStrategy(BaseStrategy):
    """
    For markets closing soon: if YES is already trading high (70c+),
    ride the momentum up toward resolution. If YES is low (<30c),
    fade it toward NO resolution.

    Also looks for ask prices meaningfully below mid (panicked sellers).
    """
    name = "expiry_momentum"

    def evaluate(self, market: CandidateMarket, orderbook: dict) -> Optional[OrderIntent]:
        if "expiring" not in market.tags:
            return None

        mid = market.mid
        bid = market.yes_bid
        ask = market.yes_ask

        # Skip near-certain outcomes
        if mid < config.EXPIRING_MIN_YES_PRICE or mid > config.EXPIRING_MAX_YES_PRICE:
            return None

        # Momentum: ride high-probability markets toward 1.0
        if mid >= 0.70:
            price_cents = min(int(ask * 100) + 1, 95)  # cap at 95c
            cost = ask  # what we pay per contract
            implied_prob = mid  # market's implied probability
            # EV check: only trade if expected value is positive after fees
            ev = implied_prob * (1.0 - cost) - (1.0 - implied_prob) * cost
            fee = kalshi_fee(1, price_cents)
            ev_net = ev - fee
            if ev_net < config.EXPIRING_EDGE_THRESHOLD:
                log.debug("%s — YES EV %.1f¢ (net of %.1f¢ fee) below threshold", market.ticker, ev_net * 100, fee * 100)
                return None
            size = self._size(ev_net, cost)
            return OrderIntent(
                ticker=market.ticker, side="yes", count=size,
                limit_price=price_cents,
                reason=f"Expiry momentum: YES EV {ev_net:.1%} (fee {fee:.0%}), mid {mid:.0%}, {market.minutes_to_close:.0f}min left",
            )

        # Momentum: ride low-probability markets toward 0.0
        if mid <= 0.30:
            no_ask = 1.0 - bid
            price_cents = min(int(no_ask * 100) + 1, 95)
            cost = no_ask
            implied_prob = 1.0 - mid  # probability of NO resolution
            ev = implied_prob * (1.0 - cost) - (1.0 - implied_prob) * cost
            fee = kalshi_fee(1, price_cents)
            ev_net = ev - fee
            if ev_net < config.EXPIRING_EDGE_THRESHOLD:
                log.debug("%s — NO EV %.1f¢ (net of %.1f¢ fee) below threshold", market.ticker, ev_net * 100, fee * 100)
                return None
            size = self._size(ev_net, cost)
            return OrderIntent(
                ticker=market.ticker, side="no", count=size,
                limit_price=price_cents,
                reason=f"Expiry momentum: NO EV {ev_net:.1%} (fee {fee:.0%}), mid {1-mid:.0%}, {market.minutes_to_close:.0f}min left",
            )

        # Mid-range: look for dislocated ask (panicked sellers)
        yes_edge = mid - ask
        no_edge = bid - mid

        # Deduct fee from the better edge before threshold check
        best_edge = max(yes_edge, no_edge)
        fee = kalshi_fee(1, int(max(ask, 1.0 - bid) * 100))
        if best_edge - fee < config.EXPIRING_EDGE_THRESHOLD:
            return None

        if yes_edge >= no_edge:
            price_cents = min(int((ask + config.LIMIT_PRICE_BUFFER) * 100), 99)
            return OrderIntent(
                ticker=market.ticker, side="yes",
                count=self._size(yes_edge, ask),
                limit_price=price_cents,
                reason=f"Expiry dislocation: YES ask {ask:.0%} < mid {mid:.0%}, {market.minutes_to_close:.0f}min",
            )
        else:
            no_price = 1.0 - bid + config.LIMIT_PRICE_BUFFER
            price_cents = min(int(no_price * 100), 99)
            return OrderIntent(
                ticker=market.ticker, side="no",
                count=self._size(no_edge, 1.0 - bid),
                limit_price=price_cents,
                reason=f"Expiry dislocation: NO bid {1-bid:.0%} < mid {1-mid:.0%}, {market.minutes_to_close:.0f}min",
            )

    def evaluate_exit(self, position: PositionInfo, market: CandidateMarket, orderbook: dict) -> Optional[OrderIntent]:
        """Exit expiring positions via trailing stop or profit-taking."""
        if "expiring" not in market.tags:
            return None

        entry_frac = position.entry_price / 100

        if position.side == "yes":
            mark = market.yes_bid
            pnl_pct = (mark - entry_frac) / entry_frac if entry_frac > 0 else 0
        else:
            no_bid = 1.0 - market.yes_ask
            mark = no_bid
            pnl_pct = (mark - entry_frac) / entry_frac if entry_frac > 0 else 0

        # Trailing stop: exit if loss exceeds threshold
        if pnl_pct <= -config.TRAILING_STOP_PCT:
            if position.side == "yes":
                sell_price = max(int(market.yes_bid * 100), 1)
            else:
                no_bid = 1.0 - market.yes_ask
                sell_price = max(int(no_bid * 100), 1)
            # Don't bother stopping out at 1c — the order rarely fills,
            # piles up as a zombie resting order, and blocks new entries.
            # The position will either recover or expire worthless.
            if sell_price < config.MIN_EXIT_PRICE_CENTS:
                return None
            return OrderIntent(
                ticker=position.ticker, side=position.side,
                count=position.count, limit_price=sell_price,
                reason=f"STOP: {pnl_pct:+.0%} loss on {position.side.upper()} (entry {entry_frac:.0%})",
                action="sell",
            )

        return None

    @staticmethod
    def _size(ev: float, cost: float) -> int:
        """Concave (sqrt) sizing with Kelly fraction cap.

        Uses sqrt scaling to dampen exposure on extreme signals.
        Also caps at quarter-Kelly to limit damage from model error.
        """
        base = config.DEFAULT_ORDER_SIZE
        threshold = config.EXPIRING_EDGE_THRESHOLD

        # Concave sqrt scaling
        sqrt_scaled = int(base * sqrt(ev / threshold))

        # Kelly fraction cap: f* = edge / odds, then take quarter-Kelly
        # For a binary: kelly = (p * b - q) / b where b = (1-cost)/cost, p = implied prob
        # Simplified: kelly ~= ev / cost
        if cost > 0:
            kelly_contracts = int(base * config.MAX_KELLY_FRACTION * (ev / cost) / threshold)
        else:
            kelly_contracts = sqrt_scaled

        size = min(sqrt_scaled, kelly_contracts)
        return max(config.MIN_ORDER_SIZE, min(size, config.MAX_POSITION_PER_MARKET))
