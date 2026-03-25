"""
strategy.py — Trading strategies for expiring and weather markets.

Each strategy takes a CandidateMarket + orderbook and returns an OrderIntent (or None).

Bot.py imports: BaseStrategy, FairValueStrategy, ExpiryMomentumStrategy, OrderIntent
"""

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from math import erf, sqrt
from typing import Optional

import requests

import config
from scanner import CandidateMarket

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
}

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


# ── OrderIntent ───────────────────────────────────────────────────────────────

@dataclass
class OrderIntent:
    """What the strategy wants to do. Passed to risk manager then executor."""
    ticker: str
    side: str           # "yes" or "no"
    count: int          # number of contracts
    limit_price: int    # cents (1–99)
    reason: str         # human-readable rationale


# ── Base ──────────────────────────────────────────────────────────────────────

class BaseStrategy(ABC):
    name: str = "base"

    @abstractmethod
    def evaluate(self, market: CandidateMarket, orderbook: dict) -> Optional[OrderIntent]:
        """Return an OrderIntent if there's an edge, else None."""


# ── FairValueStrategy (weather / obscure markets) ────────────────────────────

class FairValueStrategy(BaseStrategy):
    """
    For weather and obscure markets: compare an external fair value estimate
    against the market price. If the market is mispriced by more than the
    edge threshold, trade into it.

    For weather markets, uses NWS forecast data (resolved dynamically per city).
    For other markets, fades prices far from 50c (mean-reversion heuristic).
    """
    name = "fair_value"

    NWS_BASE = "https://api.weather.gov"

    def __init__(self):
        self._forecast_cache: dict[str, dict] = {}
        self._grid_cache: dict[str, tuple[str, int, int]] = {}  # city -> (office, gridX, gridY)

    def evaluate(self, market: CandidateMarket, orderbook: dict) -> Optional[OrderIntent]:
        if "weather" in market.tags:
            return self._evaluate_weather(market)
        else:
            return self._evaluate_mean_reversion(market)

    def _evaluate_mean_reversion(self, market: CandidateMarket) -> Optional[OrderIntent]:
        """Fade prices far from 50c on low-volume / obscure markets."""
        mid = market.mid
        fair = 0.50  # assume 50/50 for obscure markets with no signal

        edge = fair - mid  # positive means YES is cheap

        if abs(edge) < config.WEATHER_EDGE_THRESHOLD:
            return None

        if edge > 0:
            # YES looks cheap — buy YES
            price_cents = min(int(market.yes_ask * 100) + 1, 99)
            return OrderIntent(
                ticker=market.ticker, side="yes",
                count=config.DEFAULT_ORDER_SIZE,
                limit_price=price_cents,
                reason=f"Fair value: YES mid {mid:.0%} below fair {fair:.0%}",
            )
        else:
            # NO looks cheap — buy NO
            no_ask = 1.0 - market.yes_bid
            price_cents = min(int(no_ask * 100) + 1, 99)
            return OrderIntent(
                ticker=market.ticker, side="no",
                count=config.DEFAULT_ORDER_SIZE,
                limit_price=price_cents,
                reason=f"Fair value: NO mid {1-mid:.0%} below fair {1-fair:.0%}",
            )

    def _evaluate_weather(self, market: CandidateMarket) -> Optional[OrderIntent]:
        """Compare NWS forecast against Kalshi implied probability.

        For KXHIGH/KXLOW/KXRAIN: attempts NWS forecast lookup, falls back to
        mean-reversion if the lookup fails.
        For KXSNOW/KXWIND: uses mean-reversion directly (no NWS signal).
        """
        series = market.series.upper()

        nws_prob = None
        if series.startswith("KXHIGH") or series.startswith("KXLOW"):
            nws_prob = self._get_temp_probability(market)
        elif series.startswith("KXRAIN"):
            nws_prob = self._get_rain_probability(market)
        elif series.startswith("KXSNOW") or series.startswith("KXWIND"):
            # No reliable NWS signal — use mean-reversion
            log.debug("%s — no NWS signal for %s, using mean-reversion", market.ticker, series)
            return self._evaluate_mean_reversion(market)
        else:
            return self._evaluate_mean_reversion(market)

        if nws_prob is None:
            # NWS lookup failed — fall back to mean-reversion instead of skipping
            log.info("%s — NWS lookup failed, falling back to mean-reversion", market.ticker)
            return self._evaluate_mean_reversion(market)

        mid = market.mid
        edge = nws_prob - mid

        if abs(edge) < config.WEATHER_EDGE_THRESHOLD:
            log.debug("%s — weather edge %.1f¢ below threshold", market.ticker, abs(edge) * 100)
            return None

        if edge > 0:
            price_cents = min(int(market.yes_ask * 100) + 1, 99)
            return OrderIntent(
                ticker=market.ticker, side="yes",
                count=config.DEFAULT_ORDER_SIZE,
                limit_price=price_cents,
                reason=f"Weather: NWS {nws_prob:.0%} vs market {mid:.0%}",
            )
        else:
            no_ask = 1.0 - market.yes_bid
            price_cents = min(int(no_ask * 100) + 1, 99)
            return OrderIntent(
                ticker=market.ticker, side="no",
                count=config.DEFAULT_ORDER_SIZE,
                limit_price=price_cents,
                reason=f"Weather: NWS {nws_prob:.0%} vs market {mid:.0%} (fade YES)",
            )

    # ── NWS helpers ──────────────────────────────────────────────────────────

    def _resolve_city(self, market: CandidateMarket) -> Optional[str]:
        """Extract city name from ticker or title."""
        # Try ticker code: e.g. KXHIGH-26MAR26-SFO-T68 → SFO
        parts = market.ticker.upper().split("-")
        for part in parts:
            if part in TICKER_CITY_MAP:
                return TICKER_CITY_MAP[part]

        # Try title keywords
        title_lower = market.title.lower()
        for city_name in CITY_COORDS:
            if city_name.lower() in title_lower:
                return city_name

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

    def _get_forecast(self, market: CandidateMarket) -> Optional[dict]:
        """Fetch NWS forecast for the city in a market. Dynamically resolves the grid."""
        city = self._resolve_city(market)
        if not city:
            log.debug("Could not determine city for %s", market.ticker)
            return None

        cache_key = city
        if cache_key in self._forecast_cache:
            return self._forecast_cache[cache_key]

        grid = self._resolve_nws_grid(city)
        if not grid:
            return None

        office, grid_x, grid_y = grid
        url = f"{self.NWS_BASE}/gridpoints/{office}/{grid_x},{grid_y}/forecast"
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "kalshi-bot/1.0"})
            resp.raise_for_status()
            data = resp.json()
            self._forecast_cache[cache_key] = data
            return data
        except Exception as e:
            log.error("NWS forecast fetch failed for %s: %s", city, e)
            return None

    def _get_temp_probability(self, market: CandidateMarket) -> Optional[float]:
        forecast = self._get_forecast(market)
        if not forecast:
            return None

        threshold = self._extract_temp_threshold(market.title)
        if threshold is None:
            return None

        nws_temp = self._extract_nws_temp(forecast, market.close_time, is_high=market.series.upper().startswith("KXHIGH"))
        if nws_temp is None:
            return None

        # P(actual > threshold) using normal CDF with +/-3F uncertainty
        z = (nws_temp - threshold) / (3.0 * sqrt(2))
        return 0.5 * (1 + erf(z))

    def _get_rain_probability(self, market: CandidateMarket) -> Optional[float]:
        forecast = self._get_forecast(market)
        if not forecast:
            return None

        periods = forecast.get("properties", {}).get("periods", [])
        target_day = market.close_time.date()
        for period in periods:
            start = period.get("startTime", "")
            try:
                from datetime import datetime
                period_date = datetime.fromisoformat(start).date()
            except ValueError:
                continue
            if period_date == target_day and period.get("isDaytime"):
                pop = period.get("probabilityOfPrecipitation", {})
                val = pop.get("value") if isinstance(pop, dict) else pop
                if val is not None:
                    return float(val) / 100  # convert % to 0-1
        return None

    def _extract_nws_temp(self, forecast: dict, target_date, is_high: bool) -> Optional[float]:
        periods = forecast.get("properties", {}).get("periods", [])
        target_day = target_date.date()
        for period in periods:
            start = period.get("startTime", "")
            try:
                from datetime import datetime
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
            edge = 1.0 - ask  # potential profit if resolves YES
            if edge < config.EXPIRING_EDGE_THRESHOLD:
                return None
            size = self._size(edge)
            return OrderIntent(
                ticker=market.ticker, side="yes", count=size,
                limit_price=price_cents,
                reason=f"Expiry momentum: YES mid {mid:.0%}, {market.minutes_to_close:.0f}min left",
            )

        # Momentum: ride low-probability markets toward 0.0
        if mid <= 0.30:
            no_ask = 1.0 - bid
            price_cents = min(int(no_ask * 100) + 1, 95)
            edge = bid  # potential profit if resolves NO (= 1.0 - 0.0 - no_price ~ bid)
            if edge < config.EXPIRING_EDGE_THRESHOLD:
                return None
            size = self._size(edge)
            return OrderIntent(
                ticker=market.ticker, side="no", count=size,
                limit_price=price_cents,
                reason=f"Expiry momentum: NO mid {1-mid:.0%}, {market.minutes_to_close:.0f}min left",
            )

        # Mid-range: look for dislocated ask (panicked sellers)
        yes_edge = mid - ask
        no_edge = bid - mid

        best_edge = max(yes_edge, no_edge)
        if best_edge < config.EXPIRING_EDGE_THRESHOLD:
            return None

        if yes_edge >= no_edge:
            price_cents = min(int((ask + config.LIMIT_PRICE_BUFFER) * 100), 99)
            return OrderIntent(
                ticker=market.ticker, side="yes",
                count=self._size(yes_edge),
                limit_price=price_cents,
                reason=f"Expiry dislocation: YES ask {ask:.0%} < mid {mid:.0%}, {market.minutes_to_close:.0f}min",
            )
        else:
            no_price = 1.0 - bid + config.LIMIT_PRICE_BUFFER
            price_cents = min(int(no_price * 100), 99)
            return OrderIntent(
                ticker=market.ticker, side="no",
                count=self._size(no_edge),
                limit_price=price_cents,
                reason=f"Expiry dislocation: NO bid {1-bid:.0%} < mid {1-mid:.0%}, {market.minutes_to_close:.0f}min",
            )

    @staticmethod
    def _size(edge: float) -> int:
        """Scale size linearly with edge, capped."""
        base = config.DEFAULT_ORDER_SIZE
        scaled = int(base * (edge / config.EXPIRING_EDGE_THRESHOLD))
        return max(config.MIN_ORDER_SIZE, min(scaled, config.MAX_POSITION_PER_MARKET))
