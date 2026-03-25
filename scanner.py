"""
scanner.py — Finds tradeable markets.

Two scan modes:
  1. Expiring: open markets closing within N minutes
  2. Weather:  open markets whose series ticker starts with a weather prefix

The module-level scan() function is the main entry point, called by bot.py.
"""

import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field

from config import WEATHER_SERIES_PREFIXES

log = logging.getLogger(__name__)


@dataclass
class CandidateMarket:
    """A market that passed the scanner filters."""
    ticker: str
    title: str
    series: str
    yes_bid: float          # best bid for YES (0.0–1.0)
    yes_ask: float          # best ask for YES (0.0–1.0)
    mid: float              # midpoint
    volume: int             # contracts traded last 24h
    close_time: datetime
    minutes_to_close: float
    tags: list[str] = field(default_factory=list)   # "expiring", "weather"
    raw: dict = field(default_factory=dict)          # full API response


def scan(
    raw_markets: list[dict],
    expiring_within_minutes: int = 60,
    include_weather: bool = True,
    include_expiring: bool = True,
    min_volume: int = 0,
    max_spread: float = 0.40,
) -> list[CandidateMarket]:
    """
    Filter raw market dicts from the API into tradeable candidates.

    Returns a list of CandidateMarket objects with .tags indicating
    why each market was selected ("expiring" and/or "weather").
    """
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(minutes=expiring_within_minutes)

    seen: set[str] = set()
    targets: list[CandidateMarket] = []

    for m in raw_markets:
        ticker = m.get("ticker", "")
        if not ticker or ticker in seen:
            continue

        parsed = _parse(m, now)
        if parsed is None:
            continue

        # Check spread
        spread = parsed.yes_ask - parsed.yes_bid
        if spread > max_spread:
            log.debug("Skipping %s — spread %.0f¢ too wide", ticker, spread * 100)
            continue

        # Check volume
        if parsed.volume < min_volume:
            continue

        # Determine tags
        tags: list[str] = []
        if include_expiring and parsed.close_time <= cutoff:
            tags.append("expiring")
        if include_weather and any(parsed.series.upper().startswith(p) for p in WEATHER_SERIES_PREFIXES):
            tags.append("weather")

        if not tags:
            continue

        parsed.tags = tags
        targets.append(parsed)
        seen.add(ticker)

    log.info(
        "Found %d candidates (%d expiring, %d weather) from %d markets",
        len(targets),
        sum(1 for t in targets if "expiring" in t.tags),
        sum(1 for t in targets if "weather" in t.tags),
        len(raw_markets),
    )
    return targets


def _parse(m: dict, now: datetime) -> CandidateMarket | None:
    """Parse a raw market dict into CandidateMarket. Returns None if invalid."""
    try:
        ticker = m["ticker"]
        series = m.get("series_ticker", "")
        title = m.get("title", ticker)

        # Prices: Kalshi returns cents (int), convert to 0–1
        yes_bid = m.get("yes_bid", 0) / 100
        yes_ask = m.get("yes_ask", 100) / 100

        # Skip markets with no quotes
        if yes_bid == 0 and yes_ask == 1.0:
            return None

        mid = (yes_bid + yes_ask) / 2

        # Parse close time
        close_str = m.get("close_time") or m.get("expiration_time", "")
        if not close_str:
            return None
        close_time = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        minutes_to_close = (close_time - now).total_seconds() / 60

        # Skip already closed
        if minutes_to_close < 0:
            return None

        volume = m.get("volume_24h", 0) or 0

        return CandidateMarket(
            ticker=ticker,
            title=title,
            series=series,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            mid=mid,
            volume=volume,
            close_time=close_time,
            minutes_to_close=minutes_to_close,
            tags=[],
            raw=m,
        )

    except (KeyError, ValueError, TypeError) as e:
        log.debug("Failed to parse market %s: %s", m.get("ticker"), e)
        return None
