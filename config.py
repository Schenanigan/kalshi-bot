"""
config.py — All tunable parameters for the Kalshi trading bot.

Loads from environment variables with sane defaults.
"""

import os
from dataclasses import dataclass, field


# ─── Defaults ────────────────────────────────────────────────────────────────

# Auth
DEFAULT_BASE_URL_DEMO = "https://api.elections.kalshi.com/trade-api/v2"
DEFAULT_BASE_URL_PROD = "https://api.elections.kalshi.com/trade-api/v2"

# Scanner
DEFAULT_EXPIRING_WINDOW_MINUTES = 60
WEATHER_SERIES_PREFIXES = [
    "KXHIGH",   # daily high temperature
    "KXLOW",    # daily low temperature
    "KXRAIN",   # precipitation
    "KXSNOW",   # snowfall
    "KXWIND",   # wind speed
]

# Strategy — expiring
EXPIRING_MIN_YES_PRICE   = 0.05
EXPIRING_MAX_YES_PRICE   = 0.95
EXPIRING_EDGE_THRESHOLD  = 0.06     # 6¢
LIMIT_PRICE_BUFFER       = 0.01     # 1¢

# Strategy — weather
WEATHER_EDGE_THRESHOLD   = 0.08     # 8¢

# Risk
DEFAULT_MAX_TRADE_DOLLARS      = 25.0
DEFAULT_MAX_OPEN_POSITIONS     = 10
DEFAULT_MAX_DAILY_LOSS_DOLLARS = 100.0
DEFAULT_MAX_POSITION_PER_MARKET = 50
DEFAULT_MAX_TOTAL_EXPOSURE     = 500.0

# Order sizing
MIN_ORDER_SIZE       = 1
DEFAULT_ORDER_SIZE   = 5
MAX_POSITION_PER_MARKET = 50
MAX_KELLY_FRACTION   = 0.25    # quarter-Kelly cap on position sizing

# Exit management
TRAILING_STOP_PCT    = 0.50    # exit if mark-to-market loss exceeds 50% of entry cost
PROFIT_TAKE_PCT      = 0.50    # take profit when 50%+ of remaining edge captured

# Scan
DEFAULT_SCAN_INTERVAL = 30
DEFAULT_MIN_VOLUME    = 0
DEFAULT_MAX_SPREAD    = 0.15


# ─── BotConfig ───────────────────────────────────────────────────────────────

@dataclass
class BotConfig:
    api_key: str = ""
    private_key_path: str = "kalshi_key.pem"
    demo: bool = True
    dry_run: bool = True

    # scanner
    expiring_within_minutes: int = DEFAULT_EXPIRING_WINDOW_MINUTES
    include_weather: bool = True
    include_expiring: bool = True
    min_volume: int = DEFAULT_MIN_VOLUME
    max_spread_dollars: float = DEFAULT_MAX_SPREAD

    # strategies
    active_strategies: list = field(default_factory=lambda: ["all"])

    # risk
    max_trade_dollars: float = DEFAULT_MAX_TRADE_DOLLARS
    max_open_positions: int = DEFAULT_MAX_OPEN_POSITIONS
    max_daily_loss_dollars: float = DEFAULT_MAX_DAILY_LOSS_DOLLARS
    allow_duplicate_tickers: bool = False

    # timing
    scan_interval_seconds: int = DEFAULT_SCAN_INTERVAL


def load_from_env() -> BotConfig:
    """Build a BotConfig from environment variables with sensible defaults."""
    return BotConfig(
        api_key=os.environ.get("KALSHI_API_KEY", ""),
        private_key_path=os.environ.get("KALSHI_KEY_PATH", "kalshi_key.pem"),
        demo=os.environ.get("KALSHI_DEMO", "true").lower() == "true",
        dry_run=os.environ.get("KALSHI_DRY_RUN", "true").lower() == "true",
        expiring_within_minutes=int(os.environ.get("KALSHI_EXPIRING_MINUTES", DEFAULT_EXPIRING_WINDOW_MINUTES)),
        include_weather=os.environ.get("KALSHI_INCLUDE_WEATHER", "true").lower() == "true",
        include_expiring=os.environ.get("KALSHI_INCLUDE_EXPIRING", "true").lower() == "true",
        min_volume=int(os.environ.get("KALSHI_MIN_VOLUME", DEFAULT_MIN_VOLUME)),
        max_spread_dollars=float(os.environ.get("KALSHI_MAX_SPREAD", DEFAULT_MAX_SPREAD)),
        active_strategies=os.environ.get("KALSHI_STRATEGIES", "all").split(","),
        max_trade_dollars=float(os.environ.get("KALSHI_MAX_TRADE", DEFAULT_MAX_TRADE_DOLLARS)),
        max_open_positions=int(os.environ.get("KALSHI_MAX_POSITIONS", DEFAULT_MAX_OPEN_POSITIONS)),
        max_daily_loss_dollars=float(os.environ.get("KALSHI_MAX_DAILY_LOSS", DEFAULT_MAX_DAILY_LOSS_DOLLARS)),
        allow_duplicate_tickers=os.environ.get("KALSHI_ALLOW_DUPES", "false").lower() == "true",
        scan_interval_seconds=int(os.environ.get("KALSHI_SCAN_INTERVAL", DEFAULT_SCAN_INTERVAL)),
    )
