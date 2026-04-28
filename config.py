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
WEATHER_EDGE_THRESHOLD   = 0.10     # 10¢ METAR threshold — direct observation
                                    # (was 15¢; after fees the effective bar was
                                    # ~17¢ and FV almost never fired)
WEATHER_EDGE_THRESHOLD_LATE = 0.06  # 6¢ METAR late (was 8¢)
WEATHER_NWS_EDGE_THRESHOLD = 0.18  # 18¢ NWS threshold (was 25¢)
WEATHER_NWS_EDGE_THRESHOLD_LATE = 0.12  # 12¢ NWS late (was 15¢)
WEATHER_MAX_PRICE_CENTS  = 75       # never buy a single leg above 75¢
WEATHER_MAX_BETWEEN_PRICE_CENTS = 20  # between buckets are narrow ranges where
                                    # model uncertainty is highest — cap lower
                                    # (high-priced legs have tiny edge but big
                                    # loss if wrong)
WEATHER_MAX_BETWEEN_PRICE_LATE_CENTS = 50  # loosen the cap in the last ~90min,
                                    # when observed temp has mostly crystallized
WEATHER_LATE_WINDOW_MINUTES = 90    # "late" == minutes_to_close <= this
# ASOS 5-min stations report temperature rounded to the nearest whole °C,
# then NWS converts that back to °F. Round-trip introduces up to ~0.9°F of
# error. When deciding whether a partition leg's outcome is already settled
# by METAR, treat the observation as a band ±this many °F: only kill a leg
# when observed_max - band > cap (or observed_min + band < floor). Prevents
# the bot from killing a winning leg whose true temperature is on the other
# side of a bucket boundary from what METAR shows.
METAR_OBSERVATION_BAND_DEGREES = 1.0

NWS_SIGMA_DEGREES        = 5.0     # NWS forecast uncertainty in °F
                                    # (was 3.0 — too tight, produced overconfident
                                    # signals that the market already prices in)
# When NWS deterministic μ disagrees with GEFS ensemble μ by more than this
# many °F, refuse to trade — one source is materially wrong and we shouldn't
# guess which. Inflating σ via quadrature (the default) handles small/medium
# gaps, but large gaps usually mean a stale forecast or a bad station mapping
# rather than calibration noise. Fail-closed: skip the market.
MAX_FORECAST_DISAGREEMENT_F = 8.0

# Strategy — partition arbitrage
# Real Kalshi weather events (KXHIGHNY-26APR11, etc.) are partitioned into
# mutually-exclusive, exhaustive markets: one tail (<X), several ranges
# (a-b), and one tail (>Y). Their YES probabilities must sum to 1, so if
# Σ yes_ask < 1 - buffer, buying every leg at ask is risk-free arb.
# Buffer covers: trading fees, slippage between observation and fill,
# and the chance that not every leg fills.
PARTITION_ARB_MIN_EDGE   = 0.08     # 8¢ minimum edge (raised from 5¢) — thin-edge
                                    # baskets were having their edge eaten by fees
                                    # + fill slippage, dragging net PnL negative
PARTITION_ARB_MIN_LEGS   = 3        # need at least 3 legs to call it a partition
PARTITION_ARB_MAX_LEG_PRICE = 90    # skip legs priced above 90¢ — near-certainties
                                    # add cost with negligible edge contribution
PARTITION_ARB_MAX_BASKET_SIZE = 100  # hard cap per leg. Raised from 20 once
                                    # paper-mode partial-fill realism + the
                                    # 0.85 partition-coverage floor were in
                                    # place; previously $3k of capital was
                                    # producing $14 of avg notional per fire.
                                    # 100 × ~5c avg ≈ $5-50 per basket.
PARTITION_ARB_TYPICAL_DEPTH = 25    # depth at which we deploy full target_size.
                                    # Baskets with min_depth below this get a
                                    # proportional shrink (volatility-aware
                                    # sizing): a 5-deep book gets 5/25 of the
                                    # bet. Stops thin-book quotes — the most
                                    # likely to be stale or get re-priced
                                    # before we can fill — from soaking up the
                                    # full per-trade dollar cap.
PARTITION_ARB_SIZE_PER_EDGE = 150   # target_size = int(edge × this). 10¢ → 15,
                                    # 30¢ → 45, 70¢ → cap. Raised from 60 — at
                                    # 90¢ edges (real partition arbs after the
                                    # coverage floor) the old cap left obvious
                                    # money on the table.
PARTITION_ARB_PRICE_BUFFER_CENTS = 2  # pay this many cents above the displayed
                                    # ask on each arb leg so the limit actually
                                    # crosses when depth is thin. Cost per basket
                                    # = buffer × n_legs; subtracted from the
                                    # edge check so thin baskets are filtered.
PARTITION_ARB_PRICE_BUFFER_CENTS_LIQUID = 1  # 1¢ buffer is enough on deep books;
                                    # saves ~5% of edge on 5-leg baskets.
PARTITION_ARB_LIQUID_DEPTH = 50     # min_depth ≥ this → use the liquid buffer
PARTITION_ARB_MAX_LEG_PRICE_LATE = 95  # raise the leg-price cap from 90c → 95c
                                    # when minutes_to_close ≤ LATE_WINDOW. Late
                                    # in the day a "near-cert" 92c leg paired
                                    # with cheap tails is a real arb; the
                                    # 90c cap was disqualifying obvious wins.
PARTITION_ARB_LATE_WINDOW_MINUTES = 90
PARTITION_ARB_ALLOW_MISSING_LEG = False  # strict 100% coverage only. Was True
                                    # with a 15¢ phantom-cost gate, but live
                                    # results showed adverse selection — the
                                    # highest-edge 1-missing baskets were cheap
                                    # precisely because the modal outcome sat
                                    # in the gap (e.g. TSATX APR22). Complete
                                    # partitions are mathematically guaranteed
                                    # +EV; 1-missing baskets weren't real arbs.
PARTITION_ARB_MISSING_LEG_PHANTOM_CENTS = 15  # conservative assumed cost of the
                                    # missing leg. Subtracted from edge before
                                    # the MIN_EDGE check, so thin arbs with a
                                    # missing bucket don't qualify. 15c chosen
                                    # to cover typical scanner-dropped between
                                    # buckets (which tend to be thinly-traded
                                    # mid-range, often 5-20c). If the missing
                                    # bucket hits at settle, we lose the full
                                    # basket cost; the phantom just gates entry.
CERT_WINNER_ENABLED = True           # single-leg monotonic arb: when METAR
                                    # proves a leg's outcome is certain
                                    # (KXHIGH more_than floor<observed_max, or
                                    # KXLOW less_than cap>observed_min), buy
                                    # that leg at ask for guaranteed $1 payout.
                                    # Complementary to partition arb — fires
                                    # when the full partition doesn't clear
                                    # the edge threshold but one leg alone has
                                    # meaningful room above fees+buffer.
CERT_WINNER_MIN_EDGE_CENTS = 3       # minimum net edge to bother (after fees
                                    # and 2¢ buffer). 3¢ is tight but these
                                    # are risk-free so any clean positive edge
                                    # is worth taking.
CERT_WINNER_MAX_PRICE_CENTS = 97     # never pay more than 97¢ — the arithmetic
                                    # makes 98-99¢ bets net-negative after
                                    # fees, and Kalshi rounds weird at 99¢.
# NWS-based probabilistic dead-leg trim. METAR can only kill legs intra-day
# (max temp can only rise, min can only fall — monotonic). NWS forecasts
# extend that reach to 24-48h out at the cost of probabilistic uncertainty:
# a leg with NWS-implied P(win) below this threshold is treated as dead
# even when METAR can't speak yet. Cap the trim to PROB_HORIZON_HOURS so
# we don't trim 5-day-out legs where NWS sigma is too wide to be useful.
PARTITION_ARB_NWS_DEAD_LEG_PROB = 0.02    # P(win) below this → dead. Tightened
                                    # from 4% after KXHIGHTSATX-26APR26 lost
                                    # $10.95: trim killed B89.5 at 33h out
                                    # claiming P<4%, leg actually won. Stronger
                                    # evidence required to override the market.
PARTITION_ARB_NWS_DEAD_LEG_HOURS = 24     # only trim within this horizon
                                    # (was 48 — NWS skill at 33-48h is too low
                                    # to reliably zero out a leg the market
                                    # is still pricing at non-trivial cents)
PARTITION_ARB_NWS_DEAD_LEG_MAX_MKT_PRICE = 0.05  # never trim a leg the market
                                    # prices above this. If the orderbook says
                                    # >5% chance, our sub-2% model claim is
                                    # contradicted by collective wisdom — fail
                                    # closed and skip the trim. This is the
                                    # primary safety net: the SATX loss
                                    # would have been blocked here (B89.5 was
                                    # trading near 26¢ when our model trimmed
                                    # it as <4%).
PARTITION_ARB_SKIP_DEAD_LEGS = True   # when METAR already rules out a leg's
                                    # outcome (e.g. observed high 84°F makes
                                    # every bucket with cap<84 impossible),
                                    # skip buying that leg. The partition
                                    # guarantee still holds — exactly one of
                                    # the live legs will win — and basket cost
                                    # drops by the dead legs' ask sum.
# Rolling kill-switch: if the last N settled basket outcomes are all losses,
# pause new basket emission for the rest of the local day. A bad METAR
# mapping, stale NWS forecast, or partition gap feed will bleed capital
# faster than the daily-loss cap can catch it. Three straight losing
# baskets is strong signal something systemic is broken — better to pause
# than to keep firing until the $250 daily cap trips.
BASKET_KILL_SWITCH_N = 3               # consecutive losses to trip the switch
BASKET_KILL_SWITCH_MIN_LOSS = 1.0      # $ — ignore break-even outcomes
                                        # (rounding noise shouldn't count)
PARTITION_ARB_EVENT_COOLDOWN_S = 300  # after attempting a basket on an event,
                                    # suppress re-builds on that event for 5min.
                                    # Prevents the same basket re-firing every
                                    # 30s loop when fills are slow/partial.

# Risk
# Defaults sized for paper-trading exploration. For LIVE shakedown, pass
# tighter caps via env vars — e.g.
#   KALSHI_MAX_POSITIONS=30 KALSHI_MAX_TRADE=5 KALSHI_MAX_DAILY_LOSS=50
DEFAULT_MAX_TRADE_DOLLARS      = 100.0
DEFAULT_MAX_OPEN_POSITIONS     = 200
DEFAULT_MAX_DAILY_LOSS_DOLLARS = 250.0
DEFAULT_MAX_POSITION_PER_MARKET = 50

# Order sizing
# Partition-arb baskets use edge-scaled sizing (see PARTITION_ARB_SIZE_PER_EDGE
# above): target_size = int(edge × SIZE_PER_EDGE), clamped by depth and a
# hard cap. This isn't classic Kelly — the arb payout is deterministic given
# a complete partition, so the bet-fraction question collapses to "how much
# capital is free and how much does the book display." The SIZE_PER_EDGE
# heuristic additionally penalizes thin edges (where a stale quote might be
# fake) by giving them smaller allocations.
MIN_ORDER_SIZE       = 1
DEFAULT_ORDER_SIZE   = 3        # reduced from 5 — smaller bets while model is unproven
MAX_POSITION_PER_MARKET = 20    # reduced from 50
# Quarter-Kelly cap used by ExpiryMomentum sizing (non-arb strategies
# where the bet is variance-bearing and Kelly actually applies).
MAX_KELLY_FRACTION   = 0.15

# Exit management
TRAILING_STOP_PCT    = 0.50    # exit if mark-to-market loss exceeds 50% of entry cost
MIN_EXIT_PRICE_CENTS = 2        # don't submit exits at 1c — they rarely fill,
                                # create zombie resting orders that block new
                                # entries, and cost fees on any fills. Below
                                # this the position is cheaper to let expire.
WEATHER_TAKE_PROFIT_BID_CENTS = 80  # lock in profit when a weather leg's bid reaches
                                    # this price (lowered from 90¢). Legs often
                                    # rip into the 80s and then fade before
                                    # reaching 90¢, especially on basket legs
                                    # where liquidity thins out near settlement.
WEATHER_TAKE_PROFIT_MIN_GAIN_CENTS = 10  # don't pay fees on a flat round-trip

# Kalshi fees — per-fill formula: ceil(rate × contracts × price × (1 − price))
# See https://kalshi.com/fee-schedule
TAKER_FEE_RATE = 0.07          # 7% of notional variance
MAKER_FEE_RATE = 0.0175        # 1.75% of notional variance

# Order management
STALE_PRICE_DRIFT_CENTS = 5   # cancel resting orders if market moves >5c from limit

# Paper trading
PAPER_STARTING_BALANCE = 3000.0

# Scan
DEFAULT_SCAN_INTERVAL = 30      # 30s balances arb responsiveness against 429 rate-
                                # limiting; at 141+ positions the 15s cadence was
                                # triggering multiple 429s per loop
# Settle-time fast scan: in the final stretch before a tracked weather event
# closes, METAR ticks can collapse the basket arb in seconds. Drop the loop
# cadence to FAST_SCAN_INTERVAL when ANY candidate is within
# FAST_SCAN_MINUTES_TO_CLOSE of close. The loop body's 429-backoff makes
# 5s safe even with hundreds of positions.
FAST_SCAN_INTERVAL_SECONDS = 5
FAST_SCAN_MINUTES_TO_CLOSE = 30
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
