"""
Kalshi Trading Bot — Main Loop  (with metrics dashboard)
=========================================================

Run:
    python bot.py

Open dashboard.html in any browser once running.

Environment variables (see config.py for all options):
    KALSHI_API_KEY    — your Kalshi API key UUID
    KALSHI_KEY_PATH   — path to RSA private key PEM
    KALSHI_DEMO       — true/false  (default: true)
    KALSHI_DRY_RUN    — true/false  (default: true)
"""

import csv
import os
import time
import random
import signal
import logging
import logging.handlers
import sys
import datetime

import config
from config import BotConfig, load_from_env
from client import KalshiClient
from scanner import scan, build_exit_candidates
from strategy import FairValueStrategy, ExpiryMomentumStrategy, BaseStrategy, OrderIntent, PositionInfo
from risk import RiskManager, RiskConfig
from metrics import MetricsServer
from paper import PaperTrader

# ── Logging ───────────────────────────────────────────────────────────────────
# Only attach a StreamHandler when stdout is an interactive terminal.
# When the bot is backgrounded (nohup python bot.py >> bot.log 2>&1),
# stdout is redirected to bot.log, and a StreamHandler would double every
# line — once via stdout→bot.log and again via the RotatingFileHandler.
_log_handlers: list = []
if sys.stdout.isatty():
    _log_handlers.append(logging.StreamHandler(sys.stdout))
try:
    # Rotate at 20MB, keep 5 backups (~100MB total) — previously an unbounded
    # FileHandler that grew ~65MB/day.
    _log_handlers.append(logging.handlers.RotatingFileHandler(
        "bot.log", maxBytes=20 * 1024 * 1024, backupCount=5,
    ))
except OSError:
    pass  # skip file logging if not writable (e.g. in containers)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=_log_handlers,
)
log = logging.getLogger("bot")


EQUITY_CSV = "equity.csv"
PAPER_STATE_PATH = "paper_state.json"
BASKET_TRACKER_PATH = "basket_tracker.json"
DAY_STATE_PATH = "day_state.json"


def _save_day_state(date_iso: str, day_start_portfolio: float,
                    path: str = DAY_STATE_PATH):
    """Persist the UTC date and portfolio snapshot so a mid-day restart
    doesn't reset the daily-loss circuit breaker. Without this, a bot that
    lost money before a crash would see day_start = current depressed
    portfolio and happily keep trading past max_daily_loss."""
    try:
        import json
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"date": date_iso, "day_start_portfolio": day_start_portfolio}, f)
        os.replace(tmp, path)
    except Exception as e:
        log.warning("Failed to save day state: %s", e)


def _load_day_state(path: str = DAY_STATE_PATH) -> dict:
    try:
        import json
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("Failed to load day state: %s — starting fresh", e)
        return {}
_EQUITY_HEADER = ["timestamp", "balance", "portfolio", "daily_pnl",
                  "realized_pnl", "unrealized_pnl", "open_positions", "loop"]


def _save_basket_tracker(tracker: dict, path: str = BASKET_TRACKER_PATH):
    """Persist live-mode basket tracker so we can reconcile across restarts.

    Crash mid-session without this leaves orphan live orders invisible to
    the reconciler. File is tiny (< 1 KB normally); write is atomic via
    tmp+rename to prevent partial-write corruption.
    """
    try:
        import json
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(tracker, f)
        os.replace(tmp, path)
    except Exception as e:
        log.warning("Failed to save basket tracker: %s", e)


def _load_basket_tracker(path: str = BASKET_TRACKER_PATH) -> dict:
    try:
        import json
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("Failed to load basket tracker: %s — starting empty", e)
        return {}


def _append_equity_row(row: list):
    new_file = not os.path.exists(EQUITY_CSV)
    try:
        with open(EQUITY_CSV, "a", newline="") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(_EQUITY_HEADER)
            w.writerow(row)
    except OSError as e:
        log.debug("equity csv write failed: %s", e)

# ── Graceful shutdown ─────────────────────────────────────────────────────────
_running = True

def _handle_signal(sig, frame):
    global _running
    log.info("Shutdown signal received — finishing current loop…")
    _running = False

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Simulation mode ───────────────────────────────────────────────────────────

def generate_fake_markets() -> list[dict]:
    """Generate synthetic markets for testing the full pipeline."""
    now = datetime.datetime.now(datetime.timezone.utc)
    markets = []

    # Weather markets (various cities / temp thresholds / types)
    weather_tickers = [
        ("KXHIGH-26MAR26-SFO-T68", "Will the high in San Francisco be above 68°F on Mar 26?", "KXHIGH"),
        ("KXHIGH-26MAR26-NYC-T55", "Will the high in New York be above 55°F on Mar 26?", "KXHIGH"),
        ("KXRAIN-26MAR26-CHI", "Will it rain in Chicago on Mar 26?", "KXRAIN"),
        ("KXLOW-26MAR26-MIA-T60", "Will the low in Miami be above 60°F on Mar 26?", "KXLOW"),
        ("KXHIGH-27MAR26-LAX-T75", "Will the high in Los Angeles be above 75°F on Mar 27?", "KXHIGH"),
        ("KXHIGH-26MAR26-DEN-T62", "Will the high in Denver be above 62°F on Mar 26?", "KXHIGH"),
        ("KXRAIN-26MAR26-SEA", "Will it rain in Seattle on Mar 26?", "KXRAIN"),
        ("KXHIGH-26MAR26-ATL-T70", "Will the high in Atlanta be above 70°F on Mar 26?", "KXHIGH"),
        ("KXLOW-26MAR26-BOS-T38", "Will the low in Boston be above 38°F on Mar 26?", "KXLOW"),
        ("KXHIGH-26MAR26-HOU-T78", "Will the high in Houston be above 78°F on Mar 26?", "KXHIGH"),
        ("KXSNOW-26MAR26-DEN", "Will it snow in Denver on Mar 26?", "KXSNOW"),
        ("KXSNOW-26MAR26-CHI", "Will it snow in Chicago on Mar 26?", "KXSNOW"),
        ("KXWIND-26MAR26-NYC", "Will wind exceed 25 mph in New York on Mar 26?", "KXWIND"),
        ("KXWIND-26MAR26-PHX", "Will wind exceed 20 mph in Phoenix on Mar 26?", "KXWIND"),
    ]

    # Partition-arb test: a real-shape KXHIGHNY-style event with one
    # <X tail, four range buckets, and one >Y tail. Σ yes_ask = 0.94 < 1
    # so the partition-sum strategy should detect ~6¢ of basket edge
    # and emit a YES intent on every leg.
    close_dt_arb = now + datetime.timedelta(hours=12)
    arb_event = "KXHIGHNY-FAKEPART"

    def _partition_leg(suffix, sub_title, bid_d, ask_d, strike_type):
        return {
            "ticker":           f"{arb_event}-{suffix}",
            "title":            f"Will the high temp in NYC be {sub_title}?",
            "event_ticker":     arb_event,
            "yes_bid_dollars":  bid_d,
            "yes_ask_dollars":  ask_d,
            "yes_bid_size_fp":  20,
            "yes_ask_size_fp":  20,
            "volume_24h_fp":    500,
            "close_time":       close_dt_arb.isoformat(),
            "status":           "active",
            "strike_type":      strike_type,
        }

    arb_markets = [
        _partition_leg("T61",   "<61°",   0.08, 0.09, "less_than"),
        _partition_leg("B61.5", "61-62°", 0.21, 0.22, "between"),
        _partition_leg("B63.5", "63-64°", 0.31, 0.33, "between"),
        _partition_leg("B65.5", "65-66°", 0.20, 0.21, "between"),
        _partition_leg("B67.5", "67-68°", 0.06, 0.07, "between"),
        _partition_leg("T68",   ">68°",   0.01, 0.02, "more_than"),
        # Σ yes_ask = 0.09+0.22+0.33+0.21+0.07+0.02 = 0.94  → 6¢ basket edge
    ]
    markets.extend(arb_markets)
    for ticker, title, series in weather_tickers:
        yes_bid = random.randint(15, 85)
        spread = random.randint(2, 8)
        close_dt = now + datetime.timedelta(hours=random.randint(4, 48))
        markets.append({
            "ticker": ticker,
            "title": title,
            "series_ticker": series,
            "yes_bid": yes_bid,
            "yes_ask": yes_bid + spread,
            "volume_24h": random.randint(50, 2000),
            "close_time": close_dt.isoformat(),
            "status": "open",
        })

    # Expiring markets (closing within 15-60 min, various prices)
    expiring_tickers = [
        ("KXBTC-25MAR26-T88000", "Will Bitcoin be above $88,000 on Mar 25?"),
        ("KXETH-25MAR26-T2100", "Will Ethereum be above $2,100 on Mar 25?"),
        ("KXSP500-25MAR26-T5700", "Will S&P 500 close above 5,700 on Mar 25?"),
        ("KXNASDAQ-25MAR26-T18000", "Will NASDAQ close above 18,000 on Mar 25?"),
        ("KXGOLD-25MAR26-T3050", "Will Gold be above $3,050 on Mar 25?"),
        ("KXTREAS-25MAR26-T4.3", "Will 10Y Treasury yield be above 4.3%?"),
        ("KXFED-25MAR26-HOLD", "Will Fed hold rates at next meeting?"),
    ]
    for ticker, title in expiring_tickers:
        yes_bid = random.choice([random.randint(5, 25), random.randint(72, 94)])  # bimodal: low or high
        spread = random.randint(1, 5)
        mins_left = random.randint(8, 55)
        close_dt = now + datetime.timedelta(minutes=mins_left)
        markets.append({
            "ticker": ticker,
            "title": title,
            "series_ticker": ticker.split("-")[0],
            "yes_bid": yes_bid,
            "yes_ask": min(yes_bid + spread, 99),
            "volume_24h": random.randint(100, 5000),
            "close_time": close_dt.isoformat(),
            "status": "open",
        })

    return markets


SIMULATE = "--simulate" in sys.argv
PAPER    = "--paper" in sys.argv


# ── Strategy factory ──────────────────────────────────────────────────────────

def build_strategies(names: list[str]) -> list[BaseStrategy]:
    # Only partition-arb runs. FairValueStrategy is loaded because it owns
    # the evaluate_batch arb path AND the exit logic for any legacy FV legs
    # still held — its single-leg evaluate() is disabled inside the class.
    # ExpiryMomentumStrategy is disabled: historic P&L showed it consistently
    # loses money, so it's excluded from the factory entirely.
    registry = {
        "fair_value":      FairValueStrategy(),
        # "expiry_momentum": ExpiryMomentumStrategy(),  # disabled — loses money
    }
    if "all" in names:
        return list(registry.values())
    strats = []
    for n in names:
        if n in registry:
            strats.append(registry[n])
        else:
            log.warning("Unknown strategy '%s' — skipping", n)
    return strats


# ── Order execution ───────────────────────────────────────────────────────────

def execute(
    client: KalshiClient,
    intent: OrderIntent,
    risk: RiskManager,
    dry_run: bool,
    metrics: MetricsServer,
    strategy_name: str,
    placed_order_ids: set | None = None,
    paper: PaperTrader | None = None,
    basket_tracker: dict | None = None,
) -> bool:
    rejection = risk.approve(intent)
    if rejection:
        # Dedup/cap rejections are expected & high-volume; only surface real
        # risk events (daily loss, cost caps) at WARNING level.
        benign = ("Already traded" in rejection) or ("Max logical positions" in rejection)
        (log.debug if benign else log.warning)("RISK BLOCK %s — %s", intent.ticker, rejection)
        metrics.push_order(
            ticker=intent.ticker, side=intent.side, count=intent.count,
            price_cents=intent.limit_price, status="blocked",
            reason=rejection, strategy=strategy_name,
        )
        metrics.push_log(f"BLOCKED {intent.ticker}: {rejection}")
        return False

    trade_cost = (intent.limit_price / 100) * intent.count

    if paper is not None:
        ok = paper.submit_order(intent, strategy_name)
        if ok:
            metrics.push_order(
                ticker=intent.ticker, side=intent.side, count=intent.count,
                price_cents=intent.limit_price, status="paper",
                reason=intent.reason, strategy=strategy_name,
            )
        return ok

    label = "DRY" if dry_run else "LIVE"
    log.info(
        "[%s] %s %s×%d @ %dc (~$%.2f) | %s",
        label, intent.ticker, intent.side.upper(), intent.count,
        intent.limit_price, trade_cost, intent.reason,
    )

    if dry_run:
        metrics.push_order(
            ticker=intent.ticker, side=intent.side, count=intent.count,
            price_cents=intent.limit_price, status="dry_run",
            reason=intent.reason, strategy=strategy_name,
        )
        metrics.push_log(f"DRY {intent.ticker} {intent.side.upper()} ×{intent.count} @ {intent.limit_price}¢")
        return True

    try:
        result = client.place_order(
            ticker=intent.ticker,
            side=intent.side,
            count=intent.count,
            limit_price=intent.limit_price,
            action=intent.action,
        )
        order_id = result.get("order", {}).get("order_id", "?")
        log.info("Order placed: %s", order_id)
        if placed_order_ids is not None and order_id != "?":
            placed_order_ids.add(order_id)
        if (basket_tracker is not None and intent.basket_id
                and order_id != "?"):
            entry = basket_tracker.setdefault(
                intent.basket_id, {"ts": time.time(), "legs": []},
            )
            entry["legs"].append([order_id, intent.ticker, intent.side])
            _save_basket_tracker(basket_tracker)
        metrics.push_order(
            ticker=intent.ticker, side=intent.side, count=intent.count,
            price_cents=intent.limit_price, status="placed",
            reason=intent.reason, strategy=strategy_name,
        )
        metrics.push_log(f"PLACED {intent.ticker} id={order_id}")
        return True
    except Exception as e:
        log.error("Order failed for %s: %s", intent.ticker, e)
        metrics.push_log(f"ERROR {intent.ticker}: {e}")
        return False


# ── Resting order management ─────────────────────────────────────────────────

def manage_resting_orders(
    client: KalshiClient,
    candidates: list,
    placed_ids: set,
    metrics: MetricsServer,
) -> tuple[int, int, int]:
    """Check resting orders: cancel stale ones, detect fills.

    Only called in live (non-dry-run, non-simulate) mode.
    Returns (resting_count, cancelled_count, filled_count).
    """
    try:
        resting = client.get_orders(status="resting")
    except Exception as e:
        log.warning("Failed to fetch resting orders: %s", e)
        return 0, 0, 0

    resting_ids = {o.get("order_id", "") for o in resting}

    # Detect fills: orders we placed that are no longer resting
    newly_filled = placed_ids - resting_ids
    filled_count = len(newly_filled)
    for oid in newly_filled:
        log.info("Order %s filled or settled", oid)
        metrics.push_log(f"FILLED order {oid}")
    placed_ids -= newly_filled

    # Cancel stale resting orders (price drifted too far from limit)
    candidate_prices = {c.ticker: c for c in candidates}
    cancelled = 0

    for order in resting:
        ticker = order.get("ticker", "")
        order_id = order.get("order_id", "")
        side = order.get("side", "")

        if side == "yes":
            limit_cents = order.get("yes_price", 0)
        else:
            limit_cents = order.get("no_price", 0)

        if ticker not in candidate_prices:
            continue  # can't assess drift without current prices

        market = candidate_prices[ticker]
        yes_mid_cents = int(market.mid * 100)
        if side == "yes":
            drift = abs(limit_cents - yes_mid_cents)
        else:
            drift = abs(limit_cents - (100 - yes_mid_cents))

        if drift <= config.STALE_PRICE_DRIFT_CENTS:
            continue

        log.info(
            "Cancelling stale order %s on %s: limit=%dc, drift=%dc",
            order_id, ticker, limit_cents, drift,
        )
        try:
            client.cancel_order(order_id)
        except Exception as e:
            log.warning("Cancel failed for %s: %s", order_id, e)
            continue
        cancelled += 1
        placed_ids.discard(order_id)
        metrics.push_order(
            ticker=ticker, side=side,
            count=order.get("remaining_count", 0),
            price_cents=limit_cents, status="cancelled_stale",
            reason=f"price drift {drift}c", strategy="order_mgmt",
        )

    metrics.push_order_lifecycle(
        resting=len(resting) - cancelled,
        new_fills=filled_count,
        new_cancels=cancelled,
    )

    if resting or filled_count:
        log.info(
            "Orders: %d resting, %d stale cancelled, %d filled",
            len(resting) - cancelled, cancelled, filled_count,
        )

    return len(resting), cancelled, filled_count


# ── Basket fill reconciliation ────────────────────────────────────────────────

def _order_filled_count(order: dict) -> int:
    """Best-effort filled quantity from a Kalshi order dict.

    Kalshi's schema has varied across API versions: some responses expose
    ``remaining_count``, others ``taker_fill_count`` + ``maker_fill_count``,
    others just ``filled_count``. Try each in order — worst case we fall
    back to the original ``count`` which over-sells slightly but the
    place_order call still caps at real position size.
    """
    try:
        if "remaining_count" in order and "count" in order:
            total = int(order.get("count") or 0)
            remaining = int(order.get("remaining_count") or 0)
            return max(0, total - remaining)
        for key in ("filled_count", "fill_count", "total_fill_count"):
            if order.get(key) is not None:
                return int(order[key])
        taker = int(order.get("taker_fill_count") or 0)
        maker = int(order.get("maker_fill_count") or 0)
        if taker or maker:
            return taker + maker
        return int(order.get("count") or 0)
    except (TypeError, ValueError):
        return 0


BASKET_RECONCILE_TIMEOUT_S = 20   # after this, cancel any still-resting legs
                                   # of a basket where some siblings have filled
BASKET_STALE_TIMEOUT_S = 90        # fully-unfilled baskets after this are
                                   # cancelled too — edge is probably gone


def reconcile_baskets(
    client: KalshiClient,
    basket_tracker: dict,
    placed_ids: set,
    metrics: MetricsServer,
) -> None:
    """Ensure partition-arb baskets fill atomically.

    Submitting each leg as an independent limit order means any combination
    of fill / rest / reject is possible. A partially-filled basket is no
    longer arbitrage — it's a directional bet on the uncovered legs.

    This function checks each tracked basket and:
      - If some legs filled AND some still resting past RECONCILE_TIMEOUT:
        cancel the resting legs and log the broken basket. Better to eat
        the partial exposure than let it compound across scans.
      - If NO legs filled after STALE_TIMEOUT: the arb is gone; cancel all.
      - If fully filled or fully cancelled: drop from tracking.
    """
    if not basket_tracker:
        return

    try:
        resting = client.get_orders(status="resting")
    except Exception as e:
        log.warning("reconcile_baskets: failed to fetch resting orders: %s", e)
        return
    resting_ids = {o.get("order_id", "") for o in resting}

    # Fetched lazily when we actually see a partial basket — avoids a second
    # API round-trip on the common all-resting / all-filled paths.
    filled_by_id: dict | None = None

    def _leg_tuple(leg):
        # Legs persisted before side-tracking was added are 2-tuples.
        if len(leg) >= 3:
            return leg[0], leg[1], leg[2]
        return leg[0], leg[1], None

    now = time.time()
    mutated = False
    for basket_id in list(basket_tracker.keys()):
        entry = basket_tracker[basket_id]
        legs = entry["legs"]
        age = now - entry["ts"]

        leg_tuples = [_leg_tuple(l) for l in legs]
        still_resting = [(oid, tkr, side) for oid, tkr, side in leg_tuples if oid in resting_ids]
        gone = [(oid, tkr, side) for oid, tkr, side in leg_tuples if oid not in resting_ids]
        n_resting = len(still_resting)
        n_filled_or_gone = len(legs) - n_resting

        if n_resting == 0:
            # Either fully filled or cancelled — nothing to reconcile
            del basket_tracker[basket_id]
            mutated = True
            continue

        partial = n_filled_or_gone > 0 and n_resting > 0

        if partial and age >= BASKET_RECONCILE_TIMEOUT_S:
            log.warning(
                "BASKET PARTIAL %s: %d/%d legs filled, cancelling %d orphan legs",
                basket_id, n_filled_or_gone, len(legs), n_resting,
            )
            for oid, tkr, _side in still_resting:
                try:
                    client.cancel_order(oid)
                    placed_ids.discard(oid)
                    metrics.push_log(f"BASKET CANCEL orphan {tkr} id={oid}")
                except Exception as e:
                    log.warning("reconcile: cancel failed for %s: %s", oid, e)

            # Unhedge filled legs: an isolated filled leg is a naked directional
            # bet, which is the opposite of what a partition-arb basket is meant
            # to be. Close each at best bid so we eat a small spread rather than
            # carry the exposure to settle.
            if filled_by_id is None:
                try:
                    filled_orders = client.get_orders(status="filled")
                    filled_by_id = {o.get("order_id", ""): o for o in filled_orders}
                except Exception as e:
                    log.warning("reconcile: failed to fetch filled orders: %s", e)
                    filled_by_id = {}
            for oid, tkr, side in gone:
                order = filled_by_id.get(oid)
                if not order or not side:
                    continue
                filled_count = _order_filled_count(order)
                if filled_count <= 0:
                    continue
                try:
                    ob = client.get_orderbook(tkr, depth=1)
                    book = ob.get("orderbook") or {}
                    bids = book.get("yes") if side == "yes" else book.get("no")
                    if not bids:
                        log.warning(
                            "reconcile: no %s bid to unhedge %s — leaving naked",
                            side, tkr,
                        )
                        continue
                    best_bid_c = int(bids[0][0])
                    # Cross by 1c to ensure the close fills; never below 1c.
                    close_price = max(1, best_bid_c - 1)
                    client.place_order(
                        ticker=tkr, side=side, count=filled_count,
                        limit_price=close_price, action="sell",
                    )
                    log.warning(
                        "BASKET UNHEDGE %s: sold %d %s @ %dc",
                        tkr, filled_count, side, close_price,
                    )
                    metrics.push_log(f"BASKET UNHEDGE {tkr} x{filled_count} @ {close_price}c")
                except Exception as e:
                    log.warning("reconcile: unhedge failed for %s: %s", tkr, e)
            del basket_tracker[basket_id]
            mutated = True
            continue

        if not partial and age >= BASKET_STALE_TIMEOUT_S:
            log.info(
                "BASKET STALE %s: no fills after %.0fs, cancelling all %d legs",
                basket_id, age, len(legs),
            )
            for oid, tkr, _side in still_resting:
                try:
                    client.cancel_order(oid)
                    placed_ids.discard(oid)
                except Exception as e:
                    log.warning("reconcile: cancel failed for %s: %s", oid, e)
            del basket_tracker[basket_id]
            mutated = True

    if mutated:
        _save_basket_tracker(basket_tracker)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run(cfg: BotConfig):
    log.info("=" * 60)
    log.info("Kalshi Bot starting")
    mode = "SIMULATE" if SIMULATE else "PAPER" if PAPER else "DEMO" if cfg.demo else "PRODUCTION"
    log.info("  Mode      : %s", mode)
    log.info("  Dry run   : %s", cfg.dry_run)
    log.info("  Strategies: %s", cfg.active_strategies)
    log.info("  Interval  : %ds", cfg.scan_interval_seconds)
    try:
        import sys as _sys, os as _os
        _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), "scripts"))
        from calibration import summarize as _cal_summarize
        _cal = _cal_summarize(".")
        if _cal:
            level = log.warning if _cal["ece"] > 0.10 else log.info
            level(
                "  Calibration: n=%d brier=%.4f ece=%.4f%s",
                _cal["n"], _cal["brier"], _cal["ece"],
                "  WARN ECE>0.10" if _cal["ece"] > 0.10 else "",
            )
        from strategy import recompute_sigma_multiplier as _rsm
        _rec = _rsm()
        if _rec:
            log.info(
                "  σ-recal:    multiplier=%.3f gap=%+.3f n=%d",
                _rec["multiplier"], _rec["gap"], _rec["n_pairs"],
            )
    except Exception as _e:
        log.debug("calibration summary failed: %s", _e)
    log.info("=" * 60)

    if not cfg.api_key and not SIMULATE and not PAPER:
        # Previously this exited immediately, which caused 4+ crashes/day when
        # the env propagation lagged. Wait and re-read env up to ~10 min.
        backoff = 15
        for attempt in range(40):
            log.warning("KALSHI_API_KEY is not set — retry %d in %ds", attempt + 1, backoff)
            time.sleep(backoff)
            cfg = load_from_env()
            if cfg.api_key:
                log.info("KALSHI_API_KEY loaded — continuing")
                break
            backoff = min(backoff + 15, 60)
        else:
            log.error("KALSHI_API_KEY still not set after retries. Exiting.")
            sys.exit(1)

    metrics = MetricsServer()
    metrics.start()
    metrics.push_status(
        running=True, demo=cfg.demo, dry_run=cfg.dry_run,
        strategies=cfg.active_strategies,
        started_at=datetime.datetime.utcnow().strftime("%H:%M:%S UTC"),
    )
    metrics.push_log("Bot started")

    client    = KalshiClient(cfg.api_key or "simulate", cfg.private_key_path, demo=cfg.demo) if not SIMULATE else None
    paper     = PaperTrader(config.PAPER_STARTING_BALANCE) if PAPER else None
    if paper is not None:
        paper.load_state(PAPER_STATE_PATH)
    risk      = RiskManager(RiskConfig(
        max_trade_dollars      = cfg.max_trade_dollars,
        max_open_positions     = cfg.max_open_positions,
        max_daily_loss_dollars = cfg.max_daily_loss_dollars,
        allow_duplicate_tickers= cfg.allow_duplicate_tickers,
    ))
    strategies = build_strategies(cfg.active_strategies)

    # Inject the Kalshi client into strategies that need event-level data
    # (partition-arb uses it to verify every bucket is present before arbing).
    for strat in strategies:
        if hasattr(strat, "set_client"):
            strat.set_client(client)
        if hasattr(strat, "set_metrics"):
            strat.set_metrics(metrics)

    if not strategies:
        log.error("No valid strategies loaded. Exiting.")
        sys.exit(1)

    daily_pnl = 0.0
    day_start_portfolio = None
    last_reset_day = datetime.datetime.now(datetime.timezone.utc).date()
    # Restore day_start_portfolio if we crashed/restarted mid-day. Otherwise
    # a restart after a drawdown would reset the daily-loss gate to zero.
    _saved_day = _load_day_state()
    if _saved_day.get("date") == last_reset_day.isoformat():
        day_start_portfolio = _saved_day.get("day_start_portfolio")
        if day_start_portfolio is not None:
            log.info("Restored day_start_portfolio=$%.2f from prior session",
                     day_start_portfolio)
            # Sanity: a stale or corrupted day_start that diverges from the
            # current portfolio by more than 50% will spuriously trip the
            # daily-loss limit and refuse new trades. Prefer to reset rather
            # than block trading on a phantom drawdown.
            try:
                if SIMULATE:
                    _cur_port = None
                elif PAPER:
                    _cur_port = paper.get_portfolio_value([])
                else:
                    _bal = client.get_balance()
                    _cur_port = float(_bal.get("portfolio_value",
                                               _bal.get("balance", 0))) / 100
                if _cur_port and _cur_port > 0 and abs(day_start_portfolio - _cur_port) > 0.5 * _cur_port:
                    log.warning(
                        "Restored day_start $%.2f diverges from current portfolio "
                        "$%.2f by >50%% — discarding stale snapshot",
                        day_start_portfolio, _cur_port,
                    )
                    day_start_portfolio = None
            except Exception as e:
                log.warning("Day-start sanity check failed: %s", e)
    placed_order_ids: set[str] = set()
    # basket_id → {"ts": submitted_ts, "legs": [[order_id, ticker], ...]}
    # Tracks partition-arb baskets so we can cancel orphan legs if some
    # fill while others rest past the reconcile timeout. Persisted to disk
    # so a restart doesn't lose visibility into in-flight live orders.
    basket_tracker: dict[str, dict] = _load_basket_tracker()
    if basket_tracker:
        log.info("Loaded %d tracked basket(s) from prior session", len(basket_tracker))
    prev_candidates: list = []
    iteration = 0

    while _running:
        iteration += 1
        log.info("── Loop #%d ──────────────────────────────────", iteration)
        metrics.push_status(running=True, loop_count=iteration)
        metrics.push_log(f"Loop #{iteration} started")

        try:
            if SIMULATE:
                positions = []
                available = 500.00 + random.uniform(-20, 20)
                portfolio = available + random.uniform(0, 100)
                daily_pnl = random.uniform(-15, 30)
            elif PAPER:
                positions = paper.get_positions_as_dicts()
                available = paper.balance
                portfolio = paper.get_portfolio_value(prev_candidates)

                today = datetime.datetime.now(datetime.timezone.utc).date()
                if today != last_reset_day:
                    log.info("New day — resetting daily counters")
                    risk.reset_daily()
                    day_start_portfolio = portfolio
                    last_reset_day = today
                    _save_day_state(today.isoformat(), day_start_portfolio)
                    metrics.push_log("Daily reset")

                if day_start_portfolio is None:
                    day_start_portfolio = portfolio
                    log.info("Paper start portfolio: $%.2f", day_start_portfolio)
                    _save_day_state(today.isoformat(), day_start_portfolio)

                daily_pnl = portfolio - day_start_portfolio
            else:
                positions    = client.get_positions()
                balance_data = client.get_balance()
                available    = float(balance_data.get("balance", 0)) / 100
                portfolio    = float(balance_data.get("portfolio_value", available * 100)) / 100

                # Midnight UTC reset
                today = datetime.datetime.now(datetime.timezone.utc).date()
                if today != last_reset_day:
                    log.info("New day — resetting daily counters")
                    risk.reset_daily()
                    day_start_portfolio = portfolio
                    last_reset_day = today
                    _save_day_state(today.isoformat(), day_start_portfolio)
                    metrics.push_log("Daily reset")

                # Snapshot start-of-day portfolio on first loop
                if day_start_portfolio is None:
                    day_start_portfolio = portfolio
                    log.info("Day start portfolio: $%.2f", day_start_portfolio)
                    _save_day_state(today.isoformat(), day_start_portfolio)

                daily_pnl = portfolio - day_start_portfolio

            risk.sync_positions(positions)
            risk.sync_daily_pnl(daily_pnl)
            metrics.push_balance(available=available, portfolio=portfolio, daily_pnl=daily_pnl)
            metrics.push_positions(positions)
            log.info("Balance: $%.2f | P&L: $%+.2f | %d open positions",
                     available, daily_pnl, len(risk._open_tickers))

            if PAPER:
                unrealized = paper.mark_to_market(prev_candidates) if prev_candidates else 0.0
                metrics.push_pnl_snapshot(
                    balance=paper.balance, portfolio=portfolio,
                    daily_pnl=daily_pnl, realized_pnl=paper.realized_pnl,
                    unrealized_pnl=unrealized,
                    open_positions=len(paper.positions),
                    pending_orders=len(paper.pending),
                )
                _append_equity_row([
                    datetime.datetime.utcnow().isoformat(timespec="seconds"),
                    round(paper.balance, 2), round(portfolio, 2), round(daily_pnl, 2),
                    round(paper.realized_pnl, 2), round(unrealized, 2),
                    len(paper.positions), iteration,
                ])
                paper.save_state(PAPER_STATE_PATH)
                metrics.push_paper_stats(
                    starting_balance=paper.starting_balance,
                    realized_pnl=paper.realized_pnl,
                    unrealized_pnl=unrealized,
                    total_submitted=paper.total_submitted,
                    total_fills=paper.total_fills,
                    total_fees=paper.total_fees,
                    pending_orders=len(paper.pending),
                )
            elif not SIMULATE:
                _append_equity_row([
                    datetime.datetime.utcnow().isoformat(timespec="seconds"),
                    round(available, 2), round(portfolio, 2), round(daily_pnl, 2),
                    "", "", len(risk._open_tickers), iteration,
                ])

            if SIMULATE:
                raw_markets = generate_fake_markets()
            else:
                # Time-window fetch for per-market weather/expiring signals
                now_ts = int(time.time())
                expiring_window_s = cfg.expiring_within_minutes * 60 if cfg.include_expiring else 0
                weather_window_s  = 48 * 60 * 60 if cfg.include_weather else 0
                fetch_window_s    = max(expiring_window_s, weather_window_s)
                if fetch_window_s == 0:
                    raw_markets = []
                else:
                    raw_markets = client.get_markets(
                        min_close_ts=now_ts,
                        max_close_ts=now_ts + fetch_window_s,
                        limit=200,
                        paginate=True,
                    )

                # Event-based fetch for complete partitions (arb).
                # The time-window fetch may miss legs, so we also fetch
                # every market in each weather event we discovered.
                seen_events: set[str] = set()
                event_markets: list[dict] = []
                for m in raw_markets:
                    et = m.get("event_ticker", "")
                    if not et or et in seen_events:
                        continue
                    series = et.split("-")[0].upper()
                    if not any(series.startswith(p) for p in config.WEATHER_SERIES_PREFIXES):
                        continue
                    seen_events.add(et)
                    try:
                        legs = client.get_markets_for_event(et)
                        event_markets.extend(legs)
                    except Exception as e:
                        log.warning("Event fetch failed for %s: %s", et, e)

                # Merge: event fetch may return markets already in raw_markets
                seen_tickers = {m["ticker"] for m in raw_markets}
                for m in event_markets:
                    if m.get("ticker") not in seen_tickers:
                        raw_markets.append(m)
                        seen_tickers.add(m["ticker"])

                if event_markets:
                    log.info("Event fetch: %d events, %d extra legs added",
                             len(seen_events), len(raw_markets) - len(seen_tickers) + len(event_markets))

            candidates  = scan(
                raw_markets,
                expiring_within_minutes = cfg.expiring_within_minutes,
                include_weather         = cfg.include_weather,
                include_expiring        = cfg.include_expiring,
                min_volume              = cfg.min_volume,
                max_spread              = cfg.max_spread_dollars,
            )
            metrics.push_candidates(
                candidates,
                scan_stats={
                    "total_markets": len(raw_markets),
                    "candidates":    len(candidates),
                    "weather":       sum(1 for c in candidates if "weather" in c.tags),
                    "expiring":      sum(1 for c in candidates if "expiring" in c.tags),
                },
            )
            log.info("%d candidates from %d markets", len(candidates), len(raw_markets))

            # ── Manage resting / paper orders ─────────────────────────
            if PAPER:
                fills = paper.check_fills(candidates)
                for f in fills:
                    metrics.push_order(
                        ticker=f.ticker, side=f.side, count=f.count,
                        price_cents=f.limit_price, status="paper_filled",
                        reason=f.reason, strategy=f.strategy,
                    )
                    metrics.push_fill(
                        ticker=f.ticker, side=f.side, action=f.action,
                        count=f.count, price_cents=f.limit_price,
                        strategy=f.strategy, reason=f.reason,
                    )
                    metrics.push_log(f"PAPER FILL {f.ticker} {f.side.upper()} ×{f.count} @ {f.limit_price}¢")
                if fills:
                    log.info("Paper: %d orders filled, %d pending", len(fills), len(paper.pending))
                    # Re-sync portfolio and risk after fills so the loss limit
                    # isn't tripped by the transient cash-only snapshot.
                    positions = paper.get_positions_as_dicts()
                    available = paper.balance
                    portfolio = paper.get_portfolio_value(candidates)
                    daily_pnl = portfolio - day_start_portfolio
                    risk.sync_positions(positions)
                    risk.sync_daily_pnl(daily_pnl)
                    metrics.push_balance(available=available, portfolio=portfolio, daily_pnl=daily_pnl)
                    metrics.push_positions(positions)
                    log.info("Post-fill: $%.2f portfolio, P&L $%+.2f, %d positions",
                             portfolio, daily_pnl, len(paper.positions))
                # Resolve positions whose markets have expired
                resolutions = paper.resolve_expired(candidates)
                for r in resolutions:
                    metrics.push_log(
                        f"RESOLVED {r['ticker']} {r['side']} ×{r['count']} "
                        f"@ {r['entry_price']}c → {r['outcome']} ${r['pnl']:+.2f}"
                    )
                    # Feed basket settlements into the strategy's kill-switch.
                    # Only basket-level resolutions — single-leg expiries don't
                    # reflect systemic edge-model failure.
                    if r.get("outcome", "").startswith("arb_settled"):
                        for strat in strategies:
                            if hasattr(strat, "record_basket_outcome"):
                                strat.record_basket_outcome(
                                    r.get("ticker", ""),
                                    float(r.get("pnl", 0.0)),
                                )

                prev_candidates = candidates
            elif not SIMULATE and not cfg.dry_run:
                manage_resting_orders(
                    client, candidates, placed_order_ids, metrics,
                )
                reconcile_baskets(
                    client, basket_tracker, placed_order_ids, metrics,
                )

            # ── Phase 0.5: Batch strategies (partition-sum arb) ──────
            arb_placed = 0
            for strat in strategies:
                if hasattr(strat, "set_open_tickers"):
                    strat.set_open_tickers(risk._open_tickers)
                batch_intents = strat.evaluate_batch(candidates)
                # Group intents by event so we can mark events as traded
                # after all their legs are submitted
                event_legs: dict[str, list] = {}
                for intent in batch_intents:
                    event = risk._extract_event(intent.ticker)
                    event_legs.setdefault(event, []).append(intent)

                for event, legs in event_legs.items():
                    placed_this_event = 0
                    for intent in legs:
                        ok = execute(
                            client, intent, risk, cfg.dry_run,
                            metrics, f"{strat.name}_arb",
                            placed_order_ids, paper, basket_tracker,
                        )
                        if ok:
                            arb_placed += 1
                            placed_this_event += 1
                    # Mark event as traded so it won't be re-entered
                    if placed_this_event > 0:
                        risk.mark_arb_event_traded(event)
            if arb_placed:
                log.info("Partition arb: %d orders placed", arb_placed)

            # ── Phase 1: Evaluate exits on open positions ────────────────
            exited = 0
            exited_tickers: set[str] = set()
            # Collect arb basket tickers so we never exit them individually
            arb_tickers: set[str] = set()
            if PAPER and paper:
                for basket_set in paper.arb_baskets.values():
                    arb_tickers |= basket_set

            if positions and not SIMULATE:
                # Build a ticker -> CandidateMarket lookup from current candidates
                candidate_by_ticker = {c.ticker: c for c in candidates}

                # Fallback lookup for held positions whose markets got filtered
                # out of candidates (wide spread, low volume near close, etc.).
                # Without this, take-profit / stop-loss can't fire exactly when
                # liquidity is thinnest — which is when exits matter most.
                held_tickers = {
                    pos.get("ticker", "") for pos in positions
                    if pos.get("position", 0) != 0
                } - arb_tickers - set(candidate_by_ticker)
                exit_fallback = build_exit_candidates(raw_markets, held_tickers) if not SIMULATE else {}

                for pos in positions:
                    ticker = pos.get("ticker", "")
                    count = pos.get("position", 0)
                    if count == 0:
                        continue
                    if ticker in exited_tickers:
                        continue
                    # Never exit arb basket legs — they settle as a group
                    if ticker in arb_tickers:
                        continue
                    # Skip if we already have a sell pending for this position
                    # (prevents re-submitting the same stop-loss every scan
                    # when nothing lifts the 1¢ bid)
                    if PAPER and paper.has_pending_exit(
                        ticker, "yes" if count > 0 else "no"
                    ):
                        continue
                    market = candidate_by_ticker.get(ticker) or exit_fallback.get(ticker)
                    if market is None:
                        continue
                    # Determine side and entry price from position data
                    side = "yes" if count > 0 else "no"
                    entry_cents = pos.get("average_price", 50)  # Kalshi returns avg price
                    position_info = PositionInfo(
                        ticker=ticker, side=side,
                        count=abs(count), entry_price=entry_cents,
                    )

                    try:
                        orderbook = client.get_orderbook(ticker)
                    except Exception as e:
                        log.warning("Orderbook fetch failed for exit eval %s: %s", ticker, e)
                        orderbook = {}

                    for strat in strategies:
                        exit_intent = strat.evaluate_exit(position_info, market, orderbook)
                        if exit_intent is None:
                            continue
                        rejection = risk.approve(exit_intent, is_exit=True)
                        if rejection:
                            log.warning("EXIT RISK BLOCK %s — %s", ticker, rejection)
                            break
                        label = "PAPER" if PAPER else "DRY" if cfg.dry_run else "LIVE"
                        log.info(
                            "[%s EXIT] %s %s×%d @ %dc | %s",
                            label, exit_intent.ticker, exit_intent.side.upper(),
                            exit_intent.count, exit_intent.limit_price, exit_intent.reason,
                        )
                        exited_tickers.add(ticker)
                        # If sell limit is 1¢ and the book has no bid to lift it,
                        # a pending sell will sit forever. Book the loss now so
                        # the slot clears instead of hoarding zombie positions.
                        if PAPER and exit_intent.limit_price <= 1:
                            paper.writeoff_position(ticker, exit_intent.reason)
                            exited += 1
                            metrics.push_order(
                                ticker=exit_intent.ticker, side=exit_intent.side,
                                count=exit_intent.count, price_cents=exit_intent.limit_price,
                                status="writeoff", reason=exit_intent.reason,
                                strategy=f"{strat.name}_exit",
                            )
                            metrics.push_log(f"WRITEOFF {exit_intent.ticker} {exit_intent.reason}")
                            break
                        if PAPER:
                            paper.submit_order(exit_intent, f"{strat.name}_exit")
                        elif not cfg.dry_run:
                            try:
                                result = client.place_order(
                                    ticker=exit_intent.ticker, side=exit_intent.side,
                                    count=exit_intent.count, limit_price=exit_intent.limit_price,
                                    action=exit_intent.action,
                                )
                                oid = result.get("order", {}).get("order_id", "?")
                                if oid != "?":
                                    placed_order_ids.add(oid)
                            except Exception as e:
                                log.error("Exit order failed %s: %s", ticker, e)
                        exited += 1
                        metrics.push_order(
                            ticker=exit_intent.ticker, side=exit_intent.side,
                            count=exit_intent.count, price_cents=exit_intent.limit_price,
                            status="paper" if PAPER else "dry_run" if cfg.dry_run else "placed",
                            reason=exit_intent.reason, strategy=f"{strat.name}_exit",
                        )
                        metrics.push_log(f"EXIT {exit_intent.ticker} {exit_intent.reason}")
                        break

            # ── Phase 2: Evaluate new entries ─────────────────────────────
            placed = 0
            if risk._count_logical_positions() >= cfg.max_open_positions:
                log.info("Entry phase skipped — %d/%d logical positions filled",
                         risk._count_logical_positions(), cfg.max_open_positions)
                candidates_for_entry = []
            else:
                candidates_for_entry = candidates
            for market in candidates_for_entry:
                if SIMULATE:
                    orderbook = {}
                else:
                    try:
                        orderbook = client.get_orderbook(market.ticker)
                    except Exception as e:
                        log.warning("Orderbook fetch failed %s: %s", market.ticker, e)
                        orderbook = {}

                for strat in strategies:
                    intent = strat.evaluate(market, orderbook)
                    if intent is None:
                        continue
                    ok = execute(client, intent, risk, cfg.dry_run, metrics, strat.name, placed_order_ids, paper)
                    if ok:
                        placed += 1
                        break

            log.info("Loop #%d done — %d entries, %d exits, %d arb", iteration, placed, exited, arb_placed)
            metrics.push_log(f"Loop #{iteration} done — {placed} entries, {exited} exits, {arb_placed} arb")

        except KeyboardInterrupt:
            break
        except Exception as e:
            log.exception("Unhandled error in loop #%d: %s", iteration, e)
            metrics.push_log(f"ERROR loop #{iteration}: {e}")

        # Settle-time fast scan: when any weather candidate is within
        # FAST_SCAN_MINUTES_TO_CLOSE of close, drop the loop cadence.
        # METAR ticks during the final stretch can collapse a basket arb
        # in seconds; the slower 30s cadence misses the window entirely.
        sleep_seconds = cfg.scan_interval_seconds
        try:
            near_settle = any(
                c.minutes_to_close is not None
                and c.minutes_to_close <= config.FAST_SCAN_MINUTES_TO_CLOSE
                and "weather" in c.tags
                for c in candidates
            )
        except NameError:
            near_settle = False
        if near_settle:
            sleep_seconds = min(sleep_seconds, config.FAST_SCAN_INTERVAL_SECONDS)
        for _ in range(sleep_seconds):
            if not _running:
                break
            time.sleep(1)

    metrics.push_status(running=False)
    metrics.push_log("Bot stopped")
    if paper is not None:
        paper.save_state(PAPER_STATE_PATH)
    log.info("Bot shut down cleanly after %d iterations.", iteration)

    if not cfg.dry_run and client is not None:
        n = client.cancel_all_orders()
        log.info("%d orders cancelled.", n)

    metrics.stop()


if __name__ == "__main__":
    cfg = load_from_env()
    run(cfg)
