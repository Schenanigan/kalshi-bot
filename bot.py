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

import time
import random
import signal
import logging
import sys
import datetime

import config
from config import BotConfig, load_from_env
from client import KalshiClient
from scanner import scan
from strategy import FairValueStrategy, ExpiryMomentumStrategy, BaseStrategy, OrderIntent, PositionInfo
from risk import RiskManager, RiskConfig
from metrics import MetricsServer
from paper import PaperTrader

# ── Logging ───────────────────────────────────────────────────────────────────
_log_handlers = [logging.StreamHandler(sys.stdout)]
try:
    _log_handlers.append(logging.FileHandler("bot.log"))
except OSError:
    pass  # skip file logging if not writable (e.g. in containers)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=_log_handlers,
)
log = logging.getLogger("bot")

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
    registry = {
        "fair_value":      FairValueStrategy(),
        "expiry_momentum": ExpiryMomentumStrategy(),
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
) -> bool:
    rejection = risk.approve(intent)
    if rejection:
        log.warning("RISK BLOCK %s — %s", intent.ticker, rejection)
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


# ── Main loop ─────────────────────────────────────────────────────────────────

def run(cfg: BotConfig):
    log.info("=" * 60)
    log.info("Kalshi Bot starting")
    mode = "SIMULATE" if SIMULATE else "PAPER" if PAPER else "DEMO" if cfg.demo else "PRODUCTION"
    log.info("  Mode      : %s", mode)
    log.info("  Dry run   : %s", cfg.dry_run)
    log.info("  Strategies: %s", cfg.active_strategies)
    log.info("  Interval  : %ds", cfg.scan_interval_seconds)
    log.info("=" * 60)

    if not cfg.api_key and not SIMULATE:
        log.error("KALSHI_API_KEY is not set. Exiting.")
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
    risk      = RiskManager(RiskConfig(
        max_trade_dollars      = cfg.max_trade_dollars,
        max_open_positions     = cfg.max_open_positions,
        max_daily_loss_dollars = cfg.max_daily_loss_dollars,
        allow_duplicate_tickers= cfg.allow_duplicate_tickers,
    ))
    strategies = build_strategies(cfg.active_strategies)

    if not strategies:
        log.error("No valid strategies loaded. Exiting.")
        sys.exit(1)

    daily_pnl = 0.0
    day_start_portfolio = None
    last_reset_day = datetime.datetime.now(datetime.timezone.utc).date()
    placed_order_ids: set[str] = set()
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
                    metrics.push_log("Daily reset")

                if day_start_portfolio is None:
                    day_start_portfolio = portfolio
                    log.info("Paper start portfolio: $%.2f", day_start_portfolio)

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
                    metrics.push_log("Daily reset")

                # Snapshot start-of-day portfolio on first loop
                if day_start_portfolio is None:
                    day_start_portfolio = portfolio
                    log.info("Day start portfolio: $%.2f", day_start_portfolio)

                daily_pnl = portfolio - day_start_portfolio

            risk.sync_positions(positions)
            risk.sync_daily_pnl(daily_pnl)
            metrics.push_balance(available=available, portfolio=portfolio, daily_pnl=daily_pnl)
            metrics.push_positions(positions)
            log.info("Balance: $%.2f | P&L: $%+.2f | %d open positions",
                     available, daily_pnl, len(risk._open_tickers))

            if SIMULATE:
                raw_markets = generate_fake_markets()
            else:
                # Fetch targeted market subsets instead of all open markets
                raw_markets = []
                now_ts = int(time.time())

                if cfg.include_expiring:
                    max_close = now_ts + (cfg.expiring_within_minutes * 60)
                    expiring = client.get_markets(
                        min_close_ts=now_ts, max_close_ts=max_close, limit=200, paginate=False,
                    )
                    raw_markets.extend(expiring)
                    time.sleep(0.3)

                if cfg.include_weather:
                    # Only fetch weather markets expiring within 48 hours
                    weather_max_close = now_ts + (48 * 60 * 60)
                    for prefix in config.WEATHER_SERIES_PREFIXES:
                        weather = client.get_markets(
                            series=prefix, limit=200, paginate=False,
                            min_close_ts=now_ts, max_close_ts=weather_max_close,
                        )
                        raw_markets.extend(weather)
                        time.sleep(0.3)

                # Deduplicate by ticker
                seen_tickers = set()
                unique_markets = []
                for m in raw_markets:
                    t = m.get("ticker", "")
                    if t not in seen_tickers:
                        seen_tickers.add(t)
                        unique_markets.append(m)
                raw_markets = unique_markets

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
                    metrics.push_log(f"PAPER FILL {f.ticker} {f.side.upper()} ×{f.count} @ {f.limit_price}¢")
                if fills:
                    log.info("Paper: %d orders filled, %d pending", len(fills), len(paper.pending))
                prev_candidates = candidates
            elif not SIMULATE and not cfg.dry_run:
                manage_resting_orders(
                    client, candidates, placed_order_ids, metrics,
                )

            # ── Phase 1: Evaluate exits on open positions ────────────────
            exited = 0
            if positions and not SIMULATE:
                # Build a ticker -> CandidateMarket lookup from current candidates
                candidate_by_ticker = {c.ticker: c for c in candidates}

                for pos in positions:
                    ticker = pos.get("ticker", "")
                    count = pos.get("position", 0)
                    if count == 0 or ticker not in candidate_by_ticker:
                        continue

                    market = candidate_by_ticker[ticker]
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
            for market in candidates:
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

            log.info("Loop #%d done — %d entries, %d exits", iteration, placed, exited)
            metrics.push_log(f"Loop #{iteration} done — {placed} entries, {exited} exits")

        except KeyboardInterrupt:
            break
        except Exception as e:
            log.exception("Unhandled error in loop #%d: %s", iteration, e)
            metrics.push_log(f"ERROR loop #{iteration}: {e}")

        for _ in range(cfg.scan_interval_seconds):
            if not _running:
                break
            time.sleep(1)

    metrics.push_status(running=False)
    metrics.push_log("Bot stopped")
    log.info("Bot shut down cleanly after %d iterations.", iteration)

    if not cfg.dry_run and client is not None:
        n = client.cancel_all_orders()
        log.info("%d orders cancelled.", n)

    metrics.stop()


if __name__ == "__main__":
    cfg = load_from_env()
    run(cfg)
