"""
backtest.py — Replay historical market data through existing strategies.

Usage:
    python backtest.py                                    # run default backtest
    python backtest.py --strategy expiry_momentum         # specific strategy
    python backtest.py --sweep                            # parameter sweep
    python backtest.py --strategy fair_value --sweep      # sweep one strategy
"""

import json
import logging
import sqlite3
import sys
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from itertools import product
from math import sqrt
from typing import Optional

import config
from scanner import CandidateMarket
from strategy import FairValueStrategy, ExpiryMomentumStrategy, BaseStrategy

log = logging.getLogger(__name__)

DB_PATH = "data/kalshi.db"


@dataclass
class BacktestConfig:
    strategy_name: str = "expiry_momentum"
    start_date: str = None
    end_date: str = None
    initial_balance: float = 1000.0
    series_filter: list[str] = None
    param_overrides: dict = None


@dataclass
class TradeRecord:
    ticker: str
    side: str
    action: str
    count: int
    entry_price: int        # cents
    outcome: str            # "yes" or "no"
    pnl: float              # dollars
    snapshot_time: str
    strategy: str
    reason: str


@dataclass
class BacktestResult:
    run_id: str
    config: BacktestConfig
    trades: list[TradeRecord]
    total_pnl: float = 0.0
    win_rate: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    pnl_curve: list[float] = field(default_factory=list)
    num_trades: int = 0


class BacktestEngine:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def run(self, cfg: BacktestConfig) -> BacktestResult:
        run_id = str(uuid.uuid4())[:8]
        strategy = self._build_strategy(cfg.strategy_name)
        overrides_backup = self._apply_overrides(cfg.param_overrides or {})

        try:
            outcomes = self._fetch_outcomes(cfg)
            trades = []
            traded_tickers = set()

            for outcome in outcomes:
                ticker = outcome["ticker"]
                if ticker in traded_tickers:
                    continue

                snapshots = self._fetch_snapshots(ticker)
                if not snapshots:
                    continue

                # Evaluate each snapshot (strategy sees them chronologically)
                for snap in snapshots:
                    candidate = self._reconstruct_candidate(snap)
                    if candidate is None:
                        continue

                    orderbook = self._mock_orderbook(snap)
                    intent = strategy.evaluate(candidate, orderbook)

                    if intent is not None:
                        pnl = self._calculate_pnl(intent, outcome["result"])
                        trades.append(TradeRecord(
                            ticker=ticker,
                            side=intent.side,
                            action=intent.action,
                            count=intent.count,
                            entry_price=intent.limit_price,
                            outcome=outcome["result"],
                            pnl=pnl,
                            snapshot_time=snap["snapshot_time"],
                            strategy=cfg.strategy_name,
                            reason=intent.reason,
                        ))
                        traded_tickers.add(ticker)
                        break  # one trade per market

            result = self._build_result(run_id, cfg, trades)
            return result

        finally:
            self._restore_overrides(overrides_backup)

    def sweep(self, base_cfg: BacktestConfig, param_grid: dict) -> list[BacktestResult]:
        """Run backtest for every combination of parameters in param_grid."""
        keys = list(param_grid.keys())
        values = list(param_grid.values())
        results = []

        combos = list(product(*values))
        log.info("Sweep: %d parameter combinations", len(combos))

        for combo in combos:
            overrides = dict(zip(keys, combo))
            cfg = BacktestConfig(
                strategy_name=base_cfg.strategy_name,
                start_date=base_cfg.start_date,
                end_date=base_cfg.end_date,
                initial_balance=base_cfg.initial_balance,
                series_filter=base_cfg.series_filter,
                param_overrides=overrides,
            )
            result = self.run(cfg)
            results.append(result)
            log.info("  %s → %d trades, PnL=$%.2f, Sharpe=%.2f",
                     overrides, result.num_trades, result.total_pnl, result.sharpe_ratio)

        results.sort(key=lambda r: r.sharpe_ratio, reverse=True)
        return results

    def save_results(self, result: BacktestResult):
        """Persist backtest results to DB."""
        self.conn.execute(
            """INSERT OR REPLACE INTO backtest_runs
               (run_id, started_at, params, strategy, total_trades,
                win_rate, total_pnl, sharpe_ratio, max_drawdown)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                result.run_id,
                datetime.now(timezone.utc).isoformat(),
                json.dumps(result.config.param_overrides or {}),
                result.config.strategy_name,
                result.num_trades,
                result.win_rate,
                result.total_pnl,
                result.sharpe_ratio,
                result.max_drawdown,
            ),
        )
        for t in result.trades:
            self.conn.execute(
                """INSERT INTO backtest_trades
                   (run_id, strategy, ticker, side, action, count,
                    limit_price, reason, snapshot_time, outcome, pnl_cents)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    result.run_id, t.strategy, t.ticker, t.side, t.action,
                    t.count, t.entry_price, t.reason, t.snapshot_time,
                    t.outcome, t.pnl * 100,
                ),
            )
        self.conn.commit()

    def print_summary(self, result: BacktestResult):
        """Pretty-print backtest results."""
        print("\n" + "=" * 60)
        print(f"BACKTEST RESULTS — {result.config.strategy_name}")
        print("=" * 60)
        print(f"  Run ID:       {result.run_id}")
        print(f"  Trades:       {result.num_trades}")
        print(f"  Win Rate:     {result.win_rate:.1%}")
        print(f"  Total P&L:    ${result.total_pnl:.2f}")
        print(f"  Sharpe Ratio: {result.sharpe_ratio:.2f}")
        print(f"  Max Drawdown: ${result.max_drawdown:.2f}")
        if result.config.param_overrides:
            print(f"  Params:       {result.config.param_overrides}")
        print("-" * 60)

        if result.trades:
            print(f"  {'Ticker':<30} {'Side':<5} {'Price':>6} {'Outcome':>8} {'P&L':>8}")
            print(f"  {'—'*30} {'—'*5} {'—'*6} {'—'*8} {'—'*8}")
            for t in result.trades[:30]:
                pnl_str = f"${t.pnl:+.2f}"
                print(f"  {t.ticker:<30} {t.side:<5} {t.entry_price:>5}c {t.outcome:>8} {pnl_str:>8}")
            if len(result.trades) > 30:
                print(f"  ... and {len(result.trades) - 30} more trades")
        print("=" * 60 + "\n")

    # ── Internal methods ──────────────────────────────────────────────────────

    def _build_strategy(self, name: str) -> BaseStrategy:
        registry = {
            "fair_value": FairValueStrategy,
            "expiry_momentum": ExpiryMomentumStrategy,
        }
        cls = registry.get(name)
        if cls is None:
            raise ValueError(f"Unknown strategy: {name}. Choose from: {list(registry)}")
        return cls()

    def _fetch_outcomes(self, cfg: BacktestConfig) -> list[dict]:
        query = "SELECT * FROM market_outcomes WHERE 1=1"
        params = []

        if cfg.start_date:
            query += " AND close_time >= ?"
            params.append(cfg.start_date)
        if cfg.end_date:
            query += " AND close_time <= ?"
            params.append(cfg.end_date)
        if cfg.series_filter:
            placeholders = ",".join("?" * len(cfg.series_filter))
            query += f" AND series IN ({placeholders})"
            params.extend(cfg.series_filter)

        query += " AND result IN ('yes', 'no')"
        query += " ORDER BY close_time ASC"

        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def _fetch_snapshots(self, ticker: str) -> list[dict]:
        rows = self.conn.execute(
            """SELECT * FROM market_snapshots
               WHERE ticker = ?
               ORDER BY snapshot_time ASC""",
            (ticker,),
        ).fetchall()
        return [dict(r) for r in rows]

    def _reconstruct_candidate(self, snap: dict) -> Optional[CandidateMarket]:
        """Build a CandidateMarket from a database snapshot row."""
        try:
            close_time = datetime.fromisoformat(snap["close_time"].replace("Z", "+00:00"))
            snap_time = datetime.fromisoformat(snap["snapshot_time"].replace("Z", "+00:00"))
            minutes_to_close = (close_time - snap_time).total_seconds() / 60

            if minutes_to_close < 0:
                return None

            tags = json.loads(snap["tags"]) if snap["tags"] else []
            raw = json.loads(snap["raw"]) if snap["raw"] else {}

            return CandidateMarket(
                ticker=snap["ticker"],
                title=snap["title"] or "",
                series=snap["series"] or "",
                yes_bid=snap["yes_bid"],
                yes_ask=snap["yes_ask"],
                mid=snap["mid"],
                volume=snap["volume"] or 0,
                close_time=close_time,
                minutes_to_close=minutes_to_close,
                tags=tags,
                raw=raw,
            )
        except (KeyError, ValueError, TypeError) as e:
            log.debug("Failed to reconstruct candidate from snapshot: %s", e)
            return None

    def _mock_orderbook(self, snap: dict) -> dict:
        """Build a minimal orderbook dict from snapshot data."""
        bid_cents = int((snap["yes_bid"] or 0.5) * 100)
        ask_cents = int((snap["yes_ask"] or 0.5) * 100)
        return {
            "yes": [[bid_cents, 100]],
            "no": [[100 - ask_cents, 100]],
            "orderbook": {
                "yes": [[bid_cents, 100]],
                "no": [[100 - ask_cents, 100]],
            },
        }

    def _calculate_pnl(self, intent, outcome: str) -> float:
        """Binary market P&L in dollars."""
        price_cents = intent.limit_price
        count = intent.count

        if intent.side == outcome:
            # Won: receive 100c per contract, paid price_cents
            return (100 - price_cents) * count / 100
        else:
            # Lost: paid price_cents, receive nothing
            return -(price_cents * count) / 100

    def _build_result(self, run_id: str, cfg: BacktestConfig, trades: list[TradeRecord]) -> BacktestResult:
        pnl_values = [t.pnl for t in trades]
        cumulative = []
        running = 0.0
        for p in pnl_values:
            running += p
            cumulative.append(running)

        total_pnl = sum(pnl_values) if pnl_values else 0.0
        wins = sum(1 for p in pnl_values if p > 0)
        win_rate = wins / len(pnl_values) if pnl_values else 0.0
        sharpe = self._sharpe(pnl_values)
        drawdown = self._max_drawdown(cumulative)

        return BacktestResult(
            run_id=run_id,
            config=cfg,
            trades=trades,
            total_pnl=total_pnl,
            win_rate=win_rate,
            sharpe_ratio=sharpe,
            max_drawdown=drawdown,
            pnl_curve=cumulative,
            num_trades=len(trades),
        )

    def _sharpe(self, pnl_values: list[float]) -> float:
        if len(pnl_values) < 2:
            return 0.0
        mean = sum(pnl_values) / len(pnl_values)
        variance = sum((p - mean) ** 2 for p in pnl_values) / (len(pnl_values) - 1)
        std = sqrt(variance) if variance > 0 else 0.001
        return (mean / std) * sqrt(252)  # annualized

    def _max_drawdown(self, cumulative: list[float]) -> float:
        if not cumulative:
            return 0.0
        peak = cumulative[0]
        max_dd = 0.0
        for val in cumulative:
            if val > peak:
                peak = val
            dd = peak - val
            if dd > max_dd:
                max_dd = dd
        return max_dd

    def _apply_overrides(self, overrides: dict) -> dict:
        backup = {}
        for key, val in overrides.items():
            if hasattr(config, key):
                backup[key] = getattr(config, key)
                setattr(config, key, val)
        return backup

    def _restore_overrides(self, backup: dict):
        for key, val in backup.items():
            setattr(config, key, val)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    strategy = "expiry_momentum"
    do_sweep = "--sweep" in sys.argv
    for i, arg in enumerate(sys.argv):
        if arg == "--strategy" and i + 1 < len(sys.argv):
            strategy = sys.argv[i + 1]

    engine = BacktestEngine()

    if do_sweep:
        log.info("Running parameter sweep for %s...", strategy)
        base = BacktestConfig(strategy_name=strategy)

        if strategy == "expiry_momentum":
            grid = {
                "EXPIRING_EDGE_THRESHOLD": [0.03, 0.04, 0.06, 0.08, 0.10],
                "TRAILING_STOP_PCT": [0.30, 0.50, 0.70],
            }
        elif strategy == "fair_value":
            grid = {
                "WEATHER_EDGE_THRESHOLD": [0.04, 0.06, 0.08, 0.10, 0.12],
            }
        else:
            grid = {}

        results = engine.sweep(base, grid)
        print(f"\n{'='*60}")
        print(f"SWEEP RESULTS — {strategy} ({len(results)} combinations)")
        print(f"{'='*60}")
        print(f"  {'Params':<45} {'Trades':>6} {'P&L':>8} {'Sharpe':>7} {'Win%':>6}")
        for r in results[:20]:
            params_str = str(r.config.param_overrides or {})[:44]
            print(f"  {params_str:<45} {r.num_trades:>6} ${r.total_pnl:>7.2f} {r.sharpe_ratio:>7.2f} {r.win_rate:>5.1%}")

        if results:
            engine.save_results(results[0])
            print(f"\nBest run saved: {results[0].run_id}")
    else:
        log.info("Running backtest for %s...", strategy)
        cfg = BacktestConfig(strategy_name=strategy)
        result = engine.run(cfg)
        engine.print_summary(result)
        engine.save_results(result)
