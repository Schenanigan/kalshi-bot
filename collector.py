"""
collector.py — Records live market snapshots and outcomes to SQLite.

Usage:
    # From bot.py (import and call):
    from collector import Collector
    col = Collector()
    col.record_snapshots(candidates)

    # Standalone (runs its own scan loop):
    python collector.py                    # uses Kalshi API
    python collector.py --simulate         # uses synthetic data
"""

import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

DB_DIR = Path(__file__).parent / "data"
DB_PATH = DB_DIR / "kalshi.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS market_snapshots (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_time    TEXT NOT NULL,
    ticker           TEXT NOT NULL,
    title            TEXT,
    series           TEXT,
    yes_bid          REAL,
    yes_ask          REAL,
    mid              REAL,
    volume           INTEGER,
    close_time       TEXT,
    minutes_to_close REAL,
    tags             TEXT,
    raw              TEXT
);
CREATE INDEX IF NOT EXISTS idx_snap_ticker_time ON market_snapshots(ticker, snapshot_time);
CREATE INDEX IF NOT EXISTS idx_snap_time ON market_snapshots(snapshot_time);

CREATE TABLE IF NOT EXISTS market_outcomes (
    ticker          TEXT PRIMARY KEY,
    title           TEXT,
    series          TEXT,
    close_time      TEXT,
    result          TEXT,
    yes_price_final REAL,
    volume_total    INTEGER,
    scraped_at      TEXT,
    raw             TEXT
);
CREATE INDEX IF NOT EXISTS idx_outcome_series ON market_outcomes(series);
CREATE INDEX IF NOT EXISTS idx_outcome_close ON market_outcomes(close_time);

CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id      TEXT PRIMARY KEY,
    started_at  TEXT,
    params      TEXT,
    strategy    TEXT,
    total_trades INTEGER,
    win_rate    REAL,
    total_pnl   REAL,
    sharpe_ratio REAL,
    max_drawdown REAL
);

CREATE TABLE IF NOT EXISTS backtest_trades (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        TEXT NOT NULL,
    strategy      TEXT NOT NULL,
    ticker        TEXT NOT NULL,
    side          TEXT NOT NULL,
    action        TEXT NOT NULL,
    count         INTEGER,
    limit_price   INTEGER,
    reason        TEXT,
    snapshot_time TEXT,
    outcome       TEXT,
    pnl_cents     REAL
);
"""


class Collector:
    def __init__(self, db_path: str = str(DB_PATH)):
        DB_DIR.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_db()

    def _init_db(self):
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    def record_snapshots(self, candidates) -> int:
        """Record a list of CandidateMarket objects as snapshot rows."""
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for c in candidates:
            rows.append((
                now,
                c.ticker,
                c.title,
                c.series,
                c.yes_bid,
                c.yes_ask,
                c.mid,
                c.volume,
                c.close_time.isoformat() if hasattr(c.close_time, 'isoformat') else str(c.close_time),
                c.minutes_to_close,
                json.dumps(c.tags),
                json.dumps(c.raw, default=str),
            ))
        if rows:
            self.conn.executemany(
                """INSERT INTO market_snapshots
                   (snapshot_time, ticker, title, series, yes_bid, yes_ask, mid,
                    volume, close_time, minutes_to_close, tags, raw)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            self.conn.commit()
        log.info("Recorded %d market snapshots", len(rows))
        return len(rows)

    def record_outcome(self, market_dict: dict) -> bool:
        """Record a settled market outcome. Returns True if inserted."""
        ticker = market_dict.get("ticker", "")
        if not ticker:
            return False

        result_raw = market_dict.get("result", "")
        if result_raw:
            result = result_raw.lower()
        elif market_dict.get("yes_price") == 100:
            result = "yes"
        elif market_dict.get("yes_price") == 0:
            result = "no"
        else:
            result = "unknown"

        yes_final = 1.0 if result == "yes" else 0.0 if result == "no" else None
        close_str = market_dict.get("close_time") or market_dict.get("expiration_time", "")
        now = datetime.now(timezone.utc).isoformat()

        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO market_outcomes
                   (ticker, title, series, close_time, result, yes_price_final,
                    volume_total, scraped_at, raw)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    ticker,
                    market_dict.get("title", ""),
                    market_dict.get("series_ticker", ""),
                    close_str,
                    result,
                    yes_final,
                    market_dict.get("volume", 0),
                    now,
                    json.dumps(market_dict, default=str),
                ),
            )
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            log.warning("Failed to record outcome for %s: %s", ticker, e)
            return False

    def get_snapshot_count(self) -> int:
        row = self.conn.execute("SELECT count(*) FROM market_snapshots").fetchone()
        return row[0] if row else 0

    def get_outcome_count(self) -> int:
        row = self.conn.execute("SELECT count(*) FROM market_outcomes").fetchone()
        return row[0] if row else 0

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    SIMULATE = "--simulate" in sys.argv
    INTERVAL = 30

    col = Collector()
    log.info("Collector started (db=%s, simulate=%s)", col.db_path, SIMULATE)

    if SIMULATE:
        sys.path.insert(0, str(Path(__file__).parent))
        from bot import generate_fake_markets
        from scanner import scan

        while True:
            raw = generate_fake_markets()
            candidates = scan(raw, include_weather=True, include_expiring=True)
            col.record_snapshots(candidates)
            log.info("DB: %d snapshots, %d outcomes", col.get_snapshot_count(), col.get_outcome_count())
            time.sleep(INTERVAL)
    else:
        from config import load_from_env, WEATHER_SERIES_PREFIXES
        from client import KalshiClient
        from scanner import scan

        cfg = load_from_env()
        if not cfg.api_key:
            log.error("KALSHI_API_KEY not set. Use --simulate or set env var.")
            sys.exit(1)

        client = KalshiClient(cfg.api_key, cfg.private_key_path, demo=cfg.demo)
        log.info("Connected to Kalshi API (demo=%s)", cfg.demo)

        while True:
            try:
                now_ts = int(time.time())
                raw_markets = []

                if cfg.include_expiring:
                    max_close = now_ts + (cfg.expiring_within_minutes * 60)
                    raw_markets.extend(client.get_markets(
                        min_close_ts=now_ts, max_close_ts=max_close, limit=200, paginate=False,
                    ))
                    time.sleep(0.3)

                if cfg.include_weather:
                    weather_max = now_ts + (48 * 60 * 60)
                    for prefix in WEATHER_SERIES_PREFIXES:
                        raw_markets.extend(client.get_markets(
                            series=prefix, limit=200, paginate=False,
                            min_close_ts=now_ts, max_close_ts=weather_max,
                        ))
                        time.sleep(0.3)

                # Deduplicate
                seen = set()
                unique = []
                for m in raw_markets:
                    t = m.get("ticker", "")
                    if t not in seen:
                        seen.add(t)
                        unique.append(m)

                candidates = scan(unique,
                    expiring_within_minutes=cfg.expiring_within_minutes,
                    include_weather=cfg.include_weather,
                    include_expiring=cfg.include_expiring,
                    min_volume=cfg.min_volume,
                    max_spread=cfg.max_spread_dollars,
                )
                col.record_snapshots(candidates)
                log.info("DB: %d snapshots, %d outcomes", col.get_snapshot_count(), col.get_outcome_count())

            except Exception as e:
                log.exception("Collector loop error: %s", e)

            time.sleep(INTERVAL)
