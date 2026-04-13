"""
collector.py — Records live market snapshots and outcomes to database.

Uses Postgres when DATABASE_URL is set, SQLite otherwise.

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
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import db as database

log = logging.getLogger(__name__)


class Collector:
    def __init__(self):
        self.conn = database.get_connection()
        database.init_db(self.conn)
        self._pg = database.is_postgres()
        self._ph = database.ph()

    def record_snapshots(self, candidates) -> int:
        """Record a list of CandidateMarket objects as snapshot rows."""
        now = datetime.now(timezone.utc)
        now_str = now.isoformat() if not self._pg else now
        rows = []
        for c in candidates:
            close_val = c.close_time if self._pg else (
                c.close_time.isoformat() if hasattr(c.close_time, 'isoformat') else str(c.close_time)
            )
            tags_val = json.dumps(c.tags)
            raw_val = json.dumps(c.raw, default=str)
            rows.append((
                now_str, c.ticker, c.title, c.series,
                c.yes_bid, c.yes_ask, c.mid, c.volume,
                close_val, c.minutes_to_close, tags_val, raw_val,
            ))
        if rows:
            p = self._ph
            database.executemany(
                self.conn,
                f"""INSERT INTO market_snapshots
                   (snapshot_time, ticker, title, series, yes_bid, yes_ask, mid,
                    volume, close_time, minutes_to_close, tags, raw)
                   VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})""",
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
        now = datetime.now(timezone.utc)
        now_val = now if self._pg else now.isoformat()
        raw_val = json.dumps(market_dict, default=str)

        p = self._ph
        try:
            database.execute(
                self.conn,
                f"""INSERT INTO market_outcomes
                   (ticker, title, series, close_time, result, yes_price_final,
                    volume_total, scraped_at, raw)
                   VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p})
                   ON CONFLICT (ticker) DO NOTHING""",
                (
                    ticker,
                    market_dict.get("title", ""),
                    market_dict.get("series_ticker", ""),
                    close_str,
                    result,
                    yes_final,
                    market_dict.get("volume", 0),
                    now_val,
                    raw_val,
                ),
            )
            self.conn.commit()
            return True
        except Exception as e:
            log.warning("Failed to record outcome for %s: %s", ticker, e)
            try:
                self.conn.rollback()
            except Exception:
                pass
            return False

    def get_snapshot_count(self) -> int:
        rows = database.fetchall_dicts(self.conn, "SELECT count(*) as cnt FROM market_snapshots")
        return rows[0]["cnt"] if rows else 0

    def get_outcome_count(self) -> int:
        rows = database.fetchall_dicts(self.conn, "SELECT count(*) as cnt FROM market_outcomes")
        return rows[0]["cnt"] if rows else 0

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    SIMULATE = "--simulate" in sys.argv
    INTERVAL = 30

    col = Collector()
    log.info("Collector started (postgres=%s, simulate=%s)", database.is_postgres(), SIMULATE)

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
