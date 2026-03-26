"""
scraper.py — Fetches historical settled markets from Kalshi API.

Usage:
    python scraper.py                      # scrape last 90 days
    python scraper.py --days 30            # scrape last 30 days
    python scraper.py --synthetic          # generate synthetic snapshots for outcomes without history
"""

import json
import logging
import math
import random
import sys
import time
from datetime import datetime, timezone, timedelta

import db as database
from collector import Collector
from config import WEATHER_SERIES_PREFIXES

log = logging.getLogger(__name__)


class HistoricalScraper:
    def __init__(self, client):
        self.client = client
        self.col = Collector()

    def scrape_settled(
        self,
        series_list: list[str] = None,
        days_back: int = 90,
    ) -> int:
        """Fetch settled markets from Kalshi API and store outcomes."""
        if series_list is None:
            series_list = WEATHER_SERIES_PREFIXES + [None]  # None = all non-series-filtered

        now_ts = int(time.time())
        min_close_ts = now_ts - (days_back * 86400)
        total_inserted = 0

        for series in series_list:
            label = series or "all"
            log.info("Scraping settled markets: series=%s, days_back=%d", label, days_back)

            try:
                markets = self.client.get_markets(
                    status="settled",
                    series=series,
                    min_close_ts=min_close_ts,
                    max_close_ts=now_ts,
                    limit=200,
                    paginate=True,
                )
            except Exception as e:
                log.warning("Failed to scrape series=%s: %s", label, e)
                continue

            inserted = 0
            for m in markets:
                if self.col.record_outcome(m):
                    inserted += 1

            log.info("Series %s: fetched %d markets, inserted %d new outcomes",
                     label, len(markets), inserted)
            total_inserted += inserted
            time.sleep(0.5)

        log.info("Total: %d new outcomes scraped", total_inserted)
        return total_inserted

    def generate_synthetic_snapshots(
        self,
        ticker: str = None,
        num_snapshots: int = 20,
        hours_before_close: float = 48.0,
    ) -> int:
        """Generate synthetic price path snapshots for outcomes that lack snapshot history.

        If ticker is None, generates for all outcomes without snapshots.
        Returns count of snapshots generated.
        """
        p = database.ph()
        if ticker:
            outcomes = database.fetchall_dicts(
                self.col.conn,
                f"SELECT * FROM market_outcomes WHERE ticker = {p}", (ticker,)
            )
        else:
            outcomes = database.fetchall_dicts(
                self.col.conn,
                """SELECT o.* FROM market_outcomes o
                   LEFT JOIN market_snapshots s ON o.ticker = s.ticker
                   WHERE s.ticker IS NULL"""
            )

        if not outcomes:
            log.info("No outcomes need synthetic snapshots")
            return 0

        total = 0

        for outcome in outcomes:
            count = self._generate_path(outcome, num_snapshots, hours_before_close)
            total += count

        log.info("Generated %d synthetic snapshots for %d markets", total, len(outcomes))
        return total

    def _generate_path(self, outcome: dict, num_snapshots: int, hours_before: float) -> int:
        """Generate a mean-reverting price path converging to the outcome."""
        ticker = outcome["ticker"]
        result = outcome["result"]
        target = 1.0 if result == "yes" else 0.0
        series = outcome.get("series", "")
        title = outcome.get("title", "")

        close_str = outcome.get("close_time", "")
        if not close_str:
            return 0
        try:
            close_time = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return 0

        start_time = close_time - timedelta(hours=hours_before)
        interval = timedelta(hours=hours_before) / num_snapshots

        # Price path: mean-reverting random walk toward target
        mid = 0.50
        rows = []
        for i in range(num_snapshots):
            snap_time = start_time + interval * i
            minutes_left = (close_time - snap_time).total_seconds() / 60
            if minutes_left <= 0:
                break

            # Drift toward target, stronger as time runs out
            progress = i / num_snapshots
            drift_strength = 0.02 + 0.08 * progress
            drift = (target - mid) * drift_strength
            noise = random.gauss(0, 0.03 * (1 - progress * 0.5))
            mid = max(0.01, min(0.99, mid + drift + noise))

            # Spread narrows near close
            spread = random.uniform(0.02, 0.08) * (1 - progress * 0.5)
            yes_bid = max(0.01, mid - spread / 2)
            yes_ask = min(0.99, mid + spread / 2)
            volume = int(random.uniform(10, 500) * (1 + progress))

            tags = []
            if any(series.upper().startswith(p) for p in WEATHER_SERIES_PREFIXES):
                tags.append("weather")
            if minutes_left <= 60:
                tags.append("expiring")

            rows.append((
                snap_time.isoformat(),
                ticker,
                title,
                series,
                round(yes_bid, 4),
                round(yes_ask, 4),
                round(mid, 4),
                volume,
                close_time.isoformat(),
                round(minutes_left, 1),
                json.dumps(tags),
                json.dumps({"synthetic": True, "outcome": result}),
            ))

        if rows:
            p = database.ph()
            database.executemany(
                self.col.conn,
                f"""INSERT INTO market_snapshots
                   (snapshot_time, ticker, title, series, yes_bid, yes_ask, mid,
                    volume, close_time, minutes_to_close, tags, raw)
                   VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})""",
                rows,
            )
            self.col.conn.commit()

        return len(rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    days = 90
    for i, arg in enumerate(sys.argv):
        if arg == "--days" and i + 1 < len(sys.argv):
            days = int(sys.argv[i + 1])

    synthetic = "--synthetic" in sys.argv

    from config import load_from_env
    from client import KalshiClient

    cfg = load_from_env()
    if not cfg.api_key:
        log.error("KALSHI_API_KEY not set.")
        sys.exit(1)

    client = KalshiClient(cfg.api_key, cfg.private_key_path, demo=cfg.demo)
    scraper = HistoricalScraper(client)

    log.info("Scraping settled markets (last %d days)...", days)
    count = scraper.scrape_settled(days_back=days)
    log.info("Scraped %d outcomes", count)

    if synthetic:
        log.info("Generating synthetic snapshots...")
        snap_count = scraper.generate_synthetic_snapshots()
        log.info("Generated %d synthetic snapshots", snap_count)

    col = scraper.col
    log.info("DB totals: %d snapshots, %d outcomes", col.get_snapshot_count(), col.get_outcome_count())
