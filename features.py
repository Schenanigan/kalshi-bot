"""
features.py — Extract ML training features from market snapshots + outcomes.

Usage:
    python features.py                         # export to data/features.csv
    python features.py --output my_data.csv    # custom output path
"""

import csv
import json
import logging
import math
import sqlite3
import sys
from dataclasses import dataclass, fields, asdict
from typing import Optional

from config import WEATHER_SERIES_PREFIXES

log = logging.getLogger(__name__)

DB_PATH = "data/kalshi.db"


@dataclass
class FeatureRow:
    ticker: str
    snapshot_time: str
    # Market features
    mid: float
    spread: float
    volume: int
    minutes_to_close: float
    log_minutes_to_close: float
    # Series features
    series_prefix: str
    is_weather: int              # 0 or 1
    is_expiring: int             # 0 or 1
    # Price features
    yes_bid: float
    yes_ask: float
    price_distance_from_50: float
    # Momentum features (require multiple snapshots)
    price_momentum_5: float
    price_momentum_10: float
    volatility_5: float
    spread_trend: float
    volume_trend: float
    # Label
    outcome: int                 # 1 = YES, 0 = NO


class FeatureExtractor:
    def __init__(self, db_path: str = DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def extract_all(
        self,
        series_filter: list[str] = None,
        min_snapshots: int = 3,
    ) -> list[FeatureRow]:
        """Extract features for all snapshots that have a matching outcome."""
        # Get tickers with outcomes
        query = "SELECT ticker, result FROM market_outcomes WHERE result IN ('yes', 'no')"
        params = []
        if series_filter:
            placeholders = ",".join("?" * len(series_filter))
            query += f" AND series IN ({placeholders})"
            params.extend(series_filter)

        outcomes = {}
        for row in self.conn.execute(query, params):
            outcomes[row["ticker"]] = 1 if row["result"] == "yes" else 0

        if not outcomes:
            log.warning("No outcomes found in database")
            return []

        all_features = []
        processed = 0

        for ticker, outcome_label in outcomes.items():
            snapshots = self.conn.execute(
                """SELECT * FROM market_snapshots
                   WHERE ticker = ? ORDER BY snapshot_time ASC""",
                (ticker,),
            ).fetchall()

            if len(snapshots) < min_snapshots:
                continue

            snap_dicts = [dict(s) for s in snapshots]
            mids = [s["mid"] for s in snap_dicts]
            spreads = [s["yes_ask"] - s["yes_bid"] for s in snap_dicts]
            volumes = [s["volume"] or 0 for s in snap_dicts]

            for i, snap in enumerate(snap_dicts):
                tags = json.loads(snap["tags"]) if snap["tags"] else []
                series = snap["series"] or ""
                series_prefix = series.split("-")[0] if series else ""
                is_weather = 1 if any(series.upper().startswith(p) for p in WEATHER_SERIES_PREFIXES) else 0
                is_expiring = 1 if "expiring" in tags else 0
                mtc = snap["minutes_to_close"] or 0

                feature = FeatureRow(
                    ticker=ticker,
                    snapshot_time=snap["snapshot_time"],
                    mid=snap["mid"],
                    spread=spreads[i],
                    volume=volumes[i],
                    minutes_to_close=mtc,
                    log_minutes_to_close=math.log(mtc + 1) if mtc >= 0 else 0,
                    series_prefix=series_prefix,
                    is_weather=is_weather,
                    is_expiring=is_expiring,
                    yes_bid=snap["yes_bid"],
                    yes_ask=snap["yes_ask"],
                    price_distance_from_50=abs(snap["mid"] - 0.50),
                    price_momentum_5=self._momentum(mids, i, 5),
                    price_momentum_10=self._momentum(mids, i, 10),
                    volatility_5=self._volatility(mids, i, 5),
                    spread_trend=self._momentum(spreads, i, 5),
                    volume_trend=self._ratio_trend(volumes, i, 5),
                    outcome=outcome_label,
                )
                all_features.append(feature)

            processed += 1

        log.info("Extracted %d feature rows from %d tickers", len(all_features), processed)
        return all_features

    def to_csv(self, rows: list[FeatureRow], path: str):
        """Write features to CSV file."""
        if not rows:
            log.warning("No features to write")
            return

        field_names = [f.name for f in fields(FeatureRow)]
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            for row in rows:
                writer.writerow(asdict(row))

        log.info("Wrote %d rows to %s", len(rows), path)

    def to_dataframe(self, rows: list[FeatureRow]):
        """Convert to pandas DataFrame (requires pandas)."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for to_dataframe(). Install with: pip install pandas")
        return pd.DataFrame([asdict(r) for r in rows])

    def summary(self, rows: list[FeatureRow]):
        """Print feature summary statistics."""
        if not rows:
            print("No features extracted")
            return

        outcomes = [r.outcome for r in rows]
        tickers = set(r.ticker for r in rows)
        yes_count = sum(outcomes)
        no_count = len(outcomes) - yes_count

        print(f"\n{'='*50}")
        print("FEATURE EXTRACTION SUMMARY")
        print(f"{'='*50}")
        print(f"  Total rows:     {len(rows)}")
        print(f"  Unique tickers: {len(tickers)}")
        print(f"  Outcomes:       {yes_count} YES ({yes_count/len(outcomes):.1%}), {no_count} NO ({no_count/len(outcomes):.1%})")
        print(f"  Avg snapshots/ticker: {len(rows)/len(tickers):.1f}")

        # Feature ranges
        print(f"\n  {'Feature':<25} {'Min':>8} {'Max':>8} {'Mean':>8}")
        print(f"  {'—'*25} {'—'*8} {'—'*8} {'—'*8}")
        for fname in ["mid", "spread", "minutes_to_close", "price_distance_from_50",
                       "price_momentum_5", "volatility_5"]:
            vals = [getattr(r, fname) for r in rows]
            print(f"  {fname:<25} {min(vals):>8.3f} {max(vals):>8.3f} {sum(vals)/len(vals):>8.3f}")
        print(f"{'='*50}\n")

    # ── Internal ─────────────────────────────────────────────────────────────

    def _momentum(self, values: list[float], idx: int, window: int) -> float:
        """Price change over last `window` snapshots."""
        if idx < window or values[idx - window] == 0:
            return 0.0
        return (values[idx] - values[idx - window]) / values[idx - window]

    def _volatility(self, values: list[float], idx: int, window: int) -> float:
        """Std dev of last `window` values."""
        start = max(0, idx - window + 1)
        window_vals = values[start:idx + 1]
        if len(window_vals) < 2:
            return 0.0
        mean = sum(window_vals) / len(window_vals)
        variance = sum((v - mean) ** 2 for v in window_vals) / (len(window_vals) - 1)
        return math.sqrt(variance)

    def _ratio_trend(self, values: list[int], idx: int, window: int) -> float:
        """Volume ratio: current / avg of previous window."""
        start = max(0, idx - window)
        prev = values[start:idx]
        if not prev:
            return 0.0
        avg = sum(prev) / len(prev)
        if avg == 0:
            return 0.0
        return values[idx] / avg


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    output = "data/features.csv"
    for i, arg in enumerate(sys.argv):
        if arg == "--output" and i + 1 < len(sys.argv):
            output = sys.argv[i + 1]

    extractor = FeatureExtractor()
    rows = extractor.extract_all()

    if rows:
        extractor.summary(rows)
        extractor.to_csv(rows, output)
    else:
        log.warning("No features extracted. Run collector.py or scraper.py first to populate data.")
