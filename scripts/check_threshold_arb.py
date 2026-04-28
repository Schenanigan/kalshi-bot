"""
check_threshold_arb.py — One-shot read-only scan for threshold-arb edge.

Pulls live KXHIGH/KXLOW markets from Kalshi, runs the new
FairValueStrategy.evaluate_batch(), and reports any monotonicity
violations (inversions) it finds. Places NO orders.

Usage:
    python scripts/check_threshold_arb.py
"""

import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

# Make repo root importable when run as `python scripts/check_threshold_arb.py`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client import KalshiClient
from scanner import scan
from strategy import FairValueStrategy
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(name)s — %(message)s",
)
log = logging.getLogger("arb-check")


def main() -> int:
    api_key = os.environ.get("KALSHI_API_KEY", "")
    key_path = os.environ.get("KALSHI_KEY_PATH", "kalshi_key.pem")
    demo = os.environ.get("KALSHI_DEMO", "false").lower() == "true"

    if not api_key:
        log.error("KALSHI_API_KEY not set — cannot authenticate")
        return 1

    client = KalshiClient(api_key, key_path, demo=demo)

    # Fetch each weather series separately (the API filters server-side)
    raw: list[dict] = []
    for series in ("KXHIGH", "KXLOW"):
        log.info("Fetching live %s markets…", series)
        try:
            mkts = client.get_markets(status="open", series=series)
        except Exception as e:
            log.error("Failed to fetch %s: %s", series, e)
            continue
        log.info("  → %d markets", len(mkts))
        raw.extend(mkts)

    if not raw:
        log.error("No markets fetched. Aborting.")
        return 1

    # Parse into CandidateMarket via the same scanner the bot uses,
    # but with the loosest filters so nothing is culled.
    candidates = scan(
        raw,
        expiring_within_minutes=60 * 24 * 7,  # 1 week
        include_weather=True,
        include_expiring=False,
        min_volume=0,
        max_spread=1.0,
    )
    log.info("Parsed %d candidates from %d raw markets", len(candidates), len(raw))

    if not candidates:
        log.warning("No KXHIGH/KXLOW candidates — strategy has nothing to chew on.")
        return 0

    strat = FairValueStrategy()

    # Show the grouping the strategy will see — if titles don't parse
    # into (city, threshold), this is where it'll be visible.
    groups = strat._group_by_threshold(candidates)
    log.info("Strategy grouped candidates into %d (series, city, date) buckets", len(groups))

    parsed = sum(len(v) for v in groups.values())
    unparsed = len(candidates) - parsed
    log.info("  parsed: %d   unparsed (no city/threshold): %d", parsed, unparsed)

    multi_threshold_groups = {k: v for k, v in groups.items() if len(v) >= 2}
    log.info("  groups with ≥2 thresholds (eligible for arb): %d", len(multi_threshold_groups))

    if not multi_threshold_groups:
        log.warning(
            "No groups have ≥2 same-city/same-day thresholds. "
            "Real Kalshi KXHIGH/KXLOW are usually mutually-exclusive *range* "
            "markets (e.g. '60-65°F'), not above-threshold markets. "
            "The current parser pulls the first number from the title, which "
            "is the range floor, not a directional threshold — so monotonicity "
            "doesn't apply. The new approach will likely fire 0 trades against live data."
        )

    # Show the top groups by size for sanity
    log.info("─" * 60)
    log.info("Top groups (by # thresholds):")
    for key, mkts in sorted(groups.items(), key=lambda kv: -len(kv[1]))[:10]:
        prefix, city, date = key
        sorted_mkts = sorted(mkts, key=lambda x: x[0])
        rows = ", ".join(f"{t:.0f}°F@{m.mid * 100:.0f}¢" for t, m in sorted_mkts)
        log.info("  %s %s %s  (%d)  %s", prefix, city, date, len(mkts), rows)

    # Run the actual strategy
    log.info("─" * 60)
    log.info("Running FairValueStrategy.evaluate_batch()…")
    intents = strat.evaluate_batch(candidates)

    if not intents:
        log.info("RESULT: 0 threshold-arb intents generated.")
        return 0

    log.info("RESULT: %d intents generated:", len(intents))
    by_ticker: dict[str, list] = defaultdict(list)
    for i in intents:
        by_ticker[i.ticker].append(i)
    for ticker, ilist in by_ticker.items():
        for intent in ilist:
            log.info(
                "  %s  side=%s  count=%d  limit=%d¢  reason=%s",
                ticker, intent.side, intent.count, intent.limit_price, intent.reason,
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
