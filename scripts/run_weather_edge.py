"""
Run FairValueStrategy on REAL Kalshi weather markets — both per-market
(threshold + range) and partition-arb — and report all signals found.
"""
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from client import KalshiClient
from scanner import scan
from strategy import FairValueStrategy

logging.basicConfig(level=logging.INFO, format="%(name)s — %(message)s")
log = logging.getLogger("weather-edge")

api_key = os.environ.get("KALSHI_API_KEY", "")
client = KalshiClient(api_key, "kalshi_key.pem", demo=False)


def get(path, params):
    fp = urlparse(client.base_url + path).path
    h = client._headers("GET", fp)
    r = client.session.get(client.base_url + path, params=params, headers=h, timeout=15)
    r.raise_for_status()
    return r.json()


SERIES = [
    "KXHIGHNY", "KXHIGHTLV", "KXHIGHTOKC", "KXHIGHTEMPDEN",
    "KXLOWNY", "KXLOWDEN",
]

raw = []
for series in SERIES:
    events_data = get("/events", {"series_ticker": series, "status": "open", "limit": 20})
    for ev in events_data.get("events", []):
        raw.extend(get("/markets", {"event_ticker": ev["event_ticker"], "limit": 100}).get("markets", []))

candidates = scan(
    raw,
    expiring_within_minutes=60 * 24 * 7,
    include_weather=True,
    include_expiring=False,
    min_volume=0,
    max_spread=1.0,
)
log.info("Parsed %d candidates from %d raw markets", len(candidates), len(raw))

strat = FairValueStrategy()

# Per-market signals (FairValueStrategy.evaluate)
log.info("=" * 60)
log.info("PER-MARKET SIGNALS:")
trades = []
for c in candidates:
    intent = strat.evaluate(c, {})
    if intent:
        trades.append((c, intent))

if not trades:
    log.info("  No per-market signals found.")
else:
    for c, intent in trades:
        log.info(
            "  %s [%s] %s ×%d @ %dc — %s",
            c.ticker, c.strike_type, intent.side.upper(),
            intent.count, intent.limit_price, intent.reason,
        )

# Partition-arb signals
log.info("")
log.info("=" * 60)
log.info("PARTITION-ARB SIGNALS:")
arb_intents = strat.evaluate_batch(candidates)
if not arb_intents:
    log.info("  No partition-arb signals found.")
else:
    for i in arb_intents:
        log.info("  %s %s ×%d @ %dc — %s", i.ticker, i.side.upper(), i.count, i.limit_price, i.reason)

log.info("")
log.info("=" * 60)
log.info("SUMMARY: %d per-market signals, %d partition-arb intents", len(trades), len(arb_intents))
