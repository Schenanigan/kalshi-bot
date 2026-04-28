"""
Run FairValueStrategy.evaluate_batch (partition-sum arb) on REAL Kalshi
weather markets via the production scanner. Read-only — no orders.
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from client import KalshiClient
from scanner import scan
from strategy import FairValueStrategy

api_key = os.environ.get("KALSHI_API_KEY", "")
client = KalshiClient(api_key, "kalshi_key.pem", demo=False)


def get(path: str, params: dict) -> dict:
    full_path = urlparse(client.base_url + path).path
    headers = client._headers("GET", full_path)
    r = client.session.get(client.base_url + path, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()


# Series with real KXHIGH/KXLOW markets (city-specific, per probe)
SERIES = [
    "KXHIGHNY", "KXHIGHTLV", "KXHIGHTOKC", "KXHIGHTEMPDEN",
    "KXLOWNY", "KXLOWDEN",
]

raw: list[dict] = []
for series in SERIES:
    print(f"Fetching events for {series}…")
    events_data = get("/events", {"series_ticker": series, "status": "open", "limit": 20})
    events = events_data.get("events", [])
    print(f"  -> {len(events)} events")
    for ev in events:
        m_data = get("/markets", {"event_ticker": ev["event_ticker"], "limit": 100})
        raw.extend(m_data.get("markets", []))

print(f"\nFetched {len(raw)} raw market dicts")

# Run them through the production scanner — this validates the schema fix.
candidates = scan(
    raw,
    expiring_within_minutes=60 * 24 * 7,
    include_weather=True,
    include_expiring=False,
    min_volume=0,
    max_spread=1.0,
)
print(f"Scanner produced {len(candidates)} CandidateMarket objects")

if not candidates:
    print("FAIL: scanner produced 0 candidates — schema fix broken")
    sys.exit(1)

# City resolution sanity check
strat = FairValueStrategy()
resolved = sum(1 for c in candidates if strat._resolve_city(c) is not None)
print(f"City resolution: {resolved}/{len(candidates)} candidates resolved")

# Show sample
print("\nSample candidates:")
for c in candidates[:8]:
    city = strat._resolve_city(c)
    print(f"  {c.ticker:<35} {c.strike_type:<10} mid={c.mid:.2f} city={city} event={c.event_ticker}")

# Group by event for the partition-arb summary
from collections import defaultdict
by_event = defaultdict(list)
for c in candidates:
    by_event[c.event_ticker].append(c)

print(f"\nEvents covered: {len(by_event)}")
print("Partition snapshot per event (Σ yes_ask should be ≥1 for fair markets):")
for ev, legs in by_event.items():
    n = len(legs)
    sum_yes_ask = sum(m.yes_ask for m in legs)
    sum_yes_bid = sum(m.yes_bid for m in legs)
    sum_no_ask = n - sum_yes_bid
    yes_edge = 1.0 - sum_yes_ask
    no_edge = (n - 1) - sum_no_ask
    flag = "  ARB!" if max(yes_edge, no_edge) >= 0.03 else ""
    print(f"  {ev:<25} N={n}  Σyes_ask={sum_yes_ask:.3f}  Σno_ask={sum_no_ask:.3f}  "
          f"yes_edge={yes_edge:+.3f}  no_edge={no_edge:+.3f}{flag}")

# Run the actual strategy
print("\n" + "=" * 60)
print("Running FairValueStrategy.evaluate_batch()…")
intents = strat.evaluate_batch(candidates)

print(f"\nRESULT: {len(intents)} partition-arb intents")
for i in intents:
    print(f"  {i.ticker:<40} {i.side.upper():<3} ×{i.count} @ {i.limit_price}¢")
    print(f"    {i.reason}")
