"""Pull live KXHIGHNY markets and dump structure to see if threshold-arb applies."""
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from client import KalshiClient

api_key = os.environ.get("KALSHI_API_KEY", "")
client = KalshiClient(api_key, "kalshi_key.pem", demo=False)


def get(path: str, params: dict) -> dict:
    full_path = urlparse(client.base_url + path).path
    headers = client._headers("GET", full_path)
    r = client.session.get(client.base_url + path, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()


# Step 1: get events for KXHIGHNY
print("=" * 60)
print("Events under series KXHIGHNY:")
data = get("/events", {"series_ticker": "KXHIGHNY", "status": "open", "limit": 20})
events = data.get("events", [])
print(f"  -> {len(events)} events")
for e in events[:5]:
    print(f"    {e.get('event_ticker'):<35} {e.get('title','')[:60]}")

if not events:
    print("No events. Trying without status filter…")
    data = get("/events", {"series_ticker": "KXHIGHNY", "limit": 20})
    events = data.get("events", [])
    print(f"  -> {len(events)} events")
    for e in events[:5]:
        print(f"    {e.get('event_ticker'):<35} {e.get('title','')[:60]}")

if not events:
    sys.exit(1)

# Step 2: get markets for the first event
event_ticker = events[0]["event_ticker"]
print()
print("=" * 60)
print(f"Markets under event {event_ticker}:")
mdata = get("/markets", {"event_ticker": event_ticker, "limit": 100})
markets = mdata.get("markets", [])
print(f"  -> {len(markets)} markets")

if markets:
    print()
    print("First market full dict:")
    for k, v in markets[0].items():
        sv = str(v)
        if len(sv) > 80:
            sv = sv[:80] + "…"
        print(f"  {k:<28} {sv}")

print()
print("All markets, key fields:")
for m in markets:
    print(
        f"  {m.get('ticker'):<45}",
        f"yes_bid={m.get('yes_bid_dollars')}",
        f"yes_ask={m.get('yes_ask_dollars')}",
        f"vol24h={m.get('volume_24h_fp')}",
        f"title={m.get('title','')[:50]}",
    )
