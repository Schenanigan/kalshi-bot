"""Probe what events/markets exist in live Kalshi for KXHIGH/KXLOW."""
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


# 1. Try /events with series_ticker=KXHIGH
print("=" * 60)
print("Test 1: /events?series_ticker=KXHIGH")
try:
    data = get("/events", {"series_ticker": "KXHIGH", "status": "open", "limit": 50})
    events = data.get("events", [])
    print(f"  -> {len(events)} events")
    for e in events[:5]:
        print(f"    {e.get('event_ticker'):<35} {e.get('title','')[:60]}")
except Exception as e:
    print(f"  failed: {e}")

# 2. List all series
print()
print("=" * 60)
print("Test 2: /series (list all series)")
try:
    data = get("/series", {"limit": 200})
    series_list = data.get("series", [])
    print(f"  -> {len(series_list)} series")
    weather = [
        s for s in series_list
        if any(
            w in (s.get("ticker", "") + " " + s.get("title", "")).upper()
            for w in ("HIGH", "LOW", "TEMP", "WEATHER", "RAIN", "SNOW", "WIND")
        )
    ]
    print(f"  weather-related: {len(weather)}")
    for s in weather[:30]:
        print(f"    {s.get('ticker'):<25} {s.get('title','')[:60]}")
except Exception as e:
    print(f"  failed: {e}")
