"""
Vercel serverless function — proxies to Railway bot metrics,
falls back to simulated demo data if RAILWAY_METRICS_URL is not set.
"""

from http.server import BaseHTTPRequestHandler
import json
import os
import random
import datetime
import time

RAILWAY_URL = os.environ.get("RAILWAY_METRICS_URL", "")


def _generate_simulation():
    """Generate a realistic metrics snapshot using only stdlib."""
    now = datetime.datetime.now(datetime.timezone.utc)
    ts = now.strftime("%H:%M:%S UTC")

    # Seed on current minute so data is stable across 3s polls
    random.seed(int(time.time() / 60))

    loop_count = int((time.time() % 86400) / 30)

    # --- Weather tickers ---
    weather_tickers = [
        ("KXHIGH-26MAR26-SFO-T68", "High in San Francisco above 68F on Mar 26?"),
        ("KXHIGH-26MAR26-NYC-T55", "High in New York above 55F on Mar 26?"),
        ("KXRAIN-26MAR26-CHI", "Will it rain in Chicago on Mar 26?"),
        ("KXLOW-26MAR26-MIA-T60", "Low in Miami above 60F on Mar 26?"),
        ("KXHIGH-27MAR26-LAX-T75", "High in Los Angeles above 75F on Mar 27?"),
        ("KXHIGH-26MAR26-DEN-T62", "High in Denver above 62F on Mar 26?"),
        ("KXRAIN-26MAR26-SEA", "Will it rain in Seattle on Mar 26?"),
        ("KXHIGH-26MAR26-ATL-T70", "High in Atlanta above 70F on Mar 26?"),
    ]

    # --- Expiring tickers ---
    expiring_tickers = [
        ("KXBTC-25MAR26-T88000", "Bitcoin above $88,000 on Mar 25?"),
        ("KXETH-25MAR26-T2100", "Ethereum above $2,100 on Mar 25?"),
        ("KXSP500-25MAR26-T5700", "S&P 500 close above 5,700 on Mar 25?"),
        ("KXNASDAQ-25MAR26-T18000", "NASDAQ close above 18,000 on Mar 25?"),
        ("KXGOLD-25MAR26-T3050", "Gold above $3,050 on Mar 25?"),
    ]

    # Build candidates
    candidates = []
    for ticker, title in weather_tickers:
        yes_bid = random.randint(15, 85) / 100
        spread = random.randint(2, 8) / 100
        candidates.append({
            "ticker": ticker,
            "title": title,
            "tags": ["weather"],
            "yes_bid": round(yes_bid, 2),
            "yes_ask": round(yes_bid + spread, 2),
            "mid": round(yes_bid + spread / 2, 2),
            "volume": random.randint(50, 2000),
            "minutes_to_close": round(random.uniform(120, 2880), 1),
        })

    for ticker, title in expiring_tickers:
        yes_bid = random.choice([random.randint(5, 25), random.randint(72, 94)]) / 100
        spread = random.randint(1, 5) / 100
        mins_left = random.randint(8, 55)
        candidates.append({
            "ticker": ticker,
            "title": title,
            "tags": ["expiring"],
            "yes_bid": round(yes_bid, 2),
            "yes_ask": round(min(yes_bid + spread, 0.99), 2),
            "mid": round(yes_bid + spread / 2, 2),
            "volume": random.randint(100, 5000),
            "minutes_to_close": float(mins_left),
        })

    # Positions (pick 3 random candidates)
    pos_picks = random.sample(candidates, min(3, len(candidates)))
    positions = []
    for c in pos_picks:
        side = random.choice(["yes", "no"])
        positions.append({
            "ticker": c["ticker"],
            "title": c["title"],
            "side": side,
            "contracts": random.randint(1, 10),
            "avg_price": round(random.uniform(0.10, 0.80), 2),
            "current_bid": c["yes_bid"],
            "current_ask": c["yes_ask"],
            "unrealized_pnl": round(random.uniform(-5, 10), 2),
        })

    # Orders
    statuses = ["placed", "dry_run", "dry_run", "dry_run", "blocked"]
    strategies = ["fair_value", "expiry_momentum"]
    orders = []
    for i in range(random.randint(8, 15)):
        c = random.choice(candidates)
        status = random.choice(statuses)
        orders.append({
            "ts": ts,
            "ticker": c["ticker"],
            "side": random.choice(["yes", "no"]),
            "count": random.randint(1, 5),
            "price_cents": random.randint(5, 90),
            "status": status,
            "reason": "edge detected" if status != "blocked" else "max positions reached",
            "strategy": random.choice(strategies),
        })

    # Log lines
    log_lines = []
    for i in range(20):
        loop_n = max(1, loop_count - i)
        line_type = random.choice(["scan", "order", "loop", "info"])
        if line_type == "scan":
            log_lines.append(f"[{ts}] {len(candidates)} candidates from {len(candidates) + random.randint(50, 200)} markets")
        elif line_type == "order":
            c = random.choice(candidates)
            log_lines.append(f"[{ts}] DRY {c['ticker']} YES x{random.randint(1, 5)} @ {random.randint(10, 85)}c")
        elif line_type == "loop":
            log_lines.append(f"[{ts}] Loop #{loop_n} done -- {random.randint(0, 3)} orders")
        else:
            log_lines.append(f"[{ts}] Balance: ${random.uniform(400, 600):.2f} | {len(positions)} open positions")

    available = round(random.uniform(400, 600), 2)
    portfolio = round(available + random.uniform(0, 100), 2)
    daily_pnl = round(random.uniform(-15, 30), 2)

    n_weather = sum(1 for c in candidates if "weather" in c["tags"])
    n_expiring = sum(1 for c in candidates if "expiring" in c["tags"])

    return {
        "status": {
            "running": True,
            "demo": True,
            "dry_run": True,
            "loop_count": loop_count,
            "started_at": (now - datetime.timedelta(hours=2)).strftime("%H:%M:%S UTC"),
            "last_scan_at": ts,
            "strategies": ["fair_value", "expiry_momentum"],
        },
        "balance": {
            "available_dollars": available,
            "portfolio_value_dollars": portfolio,
            "daily_pnl_dollars": daily_pnl,
            "updated_at": ts,
        },
        "positions": positions,
        "orders": orders,
        "candidates": candidates,
        "scan_stats": {
            "total_markets": len(candidates) + random.randint(50, 200),
            "candidates": len(candidates),
            "weather": n_weather,
            "expiring": n_expiring,
            "orders_placed": len([o for o in orders if o["status"] == "placed"]),
        },
        "log_lines": log_lines,
    }


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if RAILWAY_URL:
            try:
                import urllib.request
                req = urllib.request.Request(f"{RAILWAY_URL.rstrip('/')}/metrics")
                req.add_header("Accept", "application/json")
                with urllib.request.urlopen(req, timeout=8) as resp:
                    data = resp.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data)
                return
            except Exception:
                pass  # fall through to simulation

        data = _generate_simulation()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()
