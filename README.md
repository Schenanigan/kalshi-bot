# Kalshi Trading Bot

Targets two market types:
- **Expiring markets** — open markets closing within 60 minutes (pricing often goes stale)
- **Weather markets** — NWS forecast vs. Kalshi implied probability

```
kalshi_bot/
├── config.py      ← all tunable parameters
├── client.py      ← Kalshi REST API wrapper
├── scanner.py     ← finds target markets
├── strategy.py    ← ExpiringStrategy + WeatherStrategy
├── risk.py        ← exposure limits and position sizing
└── bot.py         ← main loop (run this)
```

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Get Kalshi API credentials
1. Go to [kalshi.com](https://kalshi.com) → Profile → API Keys
2. Create a key and download the `.pem` private key file
3. Copy the Key ID

### 3. Configure
Edit `config.py`:
```python
KALSHI_API_KEY_ID   = "your-key-id-here"
KALSHI_API_KEY_FILE = "kalshi_key.pem"   # path to your downloaded .pem

WEATHER_NWS_OFFICE  = "MTR"   # Bay Area; see GRID_POINTS in strategy.py for other cities
DRY_RUN             = True    # ← keep True until you're confident
```

### 4. Run
```bash
python bot.py
```

---

## How it works

### Expiring Strategy
Looks for markets where the ask price is meaningfully below the mid (panicked sellers).
- Edge threshold: 6¢ (configurable via `EXPIRING_EDGE_THRESHOLD`)
- Skips markets with mid < 5¢ or > 95¢ (near-certain outcomes)
- Scales order size with edge magnitude

### Weather Strategy
Pulls NWS point forecasts and compares against Kalshi's implied probability.
- For temperature markets: uses a normal distribution around the NWS forecast (±3°F)
- For rain markets: uses NWS PoP (Probability of Precipitation) directly
- Edge threshold: 8¢ (configurable via `WEATHER_EDGE_THRESHOLD`)

To add a new city, add an entry to `WeatherStrategy.GRID_POINTS` in `strategy.py`:
```python
# Find your grid with: curl https://api.weather.gov/points/{lat},{lon}
"DEN": ("BOU", 61, 62),   # Denver
```

---

## Risk controls
| Parameter | Default | Description |
|---|---|---|
| `MAX_POSITION_PER_MARKET` | 50 | Max contracts in any one market |
| `MAX_TOTAL_EXPOSURE` | $500 | Total $ at risk across all positions |
| `MAX_POSITIONS_OPEN` | 10 | Max concurrent open positions |
| `DRY_RUN` | `True` | Log orders without submitting |

---

## Improving the expiring strategy

The expiring strategy is intentionally simple — a real edge here comes from:

1. **External data sources** — for political/event markets, you need a real signal
   (polls, news, sports data) not just the order book
2. **Order flow imbalance** — if one side is being heavily sold, fade it
3. **Historical resolution rates** — some market types resolve YES far more often than priced

A good next step is to add a `signal_fn` parameter to `ExpiringStrategy` so you can inject
custom probability estimates per market category.

---

## Notes
- Kalshi's API rate limits: ~10 req/sec. The scanner batches all markets in one call.
- The `_traded` set in `bot.py` prevents re-trading the same market within a session.
  Remove it if you want to add to positions incrementally.
- All logs go to stdout and `bot.log`.
