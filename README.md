# Kalshi Trading Bot

Targets two market types:
- **Expiring markets** — open markets closing within 60 minutes (pricing often goes stale)
- **Weather markets** — any US city, dynamically resolved via NWS forecast API

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
Set environment variables (see `config.py` for all options):
```bash
export KALSHI_API_KEY="your-key-id-here"
export KALSHI_KEY_PATH="kalshi_key.pem"   # path to your downloaded .pem
export KALSHI_DRY_RUN="true"              # keep true until you're confident
```

### 4. Run
```bash
python bot.py              # live mode (uses Kalshi API)
python bot.py --simulate   # simulation mode (synthetic markets, no API needed)
```

---

## How it works

### Expiring Strategy
Looks for markets where the ask price is meaningfully below the mid (panicked sellers).
- Edge threshold: 6¢ (configurable via `EXPIRING_EDGE_THRESHOLD`)
- Skips markets with mid < 5¢ or > 95¢ (near-certain outcomes)
- Scales order size with edge magnitude

### Weather Strategy
Trades **any** expiring weather market across all US cities. The bot:
1. Extracts the city from the ticker code (e.g. `SFO`, `NYC`, `DEN`)
2. Dynamically resolves the NWS grid point via the `/points/{lat},{lon}` API (cached per city)
3. Fetches the NWS forecast for that grid point

Supported weather market types:
- **KXHIGH / KXLOW** (temperature): uses a normal distribution around the NWS forecast (±3°F)
- **KXRAIN** (precipitation): uses NWS PoP (Probability of Precipitation) directly
- **KXSNOW / KXWIND** (snow, wind): falls back to mean-reversion heuristic

When NWS lookup fails for any reason, the bot falls back to mean-reversion instead of skipping the market.

Edge threshold: 8¢ (configurable via `WEATHER_EDGE_THRESHOLD`)

~20 US cities are pre-configured with coordinates. To add a new city, add its lat/lon to
`CITY_COORDS` and its ticker code to `TICKER_CITY_MAP` in `strategy.py`.

---

## Simulation mode

Run `python bot.py --simulate` to test the full pipeline without API credentials. This generates
synthetic markets across multiple cities and weather types, runs them through the scanner,
strategies, and risk manager, and outputs signals to the dashboard and logs.

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
- Weather market fetches are filtered to markets expiring within 48 hours.
- All logs go to stdout and `bot.log`.
