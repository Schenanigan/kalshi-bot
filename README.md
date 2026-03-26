# Kalshi Trading Bot

Automated trading bot for [Kalshi](https://kalshi.com) prediction markets with a real-time dashboard.

**Live dashboard:** [kalshi-bot.vercel.app](https://kalshi-bot.vercel.app)

**Architecture:**
```
Browser  →  Vercel (static dashboard)  →  /api/metrics  →  Railway (bot + metrics server)  →  Kalshi API
```

Targets two market types:
- **Expiring markets** — open markets closing within 60 minutes (pricing often goes stale)
- **Weather markets** — any US city, dynamically resolved via NWS forecast API

---

## Project structure

```
kalshi-bot/
├── bot.py              ← main loop (run this)
├── client.py           ← Kalshi REST API wrapper (RSA-PSS auth)
├── config.py           ← all tunable parameters
├── scanner.py          ← finds target markets
├── strategy.py         ← FairValueStrategy + ExpiryMomentumStrategy
├── risk.py             ← exposure limits and position sizing
├── metrics.py          ← FastAPI metrics server (background thread)
├── dashboard.html      ← real-time monitoring UI (local dev)
├── requirements.txt    ← Python dependencies
│
├── public/
│   └── index.html      ← dashboard for Vercel (auto-detects API URL)
├── api/
│   └── metrics.py      ← Vercel serverless proxy to Railway
│
├── Dockerfile          ← Python 3.12 container for Railway
├── railway.json        ← Railway config
├── vercel.json         ← Vercel config
├── .dockerignore       ← container build excludes
└── .mcp.json           ← Vercel MCP server for Claude Code
```

---

## Local setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Get Kalshi API credentials
1. Go to [kalshi.com](https://kalshi.com) → Profile → API Keys
2. Create a key and download the `.pem` private key file
3. Copy the Key ID

### 3. Configure
```bash
export KALSHI_API_KEY="your-key-id-here"
export KALSHI_KEY_PATH="kalshi_key.pem"
export KALSHI_DRY_RUN="true"
```

### 4. Run
```bash
python bot.py              # live mode (uses Kalshi API)
python bot.py --simulate   # simulation mode (fake markets, no API needed)
```

Open `dashboard.html` in your browser to see the real-time dashboard.

---

## Deployment

### Railway (bot process)

The bot runs 24/7 on Railway as a Docker container.

1. Create a [Railway](https://railway.com) account and connect your GitHub repo
2. Deploy the `main` branch — Railway auto-detects the Dockerfile
3. Generate a public domain under Settings → Networking
4. Set environment variables:

| Variable | Required | Description |
|---|---|---|
| `KALSHI_API_KEY` | Yes | Your Kalshi API key UUID |
| `KALSHI_KEY_B64` | Yes | Base64-encoded PEM key (run `base64 -i kalshi_key.pem`) |
| `KALSHI_DEMO` | No | `true` for sandbox API, `false` for production (default: `true`) |
| `KALSHI_DRY_RUN` | No | `true` to log orders without placing them (default: `true`) |

`PORT` is auto-injected by Railway. The metrics server auto-binds to `0.0.0.0` when `PORT` is set.

Verify: `https://your-railway-domain.up.railway.app/health` should return `{"ok": true}`

### Vercel (dashboard)

The dashboard is a static site on Vercel with a serverless API proxy.

1. Create a [Vercel](https://vercel.com) account and import your GitHub repo
2. Set environment variable:

| Variable | Value |
|---|---|
| `RAILWAY_METRICS_URL` | `https://your-railway-domain.up.railway.app` |

3. Deploy — the dashboard will proxy `/api/metrics` requests to Railway

If `RAILWAY_METRICS_URL` is not set, the dashboard falls back to simulated demo data.

---

## Environment variables reference

| Variable | Default | Description |
|---|---|---|
| `KALSHI_API_KEY` | (empty) | Kalshi API key UUID |
| `KALSHI_KEY_B64` | (empty) | Base64-encoded PEM private key (alternative to file) |
| `KALSHI_KEY_PATH` | `kalshi_key.pem` | Path to PEM private key file |
| `KALSHI_DEMO` | `true` | Use demo/sandbox API |
| `KALSHI_DRY_RUN` | `true` | Log orders without submitting |
| `KALSHI_EXPIRING_MINUTES` | `60` | Expiring market window |
| `KALSHI_INCLUDE_WEATHER` | `true` | Enable weather strategies |
| `KALSHI_INCLUDE_EXPIRING` | `true` | Enable expiring strategies |
| `KALSHI_MIN_VOLUME` | `0` | Minimum 24h volume filter |
| `KALSHI_MAX_SPREAD` | `0.40` | Maximum spread tolerance ($) |
| `KALSHI_STRATEGIES` | `all` | Active strategies (comma-separated) |
| `KALSHI_MAX_TRADE` | `25.0` | Max per-trade risk ($) |
| `KALSHI_MAX_POSITIONS` | `10` | Max concurrent positions |
| `KALSHI_MAX_DAILY_LOSS` | `100.0` | Daily loss limit ($) |
| `KALSHI_ALLOW_DUPES` | `false` | Allow multiple orders on same ticker |
| `KALSHI_SCAN_INTERVAL` | `30` | Loop interval (seconds) |

---

## How it works

### Expiring Strategy
Looks for markets where the ask price is meaningfully below the mid (panicked sellers).
- Edge threshold: 6c (configurable via `EXPIRING_EDGE_THRESHOLD`)
- Skips markets with mid < 5c or > 95c (near-certain outcomes)
- Scales order size with edge magnitude

### Weather Strategy
Trades **any** expiring weather market across all US cities. The bot:
1. Extracts the city from the ticker code (e.g. `SFO`, `NYC`, `DEN`)
2. Dynamically resolves the NWS grid point via the `/points/{lat},{lon}` API (cached per city)
3. Fetches the NWS forecast for that grid point

Supported weather market types:
- **KXHIGH / KXLOW** (temperature): uses a normal distribution around the NWS forecast (+/-3F)
- **KXRAIN** (precipitation): uses NWS PoP (Probability of Precipitation) directly
- **KXSNOW / KXWIND** (snow, wind): falls back to mean-reversion heuristic

When NWS lookup fails for any reason, the bot falls back to mean-reversion instead of skipping the market.

Edge threshold: 8c (configurable via `WEATHER_EDGE_THRESHOLD`)

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

## Notes
- Kalshi's API rate limits: ~10 req/sec. The scanner batches all markets in one call.
- Weather market fetches are filtered to markets expiring within 48 hours.
- All logs go to stdout. `bot.log` is written when the filesystem is writable (skipped in containers).
- The PEM key can be provided as a file (`KALSHI_KEY_PATH`) or base64-encoded env var (`KALSHI_KEY_B64`). The env var takes priority.
