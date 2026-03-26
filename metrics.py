"""
Metrics Server
==============
Runs as a background thread alongside the bot.
The bot calls push_* functions to update state.
The dashboard polls GET /metrics for a JSON snapshot.

Start it from bot.py:
    from metrics import MetricsServer
    srv = MetricsServer()
    srv.start()          # non-blocking, starts on port 8765
    srv.push_status(...)
    ...
    srv.stop()
"""

from __future__ import annotations
import os
import threading
import time
import datetime
import logging
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Optional

try:
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False

log = logging.getLogger(__name__)

PORT = 8765
MAX_LOG_LINES   = 200
MAX_ORDER_LINES = 100


# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class BotStatus:
    running: bool        = False
    demo: bool           = True
    dry_run: bool        = True
    loop_count: int      = 0
    started_at: str      = ""
    last_scan_at: str    = ""
    strategies: list     = field(default_factory=list)

@dataclass
class BalanceSnap:
    available_dollars: float = 0.0
    portfolio_value_dollars: float = 0.0
    daily_pnl_dollars: float = 0.0
    updated_at: str = ""

@dataclass
class Position:
    ticker: str
    title: str
    side: str         # "yes" | "no"
    contracts: int
    avg_price: float  # dollars
    current_bid: float
    current_ask: float
    unrealized_pnl: float

@dataclass
class OrderRecord:
    ts: str
    ticker: str
    side: str
    count: int
    price_cents: int
    status: str       # "placed" | "blocked" | "dry_run"
    reason: str
    strategy: str

@dataclass
class CandidateMarket:
    ticker: str
    title: str
    tags: list
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    mid: Optional[float]
    volume: float
    minutes_to_close: Optional[float]

@dataclass
class ScanStats:
    total_markets: int   = 0
    candidates: int      = 0
    weather: int         = 0
    expiring: int        = 0
    orders_placed: int   = 0


# ── Thread-safe store ─────────────────────────────────────────────────────────

class MetricsStore:
    def __init__(self):
        self._lock = threading.Lock()
        self.status    = BotStatus()
        self.balance   = BalanceSnap()
        self.positions: list[Position]       = []
        self.orders:    deque[OrderRecord]   = deque(maxlen=MAX_ORDER_LINES)
        self.candidates: list[CandidateMarket] = []
        self.scan_stats = ScanStats()
        self.log_lines: deque[str]           = deque(maxlen=MAX_LOG_LINES)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "status":      asdict(self.status),
                "balance":     asdict(self.balance),
                "positions":   [asdict(p) for p in self.positions],
                "orders":      [asdict(o) for o in self.orders],
                "candidates":  [asdict(c) for c in self.candidates],
                "scan_stats":  asdict(self.scan_stats),
                "log_lines":   list(self.log_lines),
            }

    def _now(self) -> str:
        return datetime.datetime.utcnow().strftime("%H:%M:%S UTC")

    # ── Push helpers ──────────────────────────────────────────────────────────

    def push_status(self, **kwargs):
        with self._lock:
            for k, v in kwargs.items():
                if hasattr(self.status, k):
                    setattr(self.status, k, v)
            self.status.last_scan_at = self._now()

    def push_balance(self, available: float, portfolio: float, daily_pnl: float = 0.0):
        with self._lock:
            self.balance = BalanceSnap(
                available_dollars=round(available, 2),
                portfolio_value_dollars=round(portfolio, 2),
                daily_pnl_dollars=round(daily_pnl, 2),
                updated_at=self._now(),
            )

    def push_positions(self, positions: list[dict]):
        with self._lock:
            self.positions = [
                Position(
                    ticker=p.get("ticker", ""),
                    title=p.get("title", p.get("ticker", "")),
                    side=p.get("side", "yes"),
                    contracts=int(p.get("position", 0)),
                    avg_price=float(p.get("fees_paid_dollars") or 0),
                    current_bid=float(p.get("yes_bid_dollars") or 0),
                    current_ask=float(p.get("yes_ask_dollars") or 0),
                    unrealized_pnl=float(p.get("unrealized_pnl") or 0),
                )
                for p in positions
                if int(p.get("position", 0)) != 0
            ]

    def push_candidates(self, candidates, scan_stats: dict):
        with self._lock:
            self.candidates = [
                CandidateMarket(
                    ticker=c.ticker,
                    title=c.title[:60],
                    tags=c.tags,
                    yes_bid=c.yes_bid,
                    yes_ask=c.yes_ask,
                    mid=c.mid,
                    volume=c.volume,
                    minutes_to_close=round(c.minutes_to_close, 1) if c.minutes_to_close else None,
                )
                for c in candidates[:30]  # top 30
            ]
            for k, v in scan_stats.items():
                if hasattr(self.scan_stats, k):
                    setattr(self.scan_stats, k, v)

    def push_order(self, ticker: str, side: str, count: int, price_cents: int,
                   status: str, reason: str, strategy: str):
        with self._lock:
            self.orders.appendleft(OrderRecord(
                ts=self._now(), ticker=ticker, side=side, count=count,
                price_cents=price_cents, status=status,
                reason=reason[:80], strategy=strategy,
            ))

    def push_log(self, line: str):
        with self._lock:
            self.log_lines.appendleft(
                f"[{self._now()}] {line[:120]}"
            )


# ── HTTP server ───────────────────────────────────────────────────────────────

class MetricsServer:
    def __init__(self, port: int = PORT):
        self.port  = port
        self.store = MetricsStore()
        self._thread: threading.Thread | None = None
        self._server: uvicorn.Server | None  = None

    def start(self):
        if not _FASTAPI_AVAILABLE:
            log.warning("fastapi/uvicorn not installed — dashboard disabled. "
                        "Run: pip install fastapi uvicorn")
            return

        app = FastAPI(title="Kalshi Bot Metrics", docs_url=None)
        app.add_middleware(CORSMiddleware,
                           allow_origins=["*"],
                           allow_methods=["GET"],
                           allow_headers=["*"])

        store = self.store

        @app.get("/metrics")
        def metrics():
            return store.snapshot()

        @app.get("/health")
        def health():
            return {"ok": True}

        # If PORT is set (Railway/container), auto-bind to all interfaces
        port_env = os.environ.get("PORT", "")
        if port_env:
            host = "0.0.0.0"
            port = int(port_env)
        else:
            host = "127.0.0.1"
            port = self.port
        config = uvicorn.Config(app, host=host, port=port,
                                log_level="warning", loop="asyncio")
        self._server = uvicorn.Server(config)

        def _run():
            import asyncio
            asyncio.run(self._server.serve())

        self._thread = threading.Thread(target=_run, daemon=True, name="metrics-server")
        self._thread.start()
        time.sleep(0.5)  # give uvicorn a moment
        log.info("Metrics server listening on http://%s:%d/metrics", host, port)

    def stop(self):
        if self._server:
            self._server.should_exit = True

    # Convenience proxies
    def push_status(self, **kw):   self.store.push_status(**kw)
    def push_balance(self, *a, **kw): self.store.push_balance(*a, **kw)
    def push_positions(self, *a):  self.store.push_positions(*a)
    def push_candidates(self, *a, **kw): self.store.push_candidates(*a, **kw)
    def push_order(self, **kw):    self.store.push_order(**kw)
    def push_log(self, line: str): self.store.push_log(line)
