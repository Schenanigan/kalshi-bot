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
MAX_PNL_HISTORY = 500
MAX_FILL_HISTORY = 200

# Edge-bucket boundaries (cents) for the per-edge realized dashboard.
# Anything below the partition-arb MIN_EDGE (8c today) lands in "<8c" — that
# bucket should stay empty in normal operation; if it fills it indicates
# either a config regression or a cert-winner being mistakenly tagged.
EDGE_BUCKETS: list[tuple[float, float, str]] = [
    (0.0, 8.0, "<8c"),
    (8.0, 10.0, "8-10c"),
    (10.0, 15.0, "10-15c"),
    (15.0, 25.0, "15-25c"),
    (25.0, 50.0, "25-50c"),
    (50.0, 1e9, "50c+"),
]
EDGE_BUCKET_LABELS = [label for _, _, label in EDGE_BUCKETS]


def _edge_bucket_for(edge_cents: float) -> str:
    for lo, hi, label in EDGE_BUCKETS:
        if lo <= edge_cents < hi:
            return label
    return EDGE_BUCKETS[-1][2]


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

@dataclass
class OrderLifecycle:
    resting: int           = 0
    placed_session: int    = 0
    filled_session: int    = 0
    cancelled_stale: int   = 0


@dataclass
class PnlSnapshot:
    ts: str
    balance: float
    portfolio: float
    daily_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    open_positions: int
    pending_orders: int


@dataclass
class FillRecord:
    ts: str
    ticker: str
    side: str
    action: str
    count: int
    price_cents: int
    strategy: str
    reason: str


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
        self.order_lifecycle = OrderLifecycle()
        self.log_lines: deque[str]           = deque(maxlen=MAX_LOG_LINES)
        # Paper-trading specifics
        self.pnl_history: deque[PnlSnapshot]  = deque(maxlen=MAX_PNL_HISTORY)
        self.fill_history: deque[FillRecord]   = deque(maxlen=MAX_FILL_HISTORY)
        self.paper_stats: dict = {
            "starting_balance": 0.0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "total_submitted": 0,
            "total_fills": 0,
            "total_fees": 0.0,
            "pending_orders": 0,
        }
        # Per-strategy counters so we can tell which strategies are earning
        # vs. bleeding. Keys: strategy_name → {submitted, filled, blocked,
        # notional_dollars, last_edge}. Written by push_order / push_fill.
        self.strategy_stats: dict = {}
        # Partition-arb basket-level stats, tracked separately because a
        # single basket decomposes into N order records in `orders`.
        self.basket_stats: dict = {
            "attempted": 0,       # baskets where _find_partition_arb fired
            "submitted": 0,       # baskets where every leg reached place_order
            "edge_sum_cents": 0.0,  # running total of basket edges (for avg)
            "last_edge_cents": 0.0,
        }
        # Certain-winner strategy stats (single-leg monotonic arbs).
        self.cert_winner_stats: dict = {
            "attempted": 0,
            "submitted": 0,
            "edge_sum_cents": 0.0,
        }
        # Per-edge realized P&L: for each edge bucket (at fire time),
        # how many baskets settled, how many won (pnl > 0), and the
        # cumulative realized P&L. Tells us whether the MIN_EDGE gate
        # is actually +EV at low edges, or if thin-edge baskets are
        # losing on uncovered-bucket adverse selection.
        self.edge_realized: dict[str, dict] = {
            label: {"baskets": 0, "wins": 0, "pnl_dollars": 0.0}
            for label in EDGE_BUCKET_LABELS
        }

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "status":      asdict(self.status),
                "balance":     asdict(self.balance),
                "positions":   [asdict(p) for p in self.positions],
                "orders":      [asdict(o) for o in self.orders],
                "candidates":  [asdict(c) for c in self.candidates],
                "scan_stats":  asdict(self.scan_stats),
                "order_lifecycle": asdict(self.order_lifecycle),
                "log_lines":   list(self.log_lines),
                "pnl_history": [asdict(s) for s in self.pnl_history],
                "fill_history": [asdict(f) for f in self.fill_history],
                "paper_stats": dict(self.paper_stats),
                "strategy_stats": dict(self.strategy_stats),
                "basket_stats": dict(self.basket_stats),
                "cert_winner_stats": dict(self.cert_winner_stats),
                "edge_realized": {k: dict(v) for k, v in self.edge_realized.items()},
            }

    def _now(self) -> str:
        return datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S UTC")

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
                    avg_price=float(p.get("average_price") or p.get("fees_paid_dollars") or 0),
                    current_bid=float(p.get("current_bid") or p.get("yes_bid_dollars") or 0),
                    current_ask=float(p.get("current_ask") or p.get("yes_ask_dollars") or 0),
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
            if status == "placed":
                self.order_lifecycle.placed_session += 1
            # Per-strategy accounting: every submitted order counts, and
            # blocked ones are tallied separately so we can see rejection
            # rates per strategy.
            s = self.strategy_stats.setdefault(strategy, {
                "submitted": 0, "blocked": 0, "filled": 0,
                "notional_dollars": 0.0, "last_ts": "",
            })
            if status in ("placed", "paper", "dry_run"):
                s["submitted"] += 1
                s["notional_dollars"] = round(
                    s["notional_dollars"] + count * price_cents / 100, 2,
                )
                s["last_ts"] = self._now()
            elif status == "blocked":
                s["blocked"] += 1

    def push_order_lifecycle(self, resting: int = 0,
                             new_fills: int = 0, new_cancels: int = 0):
        with self._lock:
            self.order_lifecycle.resting = resting
            self.order_lifecycle.filled_session += new_fills
            self.order_lifecycle.cancelled_stale += new_cancels

    def push_log(self, line: str):
        with self._lock:
            self.log_lines.appendleft(
                f"[{self._now()}] {line[:120]}"
            )

    def push_pnl_snapshot(self, balance: float, portfolio: float,
                          daily_pnl: float, realized_pnl: float,
                          unrealized_pnl: float, open_positions: int,
                          pending_orders: int):
        with self._lock:
            self.pnl_history.append(PnlSnapshot(
                ts=self._now(),
                balance=round(balance, 2),
                portfolio=round(portfolio, 2),
                daily_pnl=round(daily_pnl, 2),
                realized_pnl=round(realized_pnl, 2),
                unrealized_pnl=round(unrealized_pnl, 2),
                open_positions=open_positions,
                pending_orders=pending_orders,
            ))

    def push_fill(self, ticker: str, side: str, action: str,
                  count: int, price_cents: int, strategy: str, reason: str):
        with self._lock:
            self.fill_history.appendleft(FillRecord(
                ts=self._now(), ticker=ticker, side=side, action=action,
                count=count, price_cents=price_cents,
                strategy=strategy, reason=reason[:80],
            ))
            self.paper_stats["total_fills"] += 1
            s = self.strategy_stats.setdefault(strategy, {
                "submitted": 0, "blocked": 0, "filled": 0,
                "notional_dollars": 0.0, "last_ts": "",
            })
            s["filled"] += 1

    def push_basket(self, edge_cents: float, submitted: bool):
        """Track one partition-arb basket build. Called once per basket,
        not per leg. `submitted` is True iff every leg reached place_order
        (partial baskets are aborted before reaching this)."""
        with self._lock:
            self.basket_stats["attempted"] += 1
            if submitted:
                self.basket_stats["submitted"] += 1
                self.basket_stats["edge_sum_cents"] = round(
                    self.basket_stats["edge_sum_cents"] + edge_cents, 2,
                )
                self.basket_stats["last_edge_cents"] = round(edge_cents, 2)

    def push_basket_outcome(self, edge_cents: float, pnl: float):
        """Record a settled basket's realized P&L bucketed by entry edge.
        Bucket boundaries live in EDGE_BUCKETS so the dashboard can plot
        win-rate and realized $/basket per bucket."""
        with self._lock:
            label = _edge_bucket_for(edge_cents)
            stats = self.edge_realized[label]
            stats["baskets"] += 1
            if pnl > 0:
                stats["wins"] += 1
            stats["pnl_dollars"] = round(stats["pnl_dollars"] + pnl, 2)

    def push_cert_winner(self, edge_cents: float, submitted: bool):
        """Track one certain-winner single-leg attempt."""
        with self._lock:
            self.cert_winner_stats["attempted"] += 1
            if submitted:
                self.cert_winner_stats["submitted"] += 1
                self.cert_winner_stats["edge_sum_cents"] = round(
                    self.cert_winner_stats["edge_sum_cents"] + edge_cents, 2,
                )

    def push_paper_stats(self, starting_balance: float, realized_pnl: float,
                         unrealized_pnl: float, total_submitted: int,
                         total_fills: int, total_fees: float = 0.0,
                         pending_orders: int = 0):
        with self._lock:
            self.paper_stats.update({
                "starting_balance": round(starting_balance, 2),
                "realized_pnl": round(realized_pnl, 2),
                "unrealized_pnl": round(unrealized_pnl, 2),
                "total_submitted": total_submitted,
                "total_fills": total_fills,
                "total_fees": round(total_fees, 2),
                "pending_orders": pending_orders,
            })


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
    def push_order_lifecycle(self, **kw): self.store.push_order_lifecycle(**kw)
    def push_log(self, line: str): self.store.push_log(line)
    def push_pnl_snapshot(self, **kw): self.store.push_pnl_snapshot(**kw)
    def push_fill(self, **kw):     self.store.push_fill(**kw)
    def push_paper_stats(self, **kw): self.store.push_paper_stats(**kw)
    def push_basket(self, **kw):   self.store.push_basket(**kw)
    def push_basket_outcome(self, **kw): self.store.push_basket_outcome(**kw)
    def push_cert_winner(self, **kw): self.store.push_cert_winner(**kw)
