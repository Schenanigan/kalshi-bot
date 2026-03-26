"""
risk.py — Position sizing and exposure limits.

All OrderIntents pass through RiskManager.approve() before execution.
Returns None if approved, or a rejection reason string.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from strategy import OrderIntent

log = logging.getLogger(__name__)


@dataclass
class RiskConfig:
    max_trade_dollars: float = 25.0
    max_open_positions: int = 10
    max_daily_loss_dollars: float = 100.0
    allow_duplicate_tickers: bool = False


class RiskManager:
    """
    Tracks open positions and enforces:
      - max_open_positions: max number of markets with open positions
      - max_trade_dollars: max $ per single trade
      - max_daily_loss_dollars: stop trading if daily losses exceed this
      - allow_duplicate_tickers: whether to place multiple orders on same ticker
    """

    def __init__(self, cfg: RiskConfig):
        self.cfg = cfg
        self._positions: dict[str, int] = {}    # ticker -> contract count
        self._open_tickers: set[str] = set()    # tickers with non-zero positions
        self._daily_loss: float = 0.0
        self._traded_tickers: set[str] = set()  # tickers traded this session

    def sync_positions(self, api_positions: list[dict]):
        """
        Sync state from the Kalshi portfolio endpoint.
        Call at startup and each loop.
        """
        self._positions.clear()
        self._open_tickers.clear()

        for pos in api_positions:
            ticker = pos.get("ticker", "")
            count = pos.get("position", 0)
            if count != 0:
                self._positions[ticker] = count
                self._open_tickers.add(ticker)

        log.info(
            "Risk sync: %d open positions",
            len(self._open_tickers),
        )

    def approve(self, intent: OrderIntent, is_exit: bool = False) -> Optional[str]:
        """
        Check whether an order should be placed.
        Returns None if approved, or a rejection reason string.

        Exit orders bypass duplicate-ticker and max-position checks
        since they reduce exposure rather than increase it.
        """
        ticker = intent.ticker

        # Check daily loss limit
        if self._daily_loss >= self.cfg.max_daily_loss_dollars:
            return f"Daily loss ${self._daily_loss:.2f} >= limit ${self.cfg.max_daily_loss_dollars}"

        if not is_exit:
            # Check duplicate ticker (only for new entries)
            if not self.cfg.allow_duplicate_tickers and ticker in self._traded_tickers:
                return f"Already traded {ticker} this session (duplicates disabled)"

            # Check max open positions (only for new entries)
            if ticker not in self._open_tickers and len(self._open_tickers) >= self.cfg.max_open_positions:
                return f"Max positions ({self.cfg.max_open_positions}) already open"

        # Check per-trade cost
        trade_cost = (intent.limit_price / 100) * intent.count
        if trade_cost > self.cfg.max_trade_dollars:
            return f"Trade cost ${trade_cost:.2f} exceeds max ${self.cfg.max_trade_dollars}"

        # Approved
        self._traded_tickers.add(ticker)
        return None

    def record_loss(self, amount: float):
        """Record a realized loss to track daily P&L."""
        self._daily_loss += amount

    def reset_daily(self):
        """Reset daily counters (call at start of each day)."""
        self._daily_loss = 0.0
        self._traded_tickers.clear()
