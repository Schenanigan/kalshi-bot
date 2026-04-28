"""
risk.py — Position sizing and exposure limits.

All OrderIntents pass through RiskManager.approve() before execution.
Returns None if approved, or a rejection reason string.
"""

import logging
import re
from dataclasses import dataclass
from typing import Optional

from strategy import OrderIntent

log = logging.getLogger(__name__)


@dataclass
class RiskConfig:
    max_trade_dollars: float = 25.0
    max_open_positions: int = 40
    max_daily_loss_dollars: float = 100.0
    allow_duplicate_tickers: bool = False
    # Correlation cap: max aggregate notional $ on KXHIGH-* OR KXLOW-*
    # active basket events (settling within ~24h). A continental cold front
    # can move all temp markets in the same direction at once; this caps
    # the systemic exposure even when no individual position is oversized.
    max_temp_directional_dollars: float = 30.0


class RiskManager:
    """
    Tracks open positions and enforces:
      - max_open_positions: max number of logical positions (arb baskets count as 1)
      - max_trade_dollars: max $ per single trade
      - max_daily_loss_dollars: stop trading if daily losses exceed this
      - duplicate detection: event-level for arb legs, ticker-level for others
    """

    def __init__(self, cfg: RiskConfig):
        self.cfg = cfg
        self._positions: dict[str, int] = {}    # ticker -> contract count
        self._position_notional: dict[str, float] = {}  # ticker -> $ at risk
        self._open_tickers: set[str] = set()    # tickers with non-zero positions
        self._open_events: set[str] = set()     # events with ≥1 open leg (cached)
        self._daily_loss: float = 0.0
        self._traded_tickers: set[str] = set()  # tickers traded this session
        self._traded_events: set[str] = set()   # arb events traded this session
        # Track which tickers belong to arb baskets → event
        self._arb_tickers: dict[str, str] = {}  # ticker -> event_ticker

    @staticmethod
    def _extract_event(ticker: str) -> str:
        """Extract event ticker from a market ticker.

        KXLOWTDAL-26APR14-T69 → KXLOWTDAL-26APR14
        """
        m = re.match(r"(.*?-\d+[A-Z]+\d+)", ticker)
        return m.group(1) if m else ticker

    def sync_positions(self, api_positions: list[dict]):
        """
        Sync state from the Kalshi portfolio endpoint.
        Call at startup and each loop.
        """
        self._positions.clear()
        self._position_notional.clear()
        self._open_tickers.clear()
        self._open_events.clear()

        for pos in api_positions:
            ticker = pos.get("ticker", "")
            count = pos.get("position", 0)
            if count != 0:
                self._positions[ticker] = count
                self._open_tickers.add(ticker)
                self._open_events.add(self._extract_event(ticker))
                # Notional $ at risk: count × entry_price (cents → dollars).
                # Falls back to 5¢ when entry_price unavailable so the cap
                # still has rough signal even before fills are reconciled.
                entry_cents = pos.get("avg_entry_price") or pos.get("entry_price") or 5
                self._position_notional[ticker] = (entry_cents / 100.0) * abs(count)

        log.info(
            "Risk sync: %d open positions (%d logical)",
            len(self._open_tickers),
            self._count_logical_positions(),
        )

    def _count_logical_positions(self) -> int:
        """Count positions where arb basket legs count as 1."""
        arb_events = set()
        non_arb = 0
        for ticker in self._open_tickers:
            if ticker in self._arb_tickers:
                arb_events.add(self._arb_tickers[ticker])
            else:
                non_arb += 1
        return len(arb_events) + non_arb

    def sync_daily_pnl(self, pnl: float):
        """Set daily loss from portfolio-level P&L (negative = loss)."""
        self._daily_loss = abs(pnl) if pnl < 0 else 0.0
        if self._daily_loss > self.cfg.max_daily_loss_dollars * 0.8:
            log.warning(
                "Daily loss $%.2f approaching limit $%.2f",
                self._daily_loss, self.cfg.max_daily_loss_dollars,
            )

    def approve(self, intent: OrderIntent, is_exit: bool = False) -> Optional[str]:
        """
        Check whether an order should be placed.
        Returns None if approved, or a rejection reason string.

        Arb basket legs use event-level dedup (not ticker-level) and
        count as a single logical position for the max-positions check.

        Exit orders bypass duplicate-ticker and max-position checks
        since they reduce exposure rather than increase it.
        """
        ticker = intent.ticker
        is_arb = "Partition arb" in (intent.reason or "")

        # Check daily loss limit
        if self._daily_loss >= self.cfg.max_daily_loss_dollars:
            return f"Daily loss ${self._daily_loss:.2f} >= limit ${self.cfg.max_daily_loss_dollars}"

        if not is_exit:
            if is_arb:
                # Arb legs: dedup at event level, not ticker level
                event = self._extract_event(ticker)
                if event in self._traded_events:
                    return f"Already traded arb basket {event} this session"
                # Block stacking: if we already hold ANY leg of this event
                # (either from a prior session that restarted, or a partial
                # basket still open), don't layer a second basket on top.
                # Uneven leg sizes from stacking break the arb guarantee.
                if event in self._open_events:
                    return f"Already hold open basket legs for {event}"
            else:
                # Non-arb: dedup at ticker level
                if not self.cfg.allow_duplicate_tickers and ticker in self._traded_tickers:
                    return f"Already traded {ticker} this session (duplicates disabled)"

            # Check max logical positions (arb baskets = 1 position)
            logical = self._count_logical_positions()
            if ticker not in self._open_tickers:
                # Would this add a new logical position?
                if is_arb:
                    event = self._extract_event(ticker)
                    # Only counts as new if no legs of this event are open yet
                    if event not in {self._arb_tickers.get(t) for t in self._open_tickers}:
                        logical += 1
                else:
                    logical += 1
                if logical > self.cfg.max_open_positions:
                    return f"Max logical positions ({self.cfg.max_open_positions}) already open"

        # Check per-trade cost
        trade_cost = (intent.limit_price / 100) * intent.count
        if trade_cost > self.cfg.max_trade_dollars:
            return f"Trade cost ${trade_cost:.2f} exceeds max ${self.cfg.max_trade_dollars}"

        # Correlation cap: cap aggregate $ exposure across same-direction
        # temp markets. A KXHIGH long across 6 cities all moves together
        # under one weather pattern — sum the notional and gate.
        if not is_exit and (ticker.startswith("KXHIGH") or ticker.startswith("KXLOW")):
            prefix = "KXHIGH" if ticker.startswith("KXHIGH") else "KXLOW"
            existing = sum(
                self._position_notional.get(t, 0.0)
                for t in self._open_tickers if t.startswith(prefix)
            )
            if existing + trade_cost > self.cfg.max_temp_directional_dollars:
                return (
                    f"Directional {prefix} exposure ${existing + trade_cost:.2f} "
                    f"would exceed cap ${self.cfg.max_temp_directional_dollars:.2f}"
                )

        # Approved — record for dedup
        self._traded_tickers.add(ticker)
        if is_arb:
            event = self._extract_event(ticker)
            self._arb_tickers[ticker] = event
        return None

    def mark_arb_event_traded(self, event_ticker: str):
        """Mark an arb event as fully traded (call after all legs are submitted)."""
        self._traded_events.add(event_ticker)

    def reset_daily(self):
        """Reset daily counters (call at start of each day)."""
        self._daily_loss = 0.0
        self._traded_tickers.clear()
        self._traded_events.clear()
