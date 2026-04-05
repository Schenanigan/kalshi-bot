"""
paper.py — Paper trading engine for live-data backtesting.

Simulates order fills and portfolio tracking against real Kalshi
market data without placing any real orders.

Usage:
    python bot.py --paper
"""

import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class SimOrder:
	"""A simulated limit order waiting for fill."""
	ticker: str
	side: str        # "yes" or "no"
	action: str      # "buy" or "sell"
	count: int
	limit_price: int # cents (1-99)
	strategy: str
	reason: str
	reserved: float  # dollars reserved from balance (buy orders only)


@dataclass
class SimPosition:
	"""A filled paper position."""
	ticker: str
	side: str        # "yes" or "no"
	count: int
	entry_price: int # average entry price in cents


class PaperTrader:
	"""Simulates order fills and tracks paper P&L against live market data.

	Submit orders via submit_order(), then call check_fills() each loop
	with current market candidates to simulate realistic fill behavior.
	"""

	def __init__(self, starting_balance: float = 500.0):
		self.balance = starting_balance
		self.starting_balance = starting_balance
		self.pending: list[SimOrder] = []
		self.positions: dict[str, SimPosition] = {}
		self.realized_pnl = 0.0
		self.total_fills = 0
		self.total_submitted = 0

	def submit_order(self, intent, strategy: str) -> bool:
		"""Submit a simulated order. Reserves cost from balance for buys."""
		cost = 0.0
		if intent.action == "buy":
			cost = (intent.limit_price / 100) * intent.count
			if cost > self.balance:
				log.warning(
					"Paper: insufficient balance $%.2f for %s (cost $%.2f)",
					self.balance, intent.ticker, cost,
				)
				return False
			self.balance -= cost

		self.pending.append(SimOrder(
			ticker=intent.ticker,
			side=intent.side,
			action=intent.action,
			count=intent.count,
			limit_price=intent.limit_price,
			strategy=strategy,
			reason=intent.reason,
			reserved=cost,
		))
		self.total_submitted += 1
		log.info(
			"[PAPER] Submitted %s %s %s ×%d @ %dc",
			intent.action.upper(), intent.side.upper(),
			intent.ticker, intent.count, intent.limit_price,
		)
		return True

	def check_fills(self, candidates: list) -> list[SimOrder]:
		"""Check pending orders against current prices. Returns filled orders."""
		prices = {c.ticker: c for c in candidates}
		filled = []
		still_pending = []

		for order in self.pending:
			if order.ticker not in prices:
				if order.action == "buy":
					self.balance += order.reserved
				log.info(
					"Paper: cancelled %s %s on %s (market expired)",
					order.action, order.side, order.ticker,
				)
				continue

			market = prices[order.ticker]
			if self._would_fill(order, market):
				self._execute_fill(order)
				filled.append(order)
			else:
				still_pending.append(order)

		self.pending = still_pending
		return filled

	def _would_fill(self, order: SimOrder, market) -> bool:
		"""Check if the current market price crosses the order limit."""
		if order.action == "buy":
			if order.side == "yes":
				return int(market.yes_ask * 100) <= order.limit_price
			else:
				return int((1 - market.yes_bid) * 100) <= order.limit_price
		else:
			if order.side == "yes":
				return int(market.yes_bid * 100) >= order.limit_price
			else:
				return int((1 - market.yes_ask) * 100) >= order.limit_price

	def _execute_fill(self, order: SimOrder):
		"""Process a fill: update positions and balance."""
		ticker = order.ticker
		self.total_fills += 1

		if order.action == "buy":
			if ticker in self.positions:
				pos = self.positions[ticker]
				total = pos.entry_price * pos.count + order.limit_price * order.count
				pos.count += order.count
				pos.entry_price = total // pos.count
			else:
				self.positions[ticker] = SimPosition(
					ticker=ticker, side=order.side,
					count=order.count, entry_price=order.limit_price,
				)
			log.info(
				"[PAPER FILL] BUY %s %s ×%d @ %dc",
				order.side.upper(), ticker, order.count, order.limit_price,
			)
		else:
			if ticker in self.positions:
				pos = self.positions[ticker]
				pnl_cents = (order.limit_price - pos.entry_price) * order.count
				self.realized_pnl += pnl_cents / 100
				self.balance += (order.limit_price / 100) * order.count
				pos.count -= order.count
				log.info(
					"[PAPER FILL] SELL %s %s ×%d @ %dc (P&L: %+.0fc)",
					order.side.upper(), ticker, order.count,
					order.limit_price, pnl_cents,
				)
				if pos.count <= 0:
					del self.positions[ticker]
			else:
				self.balance += (order.limit_price / 100) * order.count
				log.warning("Paper: sell on %s with no position", ticker)

	def mark_to_market(self, candidates: list) -> float:
		"""Compute unrealized P&L across all positions using current mids."""
		prices = {c.ticker: c for c in candidates}
		unrealized = 0.0

		for ticker, pos in self.positions.items():
			if ticker not in prices:
				continue
			market = prices[ticker]
			if pos.side == "yes":
				mark_cents = int(market.mid * 100)
			else:
				mark_cents = int((1 - market.mid) * 100)
			unrealized += ((mark_cents - pos.entry_price) / 100) * pos.count

		return unrealized

	def get_portfolio_value(self, candidates: list) -> float:
		"""Total value: cash + position cost + unrealized P&L."""
		position_cost = sum(
			(p.entry_price / 100) * p.count
			for p in self.positions.values()
		)
		return self.balance + position_cost + self.mark_to_market(candidates)

	def get_positions_as_dicts(self) -> list[dict]:
		"""Return positions in the same format as the Kalshi API."""
		return [
			{
				"ticker": pos.ticker,
				"position": pos.count if pos.side == "yes" else -pos.count,
				"average_price": pos.entry_price,
				"side": pos.side,
			}
			for pos in self.positions.values()
		]
