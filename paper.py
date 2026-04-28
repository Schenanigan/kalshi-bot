"""
paper.py — Paper trading engine for live-data backtesting.

Simulates order fills and portfolio tracking against real Kalshi
market data without placing any real orders.

Usage:
    python bot.py --paper
"""

import json
import logging
import math
import os
import random
from dataclasses import dataclass

import config

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
		self.total_fees = 0.0
		self.total_fills = 0
		self.total_submitted = 0
		# Track arb baskets: event_ticker → set of position tickers
		self.arb_baskets: dict[str, set[str]] = {}
		# Track consecutive scans a ticker has been missing (for non-arb expiry)
		self._missing_counts: dict[str, int] = {}

	def has_pending_exit(self, ticker: str, side: str) -> bool:
		"""True if a sell order for this (ticker, side) is already pending."""
		return any(
			o.ticker == ticker and o.side == side and o.action == "sell"
			for o in self.pending
		)

	def cancel_pending_exits(self, ticker: str, side: str) -> int:
		"""Remove any pending sell orders for this (ticker, side). Returns count."""
		kept = [o for o in self.pending
		        if not (o.ticker == ticker and o.side == side and o.action == "sell")]
		removed = len(self.pending) - len(kept)
		self.pending = kept
		return removed

	def writeoff_position(self, ticker: str, reason: str) -> bool:
		"""Book a position as a total loss immediately.

		Used when a stop-loss sell can't realistically fill (e.g. bid=0)
		so we don't carry zombie positions until market expiry.
		"""
		pos = self.positions.get(ticker)
		if pos is None:
			return False
		entry_cost = (pos.entry_price / 100) * pos.count
		self.realized_pnl -= entry_cost
		log.info(
			"[PAPER WRITEOFF] %s %s ×%d @ %dc → loss $%.2f (%s)",
			pos.side.upper(), ticker, pos.count, pos.entry_price, entry_cost, reason,
		)
		# Drop any lingering sell orders for this position
		self.cancel_pending_exits(ticker, pos.side)
		del self.positions[ticker]
		return True

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

		# Track arb basket membership from the reason string
		if "Partition arb" in intent.reason:
			# Extract event ticker from reason: "Partition arb KXLOWTATL-26APR14: ..."
			parts = intent.reason.split(":")
			if parts:
				event = parts[0].replace("Partition arb ", "").strip()
				self.arb_baskets.setdefault(event, set()).add(intent.ticker)

		log.info(
			"[PAPER] Submitted %s %s %s ×%d @ %dc",
			intent.action.upper(), intent.side.upper(),
			intent.ticker, intent.count, intent.limit_price,
		)
		return True

	def check_fills(self, candidates: list) -> list[SimOrder]:
		"""Check pending orders against current prices. Returns filled orders.

		A single loop iteration may only partially fill an order — if the
		displayed top-of-book depth on the crossed side is smaller than the
		order count, we fill what's displayed and leave the remainder
		resting. Previously paper always filled the full count instantly,
		which made arb baskets look guaranteed even when live fills would
		have been partial and the edge would have evaporated.
		"""
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
			fill_count, fill_price = self._fill_size_and_price(order, market)
			if fill_count <= 0:
				still_pending.append(order)
				continue

			if fill_count >= order.count:
				self._execute_fill(order, fill_count, fill_price)
				filled.append(order)
			else:
				# Partial fill: book what crossed, re-queue the remainder
				# at the original limit price.
				remainder = order.count - fill_count
				reserved_per = order.reserved / order.count if order.count else 0.0
				partial = SimOrder(
					ticker=order.ticker, side=order.side, action=order.action,
					count=fill_count, limit_price=order.limit_price,
					strategy=order.strategy, reason=order.reason,
					reserved=reserved_per * fill_count,
				)
				self._execute_fill(partial, fill_count, fill_price)
				filled.append(partial)
				order.count = remainder
				order.reserved = reserved_per * remainder
				log.info(
					"[PAPER PARTIAL] %s %s x%d filled, %d resting",
					order.ticker, order.side, fill_count, remainder,
				)
				still_pending.append(order)

		self.pending = still_pending
		return filled

	def _fill_size_and_price(self, order: SimOrder, market) -> tuple[int, int]:
		"""Return (fill_count, fill_price_cents).

		fill_count is min(order.count, displayed depth on crossed side), or
		0 if the limit doesn't cross. fill_price is the actual top-of-book
		ask/bid we'd hit — not our limit — so paper reflects realized cost
		rather than the worst-case buffer-padded limit we submitted at.
		"""
		if order.action == "buy":
			if order.side == "yes":
				ask_c = int(market.yes_ask * 100)
				depth = getattr(market, "yes_ask_size", 0) or 0
				if ask_c > order.limit_price:
					return 0, 0
				return min(order.count, int(depth)), ask_c
			else:
				no_ask_c = int((1 - market.yes_bid) * 100)
				depth = getattr(market, "yes_bid_size", 0) or 0
				if no_ask_c > order.limit_price:
					return 0, 0
				return min(order.count, int(depth)), no_ask_c
		else:
			if order.side == "yes":
				bid_c = int(market.yes_bid * 100)
				depth = getattr(market, "yes_bid_size", 0) or 0
				if bid_c < order.limit_price:
					return 0, 0
				return min(order.count, int(depth)), bid_c
			else:
				no_bid_c = int((1 - market.yes_ask) * 100)
				depth = getattr(market, "yes_ask_size", 0) or 0
				if no_bid_c < order.limit_price:
					return 0, 0
				return min(order.count, int(depth)), no_bid_c

	@staticmethod
	def _calc_fee(contracts: int, price_cents: int) -> float:
		"""Kalshi taker fee: ceil(rate × C × P × (1−P)), in dollars."""
		p = price_cents / 100
		raw = config.TAKER_FEE_RATE * contracts * p * (1 - p)
		return math.ceil(raw * 100) / 100

	def _execute_fill(self, order: SimOrder, fill_count: int, fill_price: int):
		"""Process a fill: update positions, balance, and fees.

		``fill_price`` is the actual top-of-book price crossed (not the
		limit). Any over-reserved balance — the difference between what we
		held aside at limit and what we actually paid — is released back.
		"""
		ticker = order.ticker
		self.total_fills += 1

		fee = self._calc_fee(fill_count, fill_price)
		self.total_fees += fee
		self.balance -= fee

		if order.action == "buy":
			actual_cost = (fill_price / 100) * fill_count
			# Release the cents we over-reserved (limit - actual_price) × count.
			refund = max(0.0, order.reserved - actual_cost)
			self.balance += refund

			if ticker in self.positions:
				pos = self.positions[ticker]
				total = pos.entry_price * pos.count + fill_price * fill_count
				pos.count += fill_count
				pos.entry_price = total // pos.count
			else:
				self.positions[ticker] = SimPosition(
					ticker=ticker, side=order.side,
					count=fill_count, entry_price=fill_price,
				)
			log.info(
				"[PAPER FILL] BUY %s %s ×%d @ %dc (limit %dc, fee $%.2f)",
				order.side.upper(), ticker, fill_count, fill_price,
				order.limit_price, fee,
			)
		else:
			if ticker in self.positions:
				pos = self.positions[ticker]
				pnl_cents = (fill_price - pos.entry_price) * fill_count
				self.realized_pnl += pnl_cents / 100
				self.balance += (fill_price / 100) * fill_count
				pos.count -= fill_count
				log.info(
					"[PAPER FILL] SELL %s %s ×%d @ %dc (P&L: %+.0fc, fee $%.2f)",
					order.side.upper(), ticker, fill_count,
					fill_price, pnl_cents, fee,
				)
				if pos.count <= 0:
					del self.positions[ticker]
			else:
				self.balance += (fill_price / 100) * fill_count
				log.warning("Paper: sell on %s with no position", ticker)

	# Number of consecutive scans a ticker must be missing before we resolve it.
	# This prevents false resolutions when the API just didn't return a market
	# in one scan cycle.
	MISSING_SCANS_TO_RESOLVE = 3

	def resolve_expired(self, candidates: list) -> list[dict]:
		"""Resolve positions whose markets are no longer in candidates (expired/settled).

		- Arb basket legs are resolved together: exactly one leg wins $1,
		  so net P&L = $1 × size - total basket cost (guaranteed profit).
		- Non-arb positions must be missing for MISSING_SCANS_TO_RESOLVE
		  consecutive scans before being resolved (prevents false triggers
		  from API hiccups).

		Returns a list of resolution records for logging.
		"""
		active_tickers = {c.ticker for c in candidates}
		resolutions = []

		# Update missing counts
		for ticker in list(self._missing_counts.keys()):
			if ticker in active_tickers:
				del self._missing_counts[ticker]
		for ticker in self.positions:
			if ticker not in active_tickers:
				self._missing_counts[ticker] = self._missing_counts.get(ticker, 0) + 1

		# Stale-leg detector: held legs whose basket has fewer remaining
		# legs in `arb_baskets` than the original event had on Kalshi.
		# These positions can't recover (their basket guarantee is broken)
		# and should be exited rather than left to expire at $0.
		# Heuristic: a basket entry with only 1-2 surviving legs is almost
		# certainly stale. Log periodically (not every loop).
		_now_ts = getattr(self, "_last_stale_log", 0.0)
		import time as _time
		if _time.time() - _now_ts > 1800:  # every 30 min
			self._last_stale_log = _time.time()
			for event, basket_tickers in self.arb_baskets.items():
				held = basket_tickers & set(self.positions.keys())
				if 0 < len(held) < len(basket_tickers) and len(held) <= 2:
					log.warning(
						"Stale-leg suspect %s: %d/%d basket legs still held — "
						"siblings already closed. Consider manual exit.",
						event, len(held), len(basket_tickers),
					)

		# --- Resolve complete arb baskets ---
		resolved_arb_tickers: set[str] = set()
		for event, basket_tickers in list(self.arb_baskets.items()):
			# Only resolve if ALL legs of the basket are missing
			held = basket_tickers & set(self.positions.keys())
			if not held:
				# Already resolved or never filled
				continue
			if any(t in active_tickers for t in held):
				# Some legs still active — not expired yet
				continue
			# All held legs are missing → event settled.
			# Exactly one leg of the underlying partition pays $1; rest $0.
			# We don't know which one. Simulate by sampling from the
			# market's implied probabilities (entry price ≈ implied prob).
			#
			#   prob(leg i wins) ∝ entry_price_i   (within held legs)
			#   residual prob (1 − Σp_i) represents the probability mass
			#   on UNHELD legs of the partition — if one of those wins,
			#   we hold nothing that pays, so payout = 0.
			#
			# This matters when a basket is incomplete (missing leg bug
			# from pre-fix state or future degenerate cases). After the
			# _build_basket partition-abort fix, new baskets are complete
			# and Σp_i will be close to 1, so the residual is tiny.
			held_list = sorted(held)  # deterministic order
			legs_info = [
				(t, self.positions[t].entry_price / 100, self.positions[t].count)
				for t in held_list if t in self.positions
			]
			basket_cost = sum(p * c for _, p, c in legs_info)
			sum_p = sum(p for _, p, _ in legs_info)

			# Seed only from the event ticker — resolutions are deterministic
			# across reloads and independent of resolution order within a loop.
			rng = random.Random(event)
			r = rng.random()
			cum = 0.0
			winner_ticker = None
			winner_count = 0
			for t, p, c in legs_info:
				cum += p
				if r < cum:
					winner_ticker = t
					winner_count = c
					break
			# If r >= sum_p, no held leg won (uncovered bucket hit)

			if winner_ticker is not None:
				payout = float(winner_count)  # $1 × count of winning leg
				outcome = "arb_settled"
				winner_desc = f"winner={winner_ticker} ×{winner_count}"
			else:
				payout = 0.0
				outcome = "arb_settled_uncovered"
				winner_desc = f"UNCOVERED bucket won (Σp={sum_p:.2f})"

			pnl = payout - basket_cost
			self.realized_pnl += pnl
			self.balance += payout

			log.info(
				"[PAPER RESOLVED] Arb basket %s: %d legs, cost $%.2f, payout $%.2f, P&L $%+.2f | %s",
				event, len(held_list), basket_cost, payout, pnl, winner_desc,
			)
			basket_size = winner_count if winner_count > 0 else (legs_info[0][2] if legs_info else 1)
			resolutions.append({
				"ticker": event,
				"side": "basket",
				"count": basket_size,
				"entry_price": int(basket_cost / max(basket_size, 1) * 100),
				"pnl": pnl,
				"outcome": outcome,
			})
			for t in held:
				resolved_arb_tickers.add(t)
				if t in self.positions:
					del self.positions[t]
				self._missing_counts.pop(t, None)
			del self.arb_baskets[event]

		# Collect all tickers that belong to any arb basket
		arb_tickers = set()
		for basket_set in self.arb_baskets.values():
			arb_tickers |= basket_set

		# --- Resolve non-arb positions after sustained absence ---
		to_remove = []
		for ticker, pos in self.positions.items():
			if ticker in resolved_arb_tickers:
				continue
			if ticker in arb_tickers:
				# This position is part of an arb basket — never resolve individually.
				# It will be resolved when the whole basket settles.
				continue
			if ticker in active_tickers:
				continue
			missing = self._missing_counts.get(ticker, 0)
			if missing < self.MISSING_SCANS_TO_RESOLVE:
				log.debug(
					"Paper: %s missing %d/%d scans, not resolving yet",
					ticker, missing, self.MISSING_SCANS_TO_RESOLVE,
				)
				continue

			# Truly expired — assume total loss (conservative)
			entry_cost = (pos.entry_price / 100) * pos.count
			self.realized_pnl -= entry_cost
			resolutions.append({
				"ticker": ticker,
				"side": pos.side,
				"count": pos.count,
				"entry_price": pos.entry_price,
				"pnl": -entry_cost,
				"outcome": "expired_loss",
			})
			to_remove.append(ticker)
			log.info(
				"[PAPER RESOLVED] %s %s ×%d @ %dc → assumed loss $%.2f (missing %d scans)",
				pos.side.upper(), ticker, pos.count, pos.entry_price, entry_cost, missing,
			)

		for ticker in to_remove:
			del self.positions[ticker]
			self._missing_counts.pop(ticker, None)

		if resolutions:
			# Append to settled-outcomes log for calibration analysis.
			# Daily-rotated; reconciled offline by scripts/calibration.py.
			out_dir = os.environ.get("KALSHI_OUTCOMES_DIR", ".")
			try:
				import datetime as _dt
				now = _dt.datetime.now(_dt.timezone.utc)
				ts = now.isoformat()
				day = now.strftime("%Y-%m-%d")
				path = os.path.join(out_dir, f"outcomes-{day}.jsonl")
				with open(path, "a") as f:
					for r in resolutions:
						f.write(json.dumps({**r, "ts": ts}) + "\n")
			except Exception as e:
				log.debug("outcomes log append failed: %s", e)

		return resolutions

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

	# ── Persistence ─────────────────────────────────────────────────────────
	# Pending orders are deliberately NOT persisted — they're transient
	# intents that will be re-emitted on the next scan if still valid.

	def save_state(self, path: str) -> None:
		state = {
			"balance": self.balance,
			"starting_balance": self.starting_balance,
			"realized_pnl": self.realized_pnl,
			"total_fees": self.total_fees,
			"total_fills": self.total_fills,
			"total_submitted": self.total_submitted,
			"positions": {
				t: {"ticker": p.ticker, "side": p.side,
				    "count": p.count, "entry_price": p.entry_price}
				for t, p in self.positions.items()
			},
			"arb_baskets": {k: sorted(v) for k, v in self.arb_baskets.items()},
			"missing_counts": dict(self._missing_counts),
		}
		tmp = path + ".tmp"
		try:
			with open(tmp, "w") as f:
				json.dump(state, f)
			os.replace(tmp, path)
		except OSError as e:
			log.warning("Paper state save failed: %s", e)

	def load_state(self, path: str) -> bool:
		if not os.path.exists(path):
			return False
		try:
			with open(path) as f:
				state = json.load(f)
		except (OSError, json.JSONDecodeError) as e:
			log.warning("Paper state load failed (%s) — starting fresh", e)
			return False

		self.balance = state["balance"]
		self.starting_balance = state.get("starting_balance", self.starting_balance)
		self.realized_pnl = state.get("realized_pnl", 0.0)
		self.total_fees = state.get("total_fees", 0.0)
		self.total_fills = state.get("total_fills", 0)
		self.total_submitted = state.get("total_submitted", 0)
		self.positions = {
			t: SimPosition(ticker=p["ticker"], side=p["side"],
			               count=p["count"], entry_price=p["entry_price"])
			for t, p in state.get("positions", {}).items()
		}
		self.arb_baskets = {k: set(v) for k, v in state.get("arb_baskets", {}).items()}
		self._missing_counts = dict(state.get("missing_counts", {}))
		log.info(
			"Paper state restored: $%.2f balance, %d positions, $%+.2f realized",
			self.balance, len(self.positions), self.realized_pnl,
		)
		return True
