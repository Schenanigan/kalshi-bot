"""
client.py — Authenticated Kalshi REST API wrapper.

Kalshi uses RSA-PSS key-pair authentication:
  - Sign: timestamp_ms + "\\n" + METHOD + "\\n" + path
  - Algo: RSA-PSS with SHA-256, salt_length=32
  - Headers: KALSHI-ACCESS-KEY, KALSHI-ACCESS-TIMESTAMP, KALSHI-ACCESS-SIGNATURE
"""

import os
import time
import base64
import logging
import uuid
from typing import Optional

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from config import DEFAULT_BASE_URL_DEMO, DEFAULT_BASE_URL_PROD

log = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_BACKOFF = 3.0  # seconds, multiplied each retry


class KalshiClient:
    """
    Thin wrapper around the Kalshi v2 REST API.

    Usage:
        client = KalshiClient("key-id", "key.pem", demo=True)
        markets = client.get_markets()
    """

    def __init__(self, api_key: str, key_path: str, demo: bool = True):
        self.base_url = DEFAULT_BASE_URL_DEMO if demo else DEFAULT_BASE_URL_PROD
        self.key_id = api_key
        self.session = requests.Session()
        self._private_key = self._load_private_key(key_path)

    # ── Auth ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _load_private_key(key_path: str):
        """Load RSA private key from KALSHI_KEY_B64 env var (base64-encoded PEM) or PEM file."""
        key_b64 = os.environ.get("KALSHI_KEY_B64", "")
        if key_b64:
            try:
                pem_bytes = base64.b64decode(key_b64)
                log.info("Loaded private key from KALSHI_KEY_B64 env var")
                return serialization.load_pem_private_key(pem_bytes, password=None)
            except Exception as e:
                log.error("Failed to decode KALSHI_KEY_B64: %s", e)
                return None
        try:
            with open(key_path, "rb") as f:
                return serialization.load_pem_private_key(f.read(), password=None)
        except FileNotFoundError:
            log.warning(
                "Private key file '%s' not found. "
                "Running in unauthenticated mode (read-only).",
                key_path,
            )
            return None

    def _sign(self, timestamp_ms: str, method: str, path: str) -> str:
        """
        Create RSA-PSS SHA-256 signature for the Kalshi auth header.

        Message format: "{timestamp_ms}{METHOD}{path}" (no separators).
        The path does NOT include query parameters.
        """
        msg = f"{timestamp_ms}{method.upper()}{path}".encode()
        sig = self._private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=hashes.SHA256.digest_size,  # 32
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode()

    def _headers(self, method: str, path: str) -> dict:
        """Return auth headers. Falls back to empty dict if no key loaded."""
        if self._private_key is None:
            return {"Content-Type": "application/json"}
        ts = str(int(time.time() * 1000))
        return {
            "KALSHI-ACCESS-KEY":       self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts, method, path),
            "Content-Type":            "application/json",
        }

    # ── HTTP Helpers (with retry) ─────────────────────────────────────────────

    def _request(self, method: str, path: str, params: dict = None, json_body: dict = None) -> dict:
        url = self.base_url + path
        # Sign the full URL path (including /trade-api/v2 prefix), not just the relative path
        from urllib.parse import urlparse
        full_path = urlparse(url).path
        headers = self._headers(method, full_path)

        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.request(
                    method, url, headers=headers, params=params, json=json_body,
                    timeout=15,
                )
                if resp.status_code == 429:
                    if attempt >= MAX_RETRIES - 1:
                        resp.raise_for_status()
                    wait = RETRY_BACKOFF * (2 ** attempt)
                    log.warning("Rate limited (429). Retrying in %.1fs...", wait)
                    time.sleep(wait)
                    headers = self._headers(method, full_path)  # re-sign with fresh timestamp
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF * (2 ** attempt)
                    log.warning("Request failed (%s). Retrying in %.1fs...", e, wait)
                    time.sleep(wait)
                    headers = self._headers(method, full_path)
                else:
                    raise

    def _get(self, path: str, params: dict = None) -> dict:
        return self._request("GET", path, params=params)

    def _post(self, path: str, body: dict = None) -> dict:
        return self._request("POST", path, json_body=body)

    def _delete(self, path: str) -> dict:
        return self._request("DELETE", path)

    # ── Market Endpoints ──────────────────────────────────────────────────────

    def get_markets(
        self,
        status: str = "open",
        series: Optional[str] = None,
        limit: int = 200,
        cursor: Optional[str] = None,
        paginate: bool = True,
        min_close_ts: Optional[int] = None,
        max_close_ts: Optional[int] = None,
    ) -> list[dict]:
        """
        Fetch matching markets. Paginates through all pages by default.
        Set paginate=False to fetch only the first page.

        min_close_ts / max_close_ts: UNIX timestamps to filter by close time.
        """
        all_markets = []
        while True:
            params = {"status": status, "limit": limit}
            if series:
                params["series_ticker"] = series
            if cursor:
                params["cursor"] = cursor
            if min_close_ts:
                params["min_close_ts"] = min_close_ts
            if max_close_ts:
                params["max_close_ts"] = max_close_ts
            resp = self._get("/markets", params=params)
            all_markets.extend(resp.get("markets", []))
            if not paginate:
                break
            cursor = resp.get("cursor")
            if not cursor:
                break
            time.sleep(0.3)  # pace pagination to avoid rate limits
        log.debug("Fetched %d markets (status=%s)", len(all_markets), status)
        return all_markets

    def get_market(self, ticker: str) -> dict:
        """Get a single market by its ticker."""
        return self._get(f"/markets/{ticker}")

    def get_events(self, series_ticker: str, status: str = "open", limit: int = 20) -> list[dict]:
        """Fetch events for a series."""
        resp = self._get("/events", params={
            "series_ticker": series_ticker, "status": status, "limit": limit,
        })
        return resp.get("events", [])

    def get_markets_for_event(self, event_ticker: str, limit: int = 100) -> list[dict]:
        """Fetch ALL markets for an event — guarantees complete partitions."""
        resp = self._get("/markets", params={
            "event_ticker": event_ticker, "limit": limit,
        })
        return resp.get("markets", [])

    def get_orderbook(self, ticker: str, depth: int = 5) -> dict:
        """Get the order book for a market."""
        return self._get(f"/markets/{ticker}/orderbook", params={"depth": depth})

    # ── Portfolio Endpoints ───────────────────────────────────────────────────

    def get_balance(self) -> dict:
        """Get account balance (requires auth). Returns dict with balance fields."""
        return self._get("/portfolio/balance")

    def get_positions(self) -> list[dict]:
        """Get all open positions.

        Pages through the cursor without sleeping between requests — the
        request wrapper already backs off on 429s, so a fixed inter-page
        sleep was just adding latency (40s+ at 140+ positions).
        """
        all_positions = []
        cursor = None
        while True:
            params = {}
            if cursor:
                params["cursor"] = cursor
            resp = self._get("/portfolio/positions", params=params)
            all_positions.extend(resp.get("market_positions", []))
            cursor = resp.get("cursor")
            if not cursor:
                break
        # Filter to non-zero positions client-side
        return [p for p in all_positions if p.get("position", 0) != 0]

    def get_orders(self, status: str = "resting") -> list[dict]:
        """Get orders. status: resting | filled | canceled"""
        resp = self._get("/portfolio/orders", params={"status": status})
        return resp.get("orders", [])

    # ── Order Endpoints ───────────────────────────────────────────────────────

    def place_order(
        self,
        ticker: str,
        side: str,            # "yes" or "no"
        count: int,
        limit_price: int,     # cents (1–99)
        action: str = "buy",
        client_order_id: str | None = None,
    ) -> dict:
        """
        Place a limit order.

        limit_price is in cents (e.g. 45 = 45c = $0.45 per contract).
        Side is "yes" or "no". We set yes_price or no_price accordingly.

        A `client_order_id` (auto-generated if not supplied) is sent with
        every request. Kalshi uses this to de-duplicate: if the first
        attempt made it server-side but the response was lost to a network
        blip, a retry with the same client_order_id returns the original
        order instead of creating a second one.
        """
        if client_order_id is None:
            client_order_id = str(uuid.uuid4())
        order = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "type": "limit",
            "count": count,
            "client_order_id": client_order_id,
        }
        if side == "yes":
            order["yes_price"] = limit_price
        else:
            order["no_price"] = limit_price

        log.info(
            "Placing order: %s %s %s x%d @ %d¢ (client_id=%s)",
            action, side, ticker, count, limit_price, client_order_id[:8],
        )
        return self._post("/portfolio/orders", order)

    def cancel_order(self, order_id: str) -> dict:
        """Cancel a resting order by ID."""
        return self._delete(f"/portfolio/orders/{order_id}")

    def cancel_all_orders(self) -> int:
        """Cancel all resting orders. Returns count of orders cancelled."""
        orders = self.get_orders(status="resting")
        cancelled = 0
        for order in orders:
            oid = order.get("order_id", "")
            if oid:
                try:
                    self.cancel_order(oid)
                    cancelled += 1
                except Exception as e:
                    log.warning("Failed to cancel order %s: %s", oid, e)
        return cancelled
