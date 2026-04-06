from __future__ import annotations

import logging
import time

import requests

from .config import CoinGeckoConfig
from .storage import get_price_data, save_price_data

logger = logging.getLogger(__name__)

# Map AAVE reserve symbols to CoinGecko IDs
SYMBOL_TO_COINGECKO = {
    "WETH": "ethereum",
    "WBTC": "bitcoin",
    "USDC": "usd-coin",
    "USDT": "tether",
    "DAI": "dai",
    "LINK": "chainlink",
    "AAVE": "aave",
    "UNI": "uniswap",
    "MKR": "maker",
    "SNX": "havven",
    "CRV": "curve-dao-token",
    "BAL": "balancer",
    "COMP": "compound-governance-token",
    "YFI": "yearn-finance",
    "SUSHI": "sushi",
    "ENS": "ethereum-name-service",
    "LDO": "lido-dao",
    "RPL": "rocket-pool",
    "cbETH": "coinbase-wrapped-staked-eth",
    "rETH": "rocket-pool-eth",
    "wstETH": "wrapped-steth",
    "FRAX": "frax",
    "LUSD": "liquity-usd",
    "GHO": "gho",
    "ARB": "arbitrum",
    "OP": "optimism",
}


class PriceFetcher:
    def __init__(self, config: CoinGeckoConfig, conn):
        self.config = config
        self.conn = conn
        self.session = requests.Session()
        self._last_call_time: float = 0
        self._min_interval = 60.0 / config.rate_limit_per_minute

        if config.api_key:
            self.session.headers["x-cg-demo-api-key"] = config.api_key

    def _rate_limit(self):
        elapsed = time.time() - self._last_call_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call_time = time.time()

    def _get_coingecko_id(self, symbol: str) -> str | None:
        return SYMBOL_TO_COINGECKO.get(symbol)

    def fetch_price_range(self, symbol: str, start_ts: int, end_ts: int) -> list[tuple[int, float]]:
        """Fetch price data for a symbol in a time range. Returns [(timestamp, price_usd)]."""
        # Check cache first
        cached = get_price_data(self.conn, symbol, start_ts, end_ts)
        if cached:
            return cached

        cg_id = self._get_coingecko_id(symbol)
        if not cg_id:
            logger.warning(f"No CoinGecko mapping for {symbol}")
            return []

        self._rate_limit()

        try:
            url = f"{self.config.base_url}/coins/{cg_id}/market_chart/range"
            params = {
                "vs_currency": "usd",
                "from": start_ts,
                "to": end_ts,
            }
            resp = self.session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            prices = []
            for ts_ms, price in data.get("prices", []):
                ts = int(ts_ms / 1000)
                prices.append((ts, price))

            # Cache to DB
            if prices:
                save_price_data(self.conn, symbol, prices)

            logger.info(f"Fetched {len(prices)} price points for {symbol}")
            return prices

        except Exception as e:
            logger.error(f"CoinGecko fetch failed for {symbol}: {e}")
            return []

    def get_price_around_event(
        self, symbol: str, event_ts: int,
        hours_before: int = 2, hours_after: int = 6,
    ) -> list[tuple[int, float]]:
        """Get price data in a window around an event timestamp."""
        start = event_ts - (hours_before * 3600)
        end = event_ts + (hours_after * 3600)
        return self.fetch_price_range(symbol, start, end)
