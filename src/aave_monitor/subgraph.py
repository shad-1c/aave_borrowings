from __future__ import annotations

import logging
import time

import requests
from requests.adapters import HTTPAdapter, Retry

from .config import SubgraphConfig
from .models import BorrowEvent, ReserveSnapshot

logger = logging.getLogger(__name__)


BORROWS_QUERY = """
query RecentBorrows($lastTimestamp: Int!, $first: Int!) {
  borrows(
    where: { timestamp_gt: $lastTimestamp }
    orderBy: timestamp
    orderDirection: asc
    first: $first
  ) {
    id
    txHash
    amount
    assetPriceUSD
    timestamp
    borrowRate
    borrowRateMode
    reserve {
      symbol
      name
      decimals
      underlyingAsset
    }
    user {
      id
    }
  }
}
"""

RESERVES_QUERY = """
query ReserveState {
  reserves(where: { isActive: true, borrowingEnabled: true }) {
    symbol
    underlyingAsset
    decimals
    availableLiquidity
    totalCurrentVariableDebt
    totalATokenSupply
  }
}
"""

# CoinGecko symbol mapping
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
    "ENS": "ethereum-name-service",
    "LDO": "lido-dao",
    "RPL": "rocket-pool",
    "cbETH": "coinbase-wrapped-staked-eth",
    "rETH": "rocket-pool-eth",
    "wstETH": "wrapped-steth",
    "FRAX": "frax",
    "LUSD": "liquity-usd",
    "GHO": "gho",
    "weETH": "wrapped-eeth",
    "osETH": "stakewise-staked-eth",
    "PYUSD": "paypal-usd",
    "sDAI": "savings-dai",
    "KNC": "kyber-network-crystal",
    "1INCH": "1inch",
    "FXS": "frax-share",
    "STG": "stargate-finance",
}

RAY = 10**27


class SubgraphClient:
    def __init__(self, config: SubgraphConfig, coingecko_api_key: str = ""):
        self.config = config
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        self._coingecko_api_key = coingecko_api_key
        self._prices_usd: dict[str, float] = {}
        self._prices_fetched_at: float = 0

    def _query(self, query: str, variables: dict | None = None) -> dict:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        resp = self.session.post(
            self.config.endpoint,
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json()

        if "errors" in result:
            raise RuntimeError(f"Subgraph query error: {result['errors']}")

        return result.get("data", {})

    def _refresh_prices(self):
        """Fetch current USD prices from CoinGecko. Cache for 60 seconds."""
        now = time.time()
        if self._prices_usd and (now - self._prices_fetched_at) < 60:
            return

        cg_ids = list(SYMBOL_TO_COINGECKO.values())
        ids_str = ",".join(cg_ids)

        try:
            headers = {}
            if self._coingecko_api_key:
                headers["x-cg-demo-api-key"] = self._coingecko_api_key

            resp = self.session.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": ids_str, "vs_currencies": "usd"},
                headers=headers,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            # Build reverse mapping: coingecko_id -> symbol
            id_to_symbol = {v: k for k, v in SYMBOL_TO_COINGECKO.items()}

            for cg_id, prices in data.items():
                symbol = id_to_symbol.get(cg_id)
                if symbol and "usd" in prices:
                    self._prices_usd[symbol] = prices["usd"]

            # Stablecoins fallback
            for stable in ["USDC", "USDT", "DAI", "FRAX", "LUSD", "GHO", "PYUSD", "sDAI"]:
                if stable not in self._prices_usd:
                    self._prices_usd[stable] = 1.0

            self._prices_fetched_at = now
            logger.info(f"Refreshed prices for {len(self._prices_usd)} assets from CoinGecko")

        except Exception as e:
            logger.error(f"Failed to fetch CoinGecko prices: {e}")
            # Use stablecoin defaults at minimum
            for stable in ["USDC", "USDT", "DAI", "FRAX", "LUSD", "GHO", "PYUSD"]:
                self._prices_usd.setdefault(stable, 1.0)

    def _get_price_usd(self, symbol: str) -> float:
        self._refresh_prices()
        return self._prices_usd.get(symbol, 0.0)

    def fetch_recent_borrows(self, since_timestamp: int = 0) -> list[BorrowEvent]:
        """Fetch all borrow events since the given timestamp, handling pagination."""
        all_borrows: list[BorrowEvent] = []
        current_ts = since_timestamp

        while True:
            data = self._query(BORROWS_QUERY, {
                "lastTimestamp": current_ts,
                "first": self.config.page_size,
            })

            borrows = data.get("borrows", [])
            if not borrows:
                break

            for b in borrows:
                reserve = b["reserve"]
                decimals = int(reserve["decimals"])
                amount_raw = b["amount"]
                amount_human = int(amount_raw) / (10 ** decimals)

                # Use assetPriceUSD from subgraph if available, otherwise CoinGecko
                asset_price_usd = float(b.get("assetPriceUSD", "0"))
                if asset_price_usd == 0:
                    asset_price_usd = self._get_price_usd(reserve["symbol"])
                amount_usd = amount_human * asset_price_usd

                rate_mode_int = int(b.get("borrowRateMode", 2))
                rate_mode = "stable" if rate_mode_int == 1 else "variable"

                borrow_rate_raw = int(b.get("borrowRate", "0"))
                borrow_rate_pct = borrow_rate_raw / RAY * 100

                event = BorrowEvent(
                    id=b["id"],
                    tx_hash=b.get("txHash", ""),
                    asset_symbol=reserve["symbol"],
                    asset_address=reserve["underlyingAsset"],
                    amount_raw=amount_raw,
                    amount_human=amount_human,
                    amount_usd=amount_usd,
                    borrower=b.get("user", {}).get("id", ""),
                    interest_rate_mode=rate_mode,
                    borrow_rate=borrow_rate_pct,
                    timestamp=int(b["timestamp"]),
                    block_number=0,
                )
                all_borrows.append(event)

            logger.info(f"  ... fetched page: {len(borrows)} borrows, total so far: {len(all_borrows)}")

            if len(borrows) < self.config.page_size:
                break

            current_ts = int(borrows[-1]["timestamp"])

        logger.info(f"Fetched {len(all_borrows)} borrow events since ts={since_timestamp}")
        return all_borrows

    def fetch_reserve_state(self) -> list[ReserveSnapshot]:
        """Fetch current state of all active reserves."""
        data = self._query(RESERVES_QUERY)
        reserves = data.get("reserves", [])
        now_ts = int(time.time())

        snapshots: list[ReserveSnapshot] = []
        for r in reserves:
            decimals = int(r["decimals"])
            symbol = r["symbol"]
            price_usd = self._get_price_usd(symbol)

            available_liq_raw = int(r.get("availableLiquidity", "0"))
            available_liq = available_liq_raw / (10 ** decimals)
            available_liq_usd = available_liq * price_usd

            total_var_debt_raw = int(r.get("totalCurrentVariableDebt", "0"))
            total_var_debt = total_var_debt_raw / (10 ** decimals)

            snapshots.append(ReserveSnapshot(
                asset_symbol=symbol,
                asset_address=r["underlyingAsset"],
                decimals=decimals,
                available_liquidity=available_liq,
                available_liquidity_usd=available_liq_usd,
                total_variable_debt=total_var_debt,
                price_usd=price_usd,
                snapshot_timestamp=now_ts,
            ))

        logger.info(f"Fetched state for {len(snapshots)} reserves")
        return snapshots
