from dataclasses import dataclass


@dataclass
class BorrowEvent:
    id: str
    tx_hash: str
    asset_symbol: str
    asset_address: str
    amount_raw: str  # stored as string to avoid int overflow
    amount_human: float
    amount_usd: float
    borrower: str
    interest_rate_mode: str  # "stable" or "variable"
    borrow_rate: float
    timestamp: int
    block_number: int


@dataclass
class ReserveSnapshot:
    asset_symbol: str
    asset_address: str
    decimals: int
    available_liquidity: float
    available_liquidity_usd: float
    total_variable_debt: float
    price_usd: float
    snapshot_timestamp: int


@dataclass
class AlertEvent:
    borrow_event: BorrowEvent
    threshold_type: str  # "absolute", "relative", or "both"
    threshold_value_absolute: float
    threshold_value_relative: float
    reserve_snapshot: ReserveSnapshot
