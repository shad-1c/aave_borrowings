from __future__ import annotations

import sqlite3
from pathlib import Path

from .models import BorrowEvent, ReserveSnapshot


def init_database(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS borrow_events (
            id TEXT PRIMARY KEY,
            tx_hash TEXT,
            asset_symbol TEXT,
            asset_address TEXT,
            amount_raw TEXT,
            amount_human REAL,
            amount_usd REAL,
            borrower TEXT,
            interest_rate_mode TEXT,
            borrow_rate REAL,
            timestamp INTEGER,
            block_number INTEGER,
            is_large_borrow INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS reserve_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_symbol TEXT,
            asset_address TEXT,
            decimals INTEGER,
            available_liquidity REAL,
            available_liquidity_usd REAL,
            total_variable_debt REAL,
            price_usd REAL,
            snapshot_timestamp INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS alert_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            borrow_event_id TEXT REFERENCES borrow_events(id),
            threshold_type TEXT,
            threshold_value_absolute REAL,
            threshold_value_relative REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS price_data (
            asset_symbol TEXT,
            timestamp INTEGER,
            price_usd REAL,
            PRIMARY KEY (asset_symbol, timestamp)
        );

        CREATE INDEX IF NOT EXISTS idx_borrows_timestamp ON borrow_events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_borrows_asset ON borrow_events(asset_symbol);
        CREATE INDEX IF NOT EXISTS idx_borrows_large ON borrow_events(is_large_borrow);
        CREATE INDEX IF NOT EXISTS idx_snapshots_asset_ts ON reserve_snapshots(asset_symbol, snapshot_timestamp);
        CREATE INDEX IF NOT EXISTS idx_price_asset_ts ON price_data(asset_symbol, timestamp);
    """)
    conn.commit()
    return conn


def get_last_processed_timestamp(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(timestamp) as ts FROM borrow_events").fetchone()
    return row["ts"] if row and row["ts"] else 0


def save_borrow_events(conn: sqlite3.Connection, events: list[BorrowEvent]) -> int:
    saved = 0
    for e in events:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO borrow_events
                   (id, tx_hash, asset_symbol, asset_address, amount_raw, amount_human,
                    amount_usd, borrower, interest_rate_mode, borrow_rate, timestamp, block_number)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (e.id, e.tx_hash, e.asset_symbol, e.asset_address, e.amount_raw,
                 e.amount_human, e.amount_usd, e.borrower, e.interest_rate_mode,
                 e.borrow_rate, e.timestamp, e.block_number),
            )
            saved += conn.total_changes
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return saved


def mark_large_borrow(conn: sqlite3.Connection, event_id: str):
    conn.execute("UPDATE borrow_events SET is_large_borrow = 1 WHERE id = ?", (event_id,))
    conn.commit()


def save_alert(conn: sqlite3.Connection, borrow_id: str, threshold_type: str,
               threshold_abs: float, threshold_rel: float):
    conn.execute(
        """INSERT INTO alert_events (borrow_event_id, threshold_type,
           threshold_value_absolute, threshold_value_relative) VALUES (?, ?, ?, ?)""",
        (borrow_id, threshold_type, threshold_abs, threshold_rel),
    )
    conn.commit()


def save_reserve_snapshots(conn: sqlite3.Connection, snapshots: list[ReserveSnapshot]):
    for s in snapshots:
        conn.execute(
            """INSERT INTO reserve_snapshots
               (asset_symbol, asset_address, decimals, available_liquidity,
                available_liquidity_usd, total_variable_debt, price_usd, snapshot_timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (s.asset_symbol, s.asset_address, s.decimals, s.available_liquidity,
             s.available_liquidity_usd, s.total_variable_debt, s.price_usd,
             s.snapshot_timestamp),
        )
    conn.commit()


def save_price_data(conn: sqlite3.Connection, asset_symbol: str,
                    prices: list[tuple[int, float]]):
    conn.executemany(
        "INSERT OR IGNORE INTO price_data (asset_symbol, timestamp, price_usd) VALUES (?, ?, ?)",
        [(asset_symbol, ts, price) for ts, price in prices],
    )
    conn.commit()


def get_price_data(conn: sqlite3.Connection, asset_symbol: str,
                   start_ts: int, end_ts: int) -> list[tuple[int, float]]:
    rows = conn.execute(
        """SELECT timestamp, price_usd FROM price_data
           WHERE asset_symbol = ? AND timestamp BETWEEN ? AND ?
           ORDER BY timestamp""",
        (asset_symbol, start_ts, end_ts),
    ).fetchall()
    return [(r["timestamp"], r["price_usd"]) for r in rows]


def get_large_borrows(conn: sqlite3.Connection, asset_symbol: str | None = None,
                      start_ts: int = 0, end_ts: int | None = None) -> list[dict]:
    query = "SELECT * FROM borrow_events WHERE is_large_borrow = 1 AND timestamp >= ?"
    params: list = [start_ts]
    if end_ts:
        query += " AND timestamp <= ?"
        params.append(end_ts)
    if asset_symbol:
        query += " AND asset_symbol = ?"
        params.append(asset_symbol)
    query += " ORDER BY timestamp"
    return [dict(r) for r in conn.execute(query, params).fetchall()]
