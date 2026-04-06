from __future__ import annotations

from .models import BorrowEvent, ReserveSnapshot

import psycopg2
import psycopg2.extras


def init_database(db_url: str):
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS borrow_events (
            id TEXT PRIMARY KEY,
            tx_hash TEXT,
            asset_symbol TEXT,
            asset_address TEXT,
            amount_raw TEXT,
            amount_human DOUBLE PRECISION,
            amount_usd DOUBLE PRECISION,
            borrower TEXT,
            interest_rate_mode TEXT,
            borrow_rate DOUBLE PRECISION,
            timestamp INTEGER,
            block_number INTEGER,
            is_large_borrow INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS reserve_snapshots (
            id SERIAL PRIMARY KEY,
            asset_symbol TEXT,
            asset_address TEXT,
            decimals INTEGER,
            available_liquidity DOUBLE PRECISION,
            available_liquidity_usd DOUBLE PRECISION,
            total_variable_debt DOUBLE PRECISION,
            price_usd DOUBLE PRECISION,
            snapshot_timestamp INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS alert_events (
            id SERIAL PRIMARY KEY,
            borrow_event_id TEXT REFERENCES borrow_events(id),
            threshold_type TEXT,
            threshold_value_absolute DOUBLE PRECISION,
            threshold_value_relative DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS price_data (
            asset_symbol TEXT,
            timestamp INTEGER,
            price_usd DOUBLE PRECISION,
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


def get_last_processed_timestamp(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT MAX(timestamp) as ts FROM borrow_events")
    row = cur.fetchone()
    return row[0] if row and row[0] else 0


def save_borrow_events(conn, events: list[BorrowEvent]) -> int:
    if not events:
        return 0
    cur = conn.cursor()
    saved = 0
    for e in events:
        cur.execute(
            """INSERT INTO borrow_events
               (id, tx_hash, asset_symbol, asset_address, amount_raw, amount_human,
                amount_usd, borrower, interest_rate_mode, borrow_rate, timestamp, block_number)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT DO NOTHING""",
            (e.id, e.tx_hash, e.asset_symbol, e.asset_address, e.amount_raw,
             e.amount_human, e.amount_usd, e.borrower, e.interest_rate_mode,
             e.borrow_rate, e.timestamp, e.block_number),
        )
        saved += cur.rowcount
    conn.commit()
    return saved


def mark_large_borrow(conn, event_id: str):
    cur = conn.cursor()
    cur.execute("UPDATE borrow_events SET is_large_borrow = 1 WHERE id = %s", (event_id,))
    conn.commit()


def save_alert(conn, borrow_id: str, threshold_type: str,
               threshold_abs: float, threshold_rel: float):
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO alert_events (borrow_event_id, threshold_type,
           threshold_value_absolute, threshold_value_relative) VALUES (%s, %s, %s, %s)""",
        (borrow_id, threshold_type, threshold_abs, threshold_rel),
    )
    conn.commit()


def save_reserve_snapshots(conn, snapshots: list[ReserveSnapshot]):
    cur = conn.cursor()
    for s in snapshots:
        cur.execute(
            """INSERT INTO reserve_snapshots
               (asset_symbol, asset_address, decimals, available_liquidity,
                available_liquidity_usd, total_variable_debt, price_usd, snapshot_timestamp)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (s.asset_symbol, s.asset_address, s.decimals, s.available_liquidity,
             s.available_liquidity_usd, s.total_variable_debt, s.price_usd,
             s.snapshot_timestamp),
        )
    conn.commit()


def save_price_data(conn, asset_symbol: str,
                    prices: list[tuple[int, float]]):
    cur = conn.cursor()
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO price_data (asset_symbol, timestamp, price_usd) VALUES %s ON CONFLICT DO NOTHING",
        [(asset_symbol, ts, price) for ts, price in prices],
    )
    conn.commit()


def get_price_data(conn, asset_symbol: str,
                   start_ts: int, end_ts: int) -> list[tuple[int, float]]:
    cur = conn.cursor()
    cur.execute(
        """SELECT timestamp, price_usd FROM price_data
           WHERE asset_symbol = %s AND timestamp BETWEEN %s AND %s
           ORDER BY timestamp""",
        (asset_symbol, start_ts, end_ts),
    )
    return [(r[0], r[1]) for r in cur.fetchall()]


def get_large_borrows(conn, asset_symbol: str | None = None,
                      start_ts: int = 0, end_ts: int | None = None) -> list[dict]:
    query = "SELECT * FROM borrow_events WHERE is_large_borrow = 1 AND timestamp >= %s"
    params: list = [start_ts]
    if end_ts:
        query += " AND timestamp <= %s"
        params.append(end_ts)
    if asset_symbol:
        query += " AND asset_symbol = %s"
        params.append(asset_symbol)
    query += " ORDER BY timestamp"
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(query, params)
    return [dict(r) for r in cur.fetchall()]
