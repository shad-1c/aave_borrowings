from __future__ import annotations

import logging
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from datetime import datetime
from rich.console import Console
from rich.table import Table

from .config import Config
from .prices import PriceFetcher
from .storage import get_large_borrows

logger = logging.getLogger(__name__)
console = Console()

CHARTS_DIR = "data/charts"


def _ensure_charts_dir():
    Path(CHARTS_DIR).mkdir(parents=True, exist_ok=True)


def _find_closest_price(prices: list[tuple[int, float]], target_ts: int) -> float | None:
    """Find the price closest to the target timestamp."""
    if not prices:
        return None
    closest = min(prices, key=lambda p: abs(p[0] - target_ts))
    # Don't use if more than 30 minutes away
    if abs(closest[0] - target_ts) > 1800:
        return None
    return closest[1]


def compute_price_changes(
    prices: list[tuple[int, float]], event_ts: int
) -> dict[str, float | None]:
    """Compute price changes at various intervals relative to event timestamp."""
    price_at_event = _find_closest_price(prices, event_ts)
    if price_at_event is None or price_at_event == 0:
        return {}

    intervals = {
        "pre_1h": event_ts - 3600,
        "pre_2h": event_ts - 7200,
        "post_1h": event_ts + 3600,
        "post_2h": event_ts + 7200,
        "post_6h": event_ts + 21600,
        "post_24h": event_ts + 86400,
    }

    changes = {"price_at_event": price_at_event}
    for label, ts in intervals.items():
        price = _find_closest_price(prices, ts)
        if price is not None:
            pct_change = ((price - price_at_event) / price_at_event) * 100
            changes[label] = pct_change
        else:
            changes[label] = None

    return changes


def run_analysis(
    conn, config: Config,
    days: int = 7, asset_filter: str | None = None,
):
    """Run price correlation analysis on large borrow events."""
    _ensure_charts_dir()

    end_ts = int(time.time())
    start_ts = end_ts - (days * 86400)

    borrows = get_large_borrows(conn, asset_symbol=asset_filter, start_ts=start_ts, end_ts=end_ts)

    if not borrows:
        console.print(f"[yellow]No large borrows found in the last {days} days.[/yellow]")
        return

    console.print(f"[bold]Analyzing {len(borrows)} large borrows over {days} days[/bold]\n")

    fetcher = PriceFetcher(config.coingecko, conn)
    results = []

    for borrow in borrows:
        symbol = borrow["asset_symbol"]
        event_ts = borrow["timestamp"]

        # Fetch price data in a 26h window around the event
        prices = fetcher.get_price_around_event(symbol, event_ts, hours_before=2, hours_after=24)
        changes = compute_price_changes(prices, event_ts)

        if changes:
            results.append({
                "symbol": symbol,
                "amount_usd": borrow["amount_usd"],
                "timestamp": event_ts,
                "tx_hash": borrow["tx_hash"],
                **changes,
            })

    if not results:
        console.print("[yellow]Could not fetch price data for any events.[/yellow]")
        return

    df = pd.DataFrame(results)

    # Print summary table
    _print_summary_table(df)

    # Print per-asset statistics
    _print_asset_stats(df)

    # Generate charts
    _plot_price_changes_histogram(df)
    _plot_events_timeline(df, fetcher)

    console.print(f"\n[green]Charts saved to {CHARTS_DIR}/[/green]")


def _print_summary_table(df: pd.DataFrame):
    table = Table(title="Large Borrow Events with Price Impact")
    table.add_column("Asset", style="cyan")
    table.add_column("Amount USD", justify="right", style="green")
    table.add_column("Pre 1h", justify="right")
    table.add_column("Post 1h", justify="right")
    table.add_column("Post 6h", justify="right")
    table.add_column("Post 24h", justify="right")
    table.add_column("Date")

    for _, row in df.iterrows():
        def fmt_pct(val):
            if val is None or pd.isna(val):
                return "[dim]N/A[/dim]"
            color = "red" if val < 0 else "green"
            return f"[{color}]{val:+.2f}%[/{color}]"

        date_str = datetime.fromtimestamp(row["timestamp"]).strftime("%Y-%m-%d %H:%M")
        table.add_row(
            row["symbol"],
            f"${row['amount_usd']:,.0f}",
            fmt_pct(row.get("pre_1h")),
            fmt_pct(row.get("post_1h")),
            fmt_pct(row.get("post_6h")),
            fmt_pct(row.get("post_24h")),
            date_str,
        )

    console.print(table)


def _print_asset_stats(df: pd.DataFrame):
    console.print("\n[bold]Aggregate Statistics by Asset[/bold]")

    for symbol, group in df.groupby("symbol"):
        console.print(f"\n[cyan]{symbol}[/cyan] ({len(group)} events)")

        for period in ["post_1h", "post_2h", "post_6h", "post_24h"]:
            valid = group[period].dropna()
            if valid.empty:
                continue
            avg = valid.mean()
            median = valid.median()
            pct_negative = (valid < 0).sum() / len(valid) * 100

            console.print(
                f"  {period}: avg={avg:+.2f}%  median={median:+.2f}%  "
                f"dropped={pct_negative:.0f}% of times"
            )


def _plot_price_changes_histogram(df: pd.DataFrame):
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    for ax, period, title in zip(
        axes,
        ["post_1h", "post_6h", "post_24h"],
        ["1 Hour After", "6 Hours After", "24 Hours After"],
    ):
        data = df[period].dropna()
        if data.empty:
            ax.set_title(f"{title}\n(no data)")
            continue

        ax.hist(data, bins=20, edgecolor="black", alpha=0.7, color="steelblue")
        ax.axvline(x=0, color="red", linestyle="--", alpha=0.7)
        ax.axvline(x=data.mean(), color="orange", linestyle="-", alpha=0.9, label=f"Mean: {data.mean():+.2f}%")
        ax.set_xlabel("Price Change (%)")
        ax.set_ylabel("Count")
        ax.set_title(f"Price Change {title}")
        ax.legend()

    plt.tight_layout()
    plt.savefig(f"{CHARTS_DIR}/price_change_histogram.png", dpi=150)
    plt.close()


def _plot_events_timeline(df: pd.DataFrame, fetcher: PriceFetcher):
    """Plot price timeline with borrow events marked for each asset."""
    symbols = df["symbol"].unique()

    for symbol in symbols:
        asset_df = df[df["symbol"] == symbol]
        if asset_df.empty:
            continue

        # Get price data spanning all events
        min_ts = int(asset_df["timestamp"].min()) - 7200
        max_ts = int(asset_df["timestamp"].max()) + 86400

        prices = fetcher.fetch_price_range(symbol, min_ts, max_ts)
        if not prices:
            continue

        fig, ax = plt.subplots(figsize=(14, 6))

        # Plot price line
        price_times = [datetime.fromtimestamp(p[0]) for p in prices]
        price_vals = [p[1] for p in prices]
        ax.plot(price_times, price_vals, "b-", alpha=0.7, linewidth=1)

        # Mark borrow events
        for _, row in asset_df.iterrows():
            event_time = datetime.fromtimestamp(row["timestamp"])
            ax.axvline(x=event_time, color="red", linestyle="--", alpha=0.5)
            ax.annotate(
                f"${row['amount_usd'] / 1e6:.1f}M",
                xy=(event_time, max(price_vals) * 0.95),
                fontsize=8, color="red", rotation=45,
                ha="left",
            )

        ax.set_title(f"{symbol} Price with Large Borrow Events")
        ax.set_xlabel("Time")
        ax.set_ylabel("Price (USD)")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{CHARTS_DIR}/{symbol.lower()}_timeline.png", dpi=150)
        plt.close()
