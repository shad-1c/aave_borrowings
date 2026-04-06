import logging
import signal
import sys
import time

from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from .alerts import send_alert
from .config import load_config
from .detector import detect_large_borrows
from .storage import (
    get_last_processed_timestamp,
    init_database,
    mark_large_borrow,
    save_alert,
    save_borrow_events,
    save_reserve_snapshots,
)
from .subgraph import SubgraphClient

console = Console()
_running = True


def _handle_signal(sig, frame):
    global _running
    console.print("\n[yellow]Shutting down...[/yellow]")
    _running = False


def _print_cycle_summary(borrows_count: int, alerts_count: int, reserves_count: int):
    if borrows_count == 0:
        console.print("[dim]No new borrows[/dim]")
        return

    console.print(
        f"[green]+{borrows_count} borrows[/green]  "
        f"[{'red' if alerts_count else 'dim'}]{alerts_count} large[/{'red' if alerts_count else 'dim'}]  "
        f"[dim]{reserves_count} reserves tracked[/dim]"
    )


def _print_recent_borrows(borrows, limit: int = 5):
    if not borrows:
        return

    table = Table(title="Recent Borrows", show_lines=False)
    table.add_column("Asset", style="cyan")
    table.add_column("Amount", justify="right")
    table.add_column("USD", justify="right", style="green")
    table.add_column("Rate", justify="right")
    table.add_column("Borrower", max_width=12)

    for b in borrows[-limit:]:
        usd_str = f"${b.amount_usd:,.0f}" if b.amount_usd >= 1 else f"${b.amount_usd:.4f}"
        table.add_row(
            b.asset_symbol,
            f"{b.amount_human:,.4f}",
            usd_str,
            f"{b.borrow_rate:.2f}%",
            b.borrower[:12] + "...",
        )

    console.print(table)


def monitor(config_path: str = "config.yaml"):
    """Run the AAVE borrow monitoring loop."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, show_path=False)],
    )
    logger = logging.getLogger(__name__)

    config = load_config(config_path)

    if not config.subgraph.api_key:
        console.print(
            "[bold red]Error:[/bold red] THEGRAPH_API_KEY not set. "
            "Get a free key at https://thegraph.com/studio/"
        )
        sys.exit(1)

    conn = init_database(config.db_url)
    client = SubgraphClient(config.subgraph, coingecko_api_key=config.coingecko.api_key)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    console.print("[bold green]AAVE V3 Borrow Monitor started[/bold green]")
    console.print(f"  Polling interval: {config.polling_interval_seconds}s")
    console.print(f"  Default threshold: ${config.thresholds_default.usd_absolute:,.0f} / {config.thresholds_default.liquidity_pct}%")
    console.print()

    is_backfill = get_last_processed_timestamp(conn) == 0

    while _running:
        try:
            last_ts = get_last_processed_timestamp(conn)
            if last_ts == 0:
                # First run: start from the beginning of the current year
                import datetime
                jan1 = datetime.datetime(datetime.datetime.now().year, 1, 1, tzinfo=datetime.timezone.utc)
                last_ts = int(jan1.timestamp())
                console.print(f"[dim]First run — fetching borrows since Jan 1, {jan1.year}[/dim]")
            borrows = client.fetch_recent_borrows(since_timestamp=last_ts)
            reserves = client.fetch_reserve_state()

            save_reserve_snapshots(conn, reserves)
            save_borrow_events(conn, borrows)

            alerts = detect_large_borrows(borrows, reserves, config)

            for alert in alerts:
                mark_large_borrow(conn, alert.borrow_event.id)
                save_alert(
                    conn,
                    alert.borrow_event.id,
                    alert.threshold_type,
                    alert.threshold_value_absolute,
                    alert.threshold_value_relative,
                )
                if is_backfill:
                    # During backfill, only log to console — skip Telegram/webhook
                    from .alerts import _console_alert
                    if config.alerts.console:
                        _console_alert(alert)
                else:
                    send_alert(alert, config.alerts)

            _print_cycle_summary(len(borrows), len(alerts), len(reserves))
            if borrows:
                _print_recent_borrows(borrows)

            if is_backfill:
                is_backfill = False
                console.print(f"[bold green]Backfill complete — {len(alerts)} historical large borrows recorded[/bold green]")
                console.print("[bold green]Telegram notifications now active for new borrows[/bold green]")

        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}", exc_info=True)

        # Sleep in small increments so SIGINT is responsive
        for _ in range(config.polling_interval_seconds):
            if not _running:
                break
            time.sleep(1)

    conn.close()
    console.print("[bold green]Monitor stopped.[/bold green]")


def analyze_cmd():
    """Run price correlation analysis."""
    import argparse

    parser = argparse.ArgumentParser(description="Analyze large borrow / price correlations")
    parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")
    parser.add_argument("--asset", type=str, default=None, help="Filter by asset symbol")
    parser.add_argument("--config", type=str, default="config.yaml", help="Config file path")
    args = parser.parse_args(sys.argv[2:])  # skip 'analyze' subcommand

    from .analysis import run_analysis

    config = load_config(args.config)
    conn = init_database(config.db_url)
    run_analysis(conn, config, days=args.days, asset_filter=args.asset)
    conn.close()


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "analyze":
        analyze_cmd()
    else:
        monitor()


if __name__ == "__main__":
    main()
