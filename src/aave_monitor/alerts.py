import logging

import requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .config import AlertsConfig
from .models import AlertEvent

logger = logging.getLogger(__name__)
console = Console()


def send_alert(alert: AlertEvent, config: AlertsConfig):
    """Dispatch alert to all configured channels."""
    if config.console:
        _console_alert(alert)
    if config.webhook_url:
        _webhook_alert(alert, config.webhook_url)
    if config.telegram_bot_token and config.telegram_chat_id:
        _telegram_alert(alert, config.telegram_bot_token, config.telegram_chat_id)


def _format_amount(amount: float) -> str:
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:,.2f}M"
    if amount >= 1_000:
        return f"${amount / 1_000:,.1f}K"
    return f"${amount:,.2f}"


def _console_alert(alert: AlertEvent):
    b = alert.borrow_event
    r = alert.reserve_snapshot

    utilization_pct = 0.0
    if r.available_liquidity_usd > 0:
        utilization_pct = (b.amount_usd / r.available_liquidity_usd) * 100

    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Field", style="bold cyan")
    table.add_column("Value")

    table.add_row("Asset", b.asset_symbol)
    table.add_row("Amount", f"{b.amount_human:,.4f} ({_format_amount(b.amount_usd)})")
    table.add_row("Borrower", b.borrower)
    table.add_row("Rate Mode", b.interest_rate_mode)
    table.add_row("Borrow Rate", f"{b.borrow_rate:.2f}%")
    table.add_row("Threshold Hit", alert.threshold_type.upper())
    table.add_row("Pool Liquidity", _format_amount(r.available_liquidity_usd))
    table.add_row("% of Pool", f"{utilization_pct:.2f}%")
    table.add_row("Tx", b.tx_hash)

    title = f"[bold red]LARGE BORROW DETECTED[/bold red] - {b.asset_symbol}"
    console.print(Panel(table, title=title, border_style="red"))


def _build_text_message(alert: AlertEvent) -> str:
    b = alert.borrow_event
    r = alert.reserve_snapshot
    utilization_pct = 0.0
    if r.available_liquidity_usd > 0:
        utilization_pct = (b.amount_usd / r.available_liquidity_usd) * 100

    return (
        f"LARGE BORROW: {b.asset_symbol}\n"
        f"Amount: {b.amount_human:,.4f} ({_format_amount(b.amount_usd)})\n"
        f"Threshold: {alert.threshold_type}\n"
        f"Pool Liquidity: {_format_amount(r.available_liquidity_usd)}\n"
        f"% of Pool: {utilization_pct:.2f}%\n"
        f"Rate: {b.borrow_rate:.2f}% ({b.interest_rate_mode})\n"
        f"Borrower: {b.borrower}\n"
        f"Tx: {b.tx_hash}"
    )


def _webhook_alert(alert: AlertEvent, webhook_url: str):
    try:
        payload = {"content": _build_text_message(alert)}
        requests.post(webhook_url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Webhook alert failed: {e}")


def _telegram_alert(alert: AlertEvent, bot_token: str, chat_id: str):
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": _build_text_message(alert),
            "parse_mode": "HTML",
        }
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Telegram alert failed: {e}")
