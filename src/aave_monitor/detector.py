import logging

from .config import Config
from .models import AlertEvent, BorrowEvent, ReserveSnapshot

logger = logging.getLogger(__name__)


def detect_large_borrows(
    borrows: list[BorrowEvent],
    reserves: list[ReserveSnapshot],
    config: Config,
) -> list[AlertEvent]:
    """Check each borrow against configured thresholds."""
    reserve_map = {r.asset_symbol: r for r in reserves}
    alerts: list[AlertEvent] = []

    for borrow in borrows:
        threshold = config.get_threshold(borrow.asset_symbol)
        reserve = reserve_map.get(borrow.asset_symbol)

        if not reserve:
            logger.warning(f"No reserve snapshot for {borrow.asset_symbol}, skipping threshold check")
            continue

        hit_absolute = borrow.amount_usd >= threshold.usd_absolute

        hit_relative = False
        if reserve.available_liquidity_usd > 0:
            pct = (borrow.amount_usd / reserve.available_liquidity_usd) * 100
            hit_relative = pct >= threshold.liquidity_pct

        if hit_absolute and hit_relative:
            threshold_type = "both"
        elif hit_absolute:
            threshold_type = "absolute"
        elif hit_relative:
            threshold_type = "relative"
        else:
            continue

        alert = AlertEvent(
            borrow_event=borrow,
            threshold_type=threshold_type,
            threshold_value_absolute=threshold.usd_absolute,
            threshold_value_relative=threshold.liquidity_pct,
            reserve_snapshot=reserve,
        )
        alerts.append(alert)
        logger.info(
            f"LARGE BORROW: {borrow.asset_symbol} ${borrow.amount_usd:,.0f} "
            f"({threshold_type} threshold)"
        )

    return alerts
