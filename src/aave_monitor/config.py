from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv


@dataclass
class ThresholdConfig:
    usd_absolute: float = 1_000_000
    liquidity_pct: float = 5.0


@dataclass
class SubgraphConfig:
    subgraph_id: str = "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"
    page_size: int = 100
    api_key: str = ""

    @property
    def endpoint(self) -> str:
        return (
            f"https://gateway.thegraph.com/api/"
            f"{self.api_key}/subgraphs/id/{self.subgraph_id}"
        )


@dataclass
class AlertsConfig:
    console: bool = True
    webhook_url: str | None = None
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None


@dataclass
class CoinGeckoConfig:
    base_url: str = "https://api.coingecko.com/api/v3"
    api_key: str = ""
    rate_limit_per_minute: int = 10


@dataclass
class Config:
    polling_interval_seconds: int = 30
    subgraph: SubgraphConfig = field(default_factory=SubgraphConfig)
    thresholds_default: ThresholdConfig = field(default_factory=ThresholdConfig)
    thresholds_overrides: dict[str, ThresholdConfig] = field(default_factory=dict)
    alerts: AlertsConfig = field(default_factory=AlertsConfig)
    coingecko: CoinGeckoConfig = field(default_factory=CoinGeckoConfig)
    db_url: str = "postgresql://localhost/aave_monitor"

    def get_threshold(self, asset_symbol: str) -> ThresholdConfig:
        return self.thresholds_overrides.get(asset_symbol, self.thresholds_default)


def load_config(config_path: str = "config.yaml") -> Config:
    load_dotenv()

    raw = {}
    config_file = Path(config_path)
    if config_file.exists():
        with open(config_file) as f:
            raw = yaml.safe_load(f) or {}

    # Build subgraph config
    sg_raw = raw.get("subgraph", {})
    subgraph = SubgraphConfig(
        subgraph_id=sg_raw.get("subgraph_id", SubgraphConfig.subgraph_id),
        page_size=sg_raw.get("page_size", SubgraphConfig.page_size),
        api_key=os.getenv("THEGRAPH_API_KEY", ""),
    )

    # Build threshold configs
    thresh_raw = raw.get("thresholds", {})
    default_raw = thresh_raw.get("default", {})
    thresholds_default = ThresholdConfig(
        usd_absolute=default_raw.get("usd_absolute", 1_000_000),
        liquidity_pct=default_raw.get("liquidity_pct", 5.0),
    )
    thresholds_overrides = {}
    for symbol, vals in thresh_raw.get("overrides", {}).items():
        thresholds_overrides[symbol] = ThresholdConfig(
            usd_absolute=vals.get("usd_absolute", thresholds_default.usd_absolute),
            liquidity_pct=vals.get("liquidity_pct", thresholds_default.liquidity_pct),
        )

    # Build alerts config
    alerts_raw = raw.get("alerts", {})
    alerts = AlertsConfig(
        console=alerts_raw.get("console", True),
        webhook_url=os.getenv("WEBHOOK_URL") or alerts_raw.get("webhook_url"),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN") or alerts_raw.get("telegram_bot_token"),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID") or alerts_raw.get("telegram_chat_id"),
    )

    # Build CoinGecko config
    cg_raw = raw.get("coingecko", {})
    coingecko = CoinGeckoConfig(
        base_url=cg_raw.get("base_url", CoinGeckoConfig.base_url),
        api_key=os.getenv("COINGECKO_API_KEY", ""),
        rate_limit_per_minute=cg_raw.get("rate_limit_per_minute", 10),
    )

    return Config(
        polling_interval_seconds=raw.get("polling_interval_seconds", 30),
        subgraph=subgraph,
        thresholds_default=thresholds_default,
        thresholds_overrides=thresholds_overrides,
        alerts=alerts,
        coingecko=coingecko,
        db_url=os.getenv("DATABASE_URL") or raw.get("db_url", "postgresql://localhost/aave_monitor"),
    )
