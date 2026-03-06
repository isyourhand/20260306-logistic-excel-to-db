from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any


@dataclass
class InputWorkbook:
    filename: str
    path: Path
    expect_channels: bool = True
    provider: str = ""


@dataclass
class RateRecord:
    channel_code: str
    channel_name: str
    transport_mode: str
    cargo_natures: list[str]
    destination_keyword: str | None
    transit_days_min: int | None
    transit_days_max: int | None
    channel_note: str | None
    destination_country: str | None
    destination_scope: str
    tax_included: bool | None
    source_workbook: str
    source_company: str
    min_weight: Decimal
    max_weight: Decimal | None
    unit_price: Decimal
    currency: str
    volumetric_divisor: Decimal | None
    min_charge: Decimal


@dataclass
class SurchargeRuleRecord:
    channel_code: str
    rule_name: str
    trigger_type: str
    trigger_value: str
    calc_method: str
    amount: Decimal
    currency: str
    weight_basis: str | None
    min_charge: Decimal | None
    max_charge: Decimal | None
    requires_fuel_multiplier: bool
    stack_mode: str
    priority: int
    note: str | None
    source_excerpt: str | None


@dataclass
class DivisorCandidate:
    row_index: int
    divisor: Decimal
    text: str
    heading: str
    nearby_lines: list[str]


@dataclass
class DivisorLLMDecision:
    applies_to_main_shipping: bool
    confidence: float
    scope: str
    reason: str


@dataclass
class DivisorLLMConfig:
    enabled: bool = False
    api_key: str = ""
    model: str = "deepseek-chat"
    confidence_threshold: float = 0.8
    client: Any | None = None
    cache: dict[str, DivisorLLMDecision | None] | None = None
