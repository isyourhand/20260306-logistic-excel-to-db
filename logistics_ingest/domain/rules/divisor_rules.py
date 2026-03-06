from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal
from typing import Any

from logistics_ingest.domain.models import DivisorCandidate, DivisorLLMDecision
from logistics_ingest.shared.text_utils import normalize_text

DEFAULT_VOLUMETRIC_DIVISOR = Decimal("6000")
DIVISOR_RE = re.compile(r"(?:材积|体积)[^0-9]{0,12}/\s*(\d{3,5})")
CONTEXT_HEADER_TOKENS = {"渠道名称", "国家", "国家/地区", "国家/仓库代码", "时效/备注"}
DIVISOR_LLM_SYSTEM_PROMPT = (
    "You are a strict logistics pricing parser. "
    "Decide whether a volumetric divisor mention applies to MAIN shipping rate tiers "
    "or only applies to after-shipping services (return/reroute/claims/fees). "
    "Respond with JSON only."
)


def summarize_row_text(row: list[str], max_tokens: int = 2) -> str:
    tokens = [normalize_text(c) for c in row if normalize_text(c)]
    if not tokens:
        return ""
    unique = list(dict.fromkeys(tokens))
    if len(unique) == 1:
        return unique[0]
    return " | ".join(unique[:max_tokens])


def find_heading_above(rows: list[list[str]], row_index: int, max_lookback: int = 12) -> str:
    for ridx in range(row_index - 1, max(0, row_index - max_lookback) - 1, -1):
        text = summarize_row_text(rows[ridx], max_tokens=2)
        if not text:
            continue
        if "返回目录" in text or text in CONTEXT_HEADER_TOKENS:
            continue
        if len(text) <= 120:
            return text
    return ""


def collect_nearby_lines(rows: list[list[str]], row_index: int, window: int = 2) -> list[str]:
    lines: list[str] = []
    start = max(0, row_index - window)
    end = min(len(rows) - 1, row_index + window)
    for ridx in range(start, end + 1):
        text = summarize_row_text(rows[ridx], max_tokens=2)
        if text:
            lines.append(f"r{ridx + 1}: {text}")
    return lines


def collect_divisor_candidates(rows: list[list[str]]) -> list[DivisorCandidate]:
    candidates: list[DivisorCandidate] = []
    seen: set[tuple[int, str]] = set()
    for ridx, row in enumerate(rows):
        for cell in row:
            t = normalize_text(cell)
            if not t:
                continue
            m = DIVISOR_RE.search(t)
            if not m:
                continue
            value = Decimal(m.group(1))
            if value <= 0:
                continue
            key = (ridx, str(value))
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                DivisorCandidate(
                    row_index=ridx + 1,
                    divisor=value,
                    text=t[:300],
                    heading=find_heading_above(rows, ridx),
                    nearby_lines=collect_nearby_lines(rows, ridx, window=2),
                )
            )
            break
    return candidates


def parse_divisor_llm_decision(text: str) -> DivisorLLMDecision | None:
    raw = text.strip()
    payload: dict[str, Any] | None = None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            payload = parsed
    except Exception:
        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            try:
                parsed = json.loads(m.group(0))
                if isinstance(parsed, dict):
                    payload = parsed
            except Exception:
                payload = None
    if payload is None:
        return None

    applies = bool(payload.get("applies_to_main_shipping"))
    confidence_raw = payload.get("confidence", 0)
    try:
        confidence = float(confidence_raw)
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    scope = str(payload.get("scope") or "").strip().lower()
    reason = str(payload.get("reason") or "").strip()
    return DivisorLLMDecision(
        applies_to_main_shipping=applies,
        confidence=confidence,
        scope=scope,
        reason=reason,
    )


def llm_decide_divisor_candidate(config: Any, workbook: str, sheet: str, candidate: DivisorCandidate) -> DivisorLLMDecision | None:
    if not getattr(config, "enabled", False) or getattr(config, "client", None) is None:
        return None

    cache = getattr(config, "cache", None) if getattr(config, "cache", None) is not None else {}
    cache_key = hashlib.md5(
        f"{workbook}|{sheet}|{candidate.row_index}|{candidate.divisor}|{candidate.text}|{candidate.heading}".encode("utf-8")
    ).hexdigest()
    if cache_key in cache:
        return cache[cache_key]

    user_payload = {
        "task": "decide_if_divisor_applies_to_main_shipping_rate_table",
        "workbook": workbook,
        "sheet": sheet,
        "candidate_divisor": str(candidate.divisor),
        "candidate_row_index": candidate.row_index,
        "candidate_line": candidate.text,
        "nearest_heading_above": candidate.heading,
        "nearby_lines": candidate.nearby_lines,
        "output_schema": {
            "applies_to_main_shipping": "boolean",
            "confidence": "number_0_to_1",
            "scope": "main_rate|return_only|unknown",
            "reason": "short_string",
        },
    }
    try:
        resp = config.client.chat.completions.create(
            model=getattr(config, "model", "deepseek-chat"),
            messages=[
                {"role": "system", "content": DIVISOR_LLM_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            stream=False,
            temperature=0.0,
        )
        text = resp.choices[0].message.content or ""
        decision = parse_divisor_llm_decision(text)
    except Exception:
        decision = None

    if getattr(config, "cache", None) is not None:
        config.cache[cache_key] = decision
    return decision


def extract_divisor(
    rows: list[list[str]],
    workbook: str = "",
    sheet: str = "",
    llm_config: Any | None = None,
    audit_rows: list[dict[str, str]] | None = None,
) -> Decimal:
    candidates = collect_divisor_candidates(rows)
    if not candidates:
        return DEFAULT_VOLUMETRIC_DIVISOR

    if llm_config is None or not getattr(llm_config, "enabled", False):
        return candidates[0].divisor

    threshold = float(getattr(llm_config, "confidence_threshold", 0.8))
    for candidate in candidates:
        decision = llm_decide_divisor_candidate(llm_config, workbook, sheet, candidate)
        if audit_rows is not None:
            if decision is None:
                audit_rows.append(
                    {
                        "workbook": workbook,
                        "sheet": sheet,
                        "row_index": str(candidate.row_index),
                        "context_title": candidate.heading,
                        "destination_text": candidate.text,
                        "destination_scope": "",
                        "destination_country": "",
                        "destination_keyword": "",
                        "transport_mode": "",
                        "divisor_candidate": str(candidate.divisor),
                        "divisor_decision": "llm_parse_failed",
                        "llm_confidence": "0",
                        "flags": "divisor_llm_parse_failed",
                    }
                )
            else:
                decision_flag = (
                    "divisor_candidate_applied"
                    if decision.applies_to_main_shipping and decision.confidence >= threshold
                    else "divisor_candidate_rejected"
                )
                audit_rows.append(
                    {
                        "workbook": workbook,
                        "sheet": sheet,
                        "row_index": str(candidate.row_index),
                        "context_title": candidate.heading,
                        "destination_text": candidate.text,
                        "destination_scope": "",
                        "destination_country": "",
                        "destination_keyword": "",
                        "transport_mode": "",
                        "divisor_candidate": str(candidate.divisor),
                        "divisor_decision": decision.scope or decision.reason or decision_flag,
                        "llm_confidence": f"{decision.confidence:.3f}",
                        "flags": decision_flag,
                    }
                )

        if decision and decision.applies_to_main_shipping and decision.confidence >= threshold:
            return candidate.divisor

    return DEFAULT_VOLUMETRIC_DIVISOR


__all__ = ["extract_divisor"]
