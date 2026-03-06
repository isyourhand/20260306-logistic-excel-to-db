from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from logistics_ingest.domain.provider_catalog import ALLOWED_SOURCE_COMPANIES, infer_canonical_company_name

LLM_SYSTEM_PROMPT = (
    "You are a strict logistics-file intake classifier for an automated ETL pipeline. "
    "Your task is to decide whether a newly arrived Excel file is a logistics pricing workbook. "
    "If yes, identify the provider, classify the feed, and choose which existing active snapshot file it should replace. "
    "Respond with JSON only."
)


@dataclass
class IntakeDecision:
    decision: str
    provider: str
    feed_family: str
    replaces_filename: str
    confidence: float
    reason: str


def parse_json_object(raw: str) -> dict[str, Any] | None:
    text = raw.strip()
    if not text:
        return None
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            data = json.loads(text[start : end + 1])
            return data if isinstance(data, dict) else None
        except Exception:
            return None
    return None


def classify_with_llm(
    client: Any,
    model: str,
    path: Path,
    workbook_preview: dict[str, Any],
    current_snapshot: list[str],
) -> IntakeDecision:
    payload = {
        "task": "classify_incoming_logistics_pricing_workbook",
        "filename": path.name,
        "file_stem": path.stem,
        "folder_path": str(path.parent),
        "allowed_providers": list(ALLOWED_SOURCE_COMPANIES),
        "current_snapshot_filenames": current_snapshot,
        "workbook_preview": workbook_preview,
        "rules": [
            "Ignore irrelevant files that are not logistics pricing workbooks.",
            "If accepted, provider must be one of the allowed providers.",
            "replaces_filename must be exactly one existing current snapshot filename or empty string.",
            "Prefer replace_existing when the file is an updated version of an existing feed.",
            "Use add_new_feed when it is a new logistics feed.",
        ],
        "output_schema": {
            "decision": "accept|ignore|review",
            "provider": "one of allowed providers or unknown",
            "feed_family": "short identifier string",
            "replaces_filename": "exact existing filename or empty string",
            "confidence": "number_0_to_1",
            "reason": "short string",
        },
    }

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": LLM_SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        temperature=0.0,
        stream=False,
    )
    content = response.choices[0].message.content or ""
    parsed = parse_json_object(content)
    if parsed is None:
        return IntakeDecision(
            decision="review",
            provider="unknown",
            feed_family="",
            replaces_filename="",
            confidence=0.0,
            reason="llm_parse_failed",
        )

    decision = str(parsed.get("decision") or "").strip().lower()
    if decision not in {"accept", "ignore", "review"}:
        decision = "review"

    provider = str(parsed.get("provider") or "").strip()
    feed_family = str(parsed.get("feed_family") or "").strip()
    replaces_filename = str(parsed.get("replaces_filename") or "").strip()
    if replaces_filename not in current_snapshot:
        replaces_filename = ""

    confidence_raw = parsed.get("confidence", 0)
    try:
        confidence = float(confidence_raw)
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    reason = str(parsed.get("reason") or "").strip()

    return IntakeDecision(
        decision=decision,
        provider=provider,
        feed_family=feed_family,
        replaces_filename=replaces_filename,
        confidence=confidence,
        reason=reason,
    )


def canonicalize_decision(
    decision: IntakeDecision,
    filename: str,
    confidence_threshold: float,
) -> IntakeDecision:
    deterministic_provider = infer_canonical_company_name(filename)
    provider = decision.provider if decision.provider in ALLOWED_SOURCE_COMPANIES else "unknown"
    final_decision = decision.decision
    reason_parts: list[str] = [decision.reason] if decision.reason else []

    if provider == "unknown":
        final_decision = "review"
        reason_parts.append("provider_not_allowed")

    if deterministic_provider and provider != "unknown" and deterministic_provider != provider:
        final_decision = "review"
        reason_parts.append(f"provider_mismatch:{deterministic_provider}")

    if final_decision == "accept" and decision.confidence < confidence_threshold:
        final_decision = "review"
        reason_parts.append("confidence_below_threshold")

    if final_decision == "accept" and not decision.feed_family:
        final_decision = "review"
        reason_parts.append("feed_family_missing")

    return IntakeDecision(
        decision=final_decision,
        provider=provider,
        feed_family=decision.feed_family,
        replaces_filename=decision.replaces_filename,
        confidence=decision.confidence,
        reason=" | ".join([part for part in reason_parts if part]),
    )


def classify_filename(
    client: Any,
    model: str,
    path: Path,
    workbook_preview: dict[str, Any],
    current_snapshot: list[str],
    confidence_threshold: float,
) -> IntakeDecision:
    raw = classify_with_llm(client, model, path, workbook_preview, current_snapshot)
    return canonicalize_decision(raw, path.name, confidence_threshold)


__all__ = ["IntakeDecision", "classify_with_llm", "canonicalize_decision", "classify_filename"]
