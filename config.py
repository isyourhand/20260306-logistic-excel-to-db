from __future__ import annotations

# Local defaults for scripts in this workspace.
# If needed, override any value via CLI args or environment variables.

PG_DSN = "host=localhost port=5432 dbname=logistics_pricing user=postgres password=lxe0122"
# Local-only key for DeepSeek API usage in llm_readable_report.py
DEEPSEEK_API_KEY = "sk-ac2c60c434e24326acd88063aae2ab8e"

PRICING_ENGINE_DEFAULTS = {
    "warehouse": "ONT8",
    "address": "San Bernardino, CA",
    "target_country": "US",
    "transport_mode": "air",
    "cargo_nature": "general",
    "tax_included": "any",
    "boxes": [
        {
            "gross_weight": 12.5,
            "length": 40,
            "width": 30,
            "height": 25,
        }
    ],
    "top_n": 3,
}
