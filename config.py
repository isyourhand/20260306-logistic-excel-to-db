from __future__ import annotations

# Local defaults for scripts in this workspace.
# If needed, override any value via CLI args or environment variables.

PG_DSN = "host=localhost port=5432 dbname=logistics_pricing user=postgres password=lxe0122"
# Optional key for DeepSeek API usage in normalize_rates_to_pg.py (--llm-divisor-check)
DEEPSEEK_API_KEY = "sk-ac2c60c434e24326acd88063aae2ab8e"
