# llm_readable_report.py
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from openai import OpenAI

try:
    from config import PG_DSN as CONFIG_PG_DSN
    from config import DEEPSEEK_API_KEY as CONFIG_DEEPSEEK_API_KEY
except Exception:
    CONFIG_PG_DSN = ""
    CONFIG_DEEPSEEK_API_KEY = ""


SYSTEM_PROMPT = (
    "You are a logistics pricing analyst. "
    "Use only the provided JSON. Do not invent prices, channels, or rules. "
    "If a field is missing, say it is missing."
)


def load_engine_result(args: argparse.Namespace) -> dict[str, Any]:
    if args.input_file:
        return json.loads(Path(args.input_file).read_text(encoding="utf-8"))

    cmd = [sys.executable, "pricing_engine.py"]
    if args.dsn:
        cmd.extend(["--dsn", args.dsn])
    if args.engine_args:
        cmd.extend(args.engine_args)

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"pricing_engine.py failed: {proc.stderr.strip() or proc.stdout.strip()}")

    return json.loads(proc.stdout)


def build_user_prompt(result: dict[str, Any], language: str) -> str:
    language_label = "简体中文" if language == "zh" else "English"
    return (
        f"Please convert this machine JSON result into a concise, human-readable report in {language_label}.\n"
        "Requirements:\n"
        "1) Start with Top Recommendations (table-like bullets).\n"
        "2) Include company/source workbook for each recommended channel.\n"
        "3) Explain why #1 is chosen using exact numbers from JSON.\n"
        "4) Summarize rejected reasons from rejected_summary.\n"
        "5) Give 2-3 actionable next steps to improve options.\n"
        "6) No hallucinations; only use provided fields.\n\n"
        "JSON:\n"
        f"{json.dumps(result, ensure_ascii=False)}"
    )


def call_deepseek(api_key: str, model: str, user_prompt: str) -> str:
    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        stream=False,
        temperature=0.2,
    )
    return resp.choices[0].message.content or ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate readable report from pricing_engine JSON using DeepSeek")
    parser.add_argument(
        "--api-key",
        default=os.getenv("DEEPSEEK_API_KEY", CONFIG_DEEPSEEK_API_KEY),
        help="DeepSeek API key (arg > env DEEPSEEK_API_KEY > config.py)",
    )
    parser.add_argument(
        "--model",
        default="deepseek-chat",
        help="DeepSeek model name (default: deepseek-chat)",
    )
    parser.add_argument(
        "--input-file",
        default="",
        help="Existing pricing_engine JSON file. If omitted, run pricing_engine.py directly.",
    )
    parser.add_argument(
        "--dsn",
        default=os.getenv("PG_DSN", CONFIG_PG_DSN),
        help="DB DSN passed to pricing_engine.py when --input-file is not provided.",
    )
    parser.add_argument(
        "--engine-args",
        nargs="*",
        default=[],
        help="Extra args forwarded to pricing_engine.py, e.g. --engine-args --transport-mode air",
    )
    parser.add_argument(
        "--language",
        choices=("zh", "en"),
        default="zh",
        help="Output language",
    )
    parser.add_argument(
        "--save-json",
        default="",
        help="Optional path to save raw engine JSON before LLM formatting",
    )
    parser.add_argument(
        "--output-md",
        default="llm_readable_report.md",
        help="Markdown output file path (overwritten each run)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.api_key:
        raise SystemExit("Missing DeepSeek key. Set DEEPSEEK_API_KEY or pass --api-key.")

    result = load_engine_result(args)
    if args.save_json:
        Path(args.save_json).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    prompt = build_user_prompt(result, args.language)
    report = call_deepseek(args.api_key, args.model, prompt)
    output_md_path = Path(args.output_md)
    output_md_path.parent.mkdir(parents=True, exist_ok=True)
    output_md_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"\n[Saved markdown report to: {output_md_path.resolve()}]")


if __name__ == "__main__":
    main()
