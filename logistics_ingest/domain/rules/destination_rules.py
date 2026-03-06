from __future__ import annotations

import re

from logistics_ingest.shared.text_utils import normalize_text

FBA_CODE_RE = re.compile(r"\b[A-Z]{3}\d\b")
EXTENDED_FBA_CODE_RE = re.compile(r"^[A-Z]{2,}(?:-[A-Z]{2,}[A-Z0-9]*)+\d$")
ZIP_PREFIX_RE = re.compile(r"邮编[^0-9A-Z]*([0-9](?:[、,，/\-]\d)*)")
ZIP_PREFIX_LEADING_RE = re.compile(r"(?P<range>\d{1,3}(?:\s*[-/,，、]\s*\d{1,3})*)\s*邮编")
ZIP_PREFIX_TRAILING_RE = re.compile(r"邮编[^0-9A-Z]*(?P<range>\d{1,3}(?:\s*[-/,，、]\s*\d{1,3})*)")
US_ZONE_RE = re.compile(r"(?:美东|美西|美中|美国)")
BRACKET_DIGITS_RE = re.compile(r"[（(]([^）)]{1,30})[）)]")
GENERIC_DEST_CODE_RE = re.compile(r"^[A-Z]{3,6}\d?$")
SERVICE_ENDPOINT_HINTS = ("自提", "一件代发", "中转", "海外仓")

COUNTRY_ALIASES = [
    ("US", ("美国", "UNITED STATES", "USA", "AMERICA")),
    ("CA", ("加拿大", "CANADA")),
    ("UK", ("英国", "UNITED KINGDOM", "ENGLAND", "BRITAIN", "GREAT BRITAIN")),
    ("DE", ("德国", "GERMANY")),
    ("FR", ("法国", "FRANCE")),
    ("IT", ("意大利", "ITALY")),
    ("ES", ("西班牙", "SPAIN")),
    ("NL", ("荷兰", "NETHERLANDS")),
    ("PL", ("波兰", "POLAND")),
    ("CZ", ("捷克", "CZECH")),
    ("HU", ("匈牙利", "HUNGARY")),
    ("BE", ("比利时", "BELGIUM")),
    ("DK", ("丹麦", "DENMARK")),
    ("FI", ("芬兰", "FINLAND")),
    ("IE", ("爱尔兰", "IRELAND")),
    ("GR", ("希腊", "GREECE")),
    ("AU", ("澳大利亚", "AUSTRALIA", "澳洲")),
    ("JP", ("日本", "JAPAN")),
]

BROAD_DESTINATION_HINTS = [
    ("EU", ("欧盟", "欧洲", "EUROPE")),
    ("US", ("美国", "美线", "美东", "美西", "美中", "UNITED STATES", "USA", "AMERICA")),
    ("CA", ("加拿大", "CANADA")),
    ("UK", ("英国", "UNITED KINGDOM", "ENGLAND", "BRITAIN")),
    ("AU", ("澳大利亚", "AUSTRALIA", "澳洲")),
    ("JP", ("日本", "JAPAN")),
]

EU_COUNTRY_CODES = {"DE", "FR", "IT", "ES", "NL", "PL", "CZ", "HU", "BE", "DK", "FI", "IE", "GR"}


def normalize_destination_text(text: str) -> str:
    t = normalize_text(text)
    t = t.replace("\u3000", " ")
    t = re.sub(r"[，、/;；|\\\\]+", ",", t)
    t = re.sub(r"\s*,\s*", ",", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip(" ,")


def split_destination_tokens(text: str) -> list[str]:
    normalized = normalize_destination_text(text)
    if not normalized:
        return []
    return [x.strip() for x in normalized.split(",") if x.strip()]


def extract_fba_like_codes(text: str) -> list[str]:
    normalized = normalize_destination_text(text).upper()
    if not normalized:
        return []

    standard_codes = list(dict.fromkeys(FBA_CODE_RE.findall(normalized)))
    if standard_codes:
        return standard_codes

    extended_codes: list[str] = []
    for token in split_destination_tokens(normalized):
        if EXTENDED_FBA_CODE_RE.match(token) and token not in extended_codes:
            extended_codes.append(token)
    if extended_codes:
        return extended_codes

    tokens = split_destination_tokens(normalized)
    generic_codes = [token for token in tokens if GENERIC_DEST_CODE_RE.fullmatch(token)]
    if generic_codes and len(generic_codes) == len(tokens):
        return list(dict.fromkeys(generic_codes))
    return extended_codes


def extract_zip_prefix_keyword(text: str) -> str | None:
    normalized = normalize_destination_text(text)
    if not normalized:
        return None

    for pattern in (ZIP_PREFIX_LEADING_RE, ZIP_PREFIX_TRAILING_RE):
        match = pattern.search(normalized)
        if not match:
            continue
        parts = re.findall(r"\d{1,3}", match.group("range"))
        if parts:
            return ",".join(parts)

    match = ZIP_PREFIX_RE.search(normalized)
    if match:
        return match.group(1).replace("，", ",").replace("、", ",").replace("/", ",").replace("-", ",")
    return None


def match_country_code(text: str) -> str | None:
    upper = normalize_text(text).upper()
    if not upper:
        return None
    for code, aliases in COUNTRY_ALIASES:
        if any(alias.upper() in upper for alias in aliases):
            return code
    return None


def extract_country_codes(destination_text: str) -> list[str]:
    codes: list[str] = []
    for token in split_destination_tokens(destination_text):
        code = match_country_code(token)
        if code and code not in codes:
            codes.append(code)
    if not codes:
        code = match_country_code(destination_text)
        if code:
            codes.append(code)
    return codes


def parse_zone_digits(text: str) -> str | None:
    normalized = normalize_destination_text(text)
    if not normalized:
        return None
    if not US_ZONE_RE.search(normalized):
        return None

    bracket_match = BRACKET_DIGITS_RE.search(text)
    candidate = bracket_match.group(1) if bracket_match else normalized
    digits = re.findall(r"\d", candidate)
    if not digits:
        digits = re.findall(r"\d", normalized)
    if not digits:
        return None
    uniq_digits = list(dict.fromkeys(digits))
    return ",".join(uniq_digits)


def choose_destination_keyword(destination_text: str) -> str | None:
    text = normalize_destination_text(destination_text)
    codes = extract_fba_like_codes(text)
    if codes:
        return ",".join(codes)
    zip_prefix = extract_zip_prefix_keyword(text)
    if zip_prefix:
        return zip_prefix
    zone_digits = parse_zone_digits(text)
    if zone_digits:
        return zone_digits
    country_codes = extract_country_codes(text)
    if len(country_codes) > 1:
        return ",".join(country_codes)
    return None


def infer_destination_country(*parts: str) -> str | None:
    destination_text = normalize_destination_text(parts[-1]) if parts else ""
    country_codes = extract_country_codes(destination_text)
    if len(country_codes) == 1:
        return country_codes[0]
    if len(country_codes) > 1:
        if all(c in EU_COUNTRY_CODES for c in country_codes):
            return "EU"
        return None

    joined = " ".join([normalize_text(p) for p in parts if p]).upper()
    for code, hints in BROAD_DESTINATION_HINTS:
        if any(h.upper() in joined for h in hints):
            return code
    return None


def infer_destination_scope(destination_text: str, destination_keyword: str | None, destination_country: str | None) -> str:
    text = normalize_destination_text(destination_text)
    if destination_keyword and extract_fba_like_codes(destination_keyword):
        return "fba_code"
    if extract_zip_prefix_keyword(text):
        return "zip_prefix"
    if parse_zone_digits(text):
        return "zip_prefix"
    if any(h in text for h in SERVICE_ENDPOINT_HINTS):
        return "any"
    country_codes = extract_country_codes(text)
    if len(country_codes) > 1:
        return "country_list"
    if len(country_codes) == 1:
        return "country"
    if destination_country:
        return "region" if destination_country == "EU" else "country"
    return "any"

__all__ = [
    "normalize_destination_text",
    "split_destination_tokens",
    "extract_fba_like_codes",
    "extract_country_codes",
    "parse_zone_digits",
    "infer_destination_country",
    "infer_destination_scope",
    "choose_destination_keyword",
]
