from __future__ import annotations

import re


SKIP_SHEET_KEYWORDS = (
    "目录",
    "说明",
    "查询",
    "教程",
    "地址表",
    "须知",
    "区别",
    "列表",
    "反倾销",
    "禁运",
    "发票",
    "船期",
)

HEADER_HINTS = ("KG", "公斤", "重量", "计费", "+")
CHANNEL_COL_HINTS = ("渠道", "国家", "区域", "分区", "目的", "仓", "邮编")
ROW_CHANNEL_COL_HINTS = ("渠道名称", "渠道")
DESTINATION_COL_HINTS = ("末端分区", "国家/仓库代码", "国家/地区", "国家", "区域", "分区", "目的地", "目的", "仓库", "邮编", "仓")
TRANSIT_COL_HINTS = ("时效", "提取", "签收")
NOTE_COL_HINTS = ("备注", "说明")
ROW_SKIP_KEYWORDS = (
    "备注",
    "说明",
    "赔偿",
    "拒收",
    "提醒",
    "收费",
    "附加费",
    "报关",
    "清关",
    "发货",
    "要求",
    "特别",
    "单票",
    "票",
    "住宅",
    "偏远",
    "磁检",
    "商检",
)
CONTEXT_NOISE_TOKENS = {"返回目录"}
CONTEXT_HEADER_TOKENS = {"渠道名称", "国家", "国家/地区", "国家/仓库代码", "时效/备注"}
CHANNEL_TITLE_HINTS = (
    "空派",
    "海派",
    "海卡",
    "空运",
    "海运",
    "专线",
    "渠道",
    "包税",
    "带电",
    "普货",
    "卡派",
    "快递派",
    "直发",
    "定时达",
    "星速达",
)
CONTEXT_PRICING_NOTE_HINTS = ("单价", "/KG", "递延", "自税", "实报实销", "VAT", "EORI")
COMPANY_NAME_HINTS = ("有限公司", "物流", "国际", "集团", "公司")

RANGE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?\s*[-~—–至到]\s*(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?", re.IGNORECASE)
PLUS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?\s*\+", re.IGNORECASE)
LE_RE = re.compile(r"(?:<=|≤)?\s*(\d+(?:\.\d+)?)\s*(?:KG|KGS|公斤)?\s*(?:以下|以内)?", re.IGNORECASE)
PURE_RANGE_CELL_RE = re.compile(r"^\s*\d+(?:\.\d+)?\s*[-~—–至到]\s*\d+(?:\.\d+)?\s*$", re.IGNORECASE)
PURE_PLUS_CELL_RE = re.compile(r"^\s*\d+(?:\.\d+)?\s*\+\s*$", re.IGNORECASE)
PURE_LE_CELL_RE = re.compile(r"^\s*(?:<=|≤)?\s*\d+(?:\.\d+)?\s*(?:以下|以内)?\s*$", re.IGNORECASE)
NUM_RE = re.compile(r"^-?\d+(?:\.\d+)?$")
DECIMAL_RE = re.compile(r"-?\d+(?:\.\d+)?")
STRICT_PRICE_RE = re.compile(
    r"^(?:RMB|USD|\$)?\s*-?\d+(?:\.\d+)?\s*(?:RMB|USD|/KG|元|美元|美金)?$",
    re.IGNORECASE,
)
DAYS_RANGE_RE = re.compile(
    r"(\d{1,3})\s*[-~—–]\s*(\d{1,3})\s*(?:个)?\s*(?:[（(]?\s*(?:自然日|天|工作日)\s*[）)])?",
)
DAYS_SINGLE_RE = re.compile(
    r"(\d{1,3})\s*(?:个)?\s*(?:[（(]?\s*(?:自然日|天|工作日)\s*[）)])",
)
FBA_CODE_RE = re.compile(r"\b[A-Z]{3}\d\b")
EXTENDED_FBA_CODE_RE = re.compile(r"^[A-Z]{2,}(?:-[A-Z]{2,}[A-Z0-9]*)+\d$")
ZIP_PREFIX_RE = re.compile(r"邮编[^0-9A-Z]*([0-9](?:[、,，/\-]\d)*)")
US_ZONE_RE = re.compile(r"(?:美东|美西|美中|美国)")
BRACKET_DIGITS_RE = re.compile(r"[（(]([^）)]{1,30})[）)]")
TRANSPORT_RAIL_HINTS = ("班列", "卡铁", "快铁", "铁路", "中欧", "RAIL", "TRAIN")
TRANSPORT_SEA_HINTS = (
    "海派",
    "海卡",
    "海运",
    "船运",
    "船期",
    "船司",
    "开船",
    "快船",
    "普船",
    "整柜",
    "拼柜",
    "美森",
    "以星",
    "MATSON",
    "COSCO",
    "EMC",
    "直航",
    "QY-OA",
    "卡航",
    "定时达卡派（美转加，OA头程，尾端卡车派送）",
    "定时达快递派（美转加）",
)
TRANSPORT_AIR_HINTS = ("空派", "空运", "航空", "直飞", "航班")
BATTERY_POSITIVE_HINTS = ("纯电", "带电", "电池", "带磁", "磁")
BATTERY_NEGATIVE_HINTS_RE = re.compile(
    r"不(?:接|收|走)?\s*(?:带电|纯电|电池|磁)|"
    r"拒(?:接|收)\s*(?:带电|纯电|电池|磁)|"
    r"禁(?:止|收)\s*(?:带电|纯电|电池|磁)|"
    r"仅(?:限)?\s*普货|"
    r"普货[^，,。;；]{0,16}不(?:接|收|走)?\s*(?:带电|纯电|电池|磁)"
)

NON_WEIGHT_RANGE_HINTS = ("邮编", "时效", "自然日", "工作日", "提取", "开头")


__all__ = [
    "BATTERY_NEGATIVE_HINTS_RE",
    "BATTERY_POSITIVE_HINTS",
    "BRACKET_DIGITS_RE",
    "CHANNEL_COL_HINTS",
    "CHANNEL_TITLE_HINTS",
    "COMPANY_NAME_HINTS",
    "CONTEXT_HEADER_TOKENS",
    "CONTEXT_NOISE_TOKENS",
    "CONTEXT_PRICING_NOTE_HINTS",
    "DAYS_RANGE_RE",
    "DAYS_SINGLE_RE",
    "DECIMAL_RE",
    "DESTINATION_COL_HINTS",
    "EXTENDED_FBA_CODE_RE",
    "FBA_CODE_RE",
    "HEADER_HINTS",
    "LE_RE",
    "NON_WEIGHT_RANGE_HINTS",
    "NOTE_COL_HINTS",
    "NUM_RE",
    "PLUS_RE",
    "PURE_LE_CELL_RE",
    "PURE_PLUS_CELL_RE",
    "PURE_RANGE_CELL_RE",
    "RANGE_RE",
    "ROW_CHANNEL_COL_HINTS",
    "ROW_SKIP_KEYWORDS",
    "SKIP_SHEET_KEYWORDS",
    "STRICT_PRICE_RE",
    "TRANSIT_COL_HINTS",
    "TRANSPORT_AIR_HINTS",
    "TRANSPORT_RAIL_HINTS",
    "TRANSPORT_SEA_HINTS",
    "US_ZONE_RE",
    "ZIP_PREFIX_RE",
]
