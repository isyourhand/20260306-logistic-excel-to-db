from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from logistics_ingest.app.header_parser import (
    DESTINATION_PLACEHOLDERS,
    expand_header_row,
    find_col,
    find_last_col,
    flatten_row_text,
    is_valid_data_row,
    normalize_text,
    parse_weight_range,
)
from logistics_ingest.app.normalize_parser_constants import NOTE_COL_HINTS


@dataclass
class ShenzhenKGMatrixSection:
    group_row_idx: int
    title_row_idx: int
    unit_row_idx: int
    next_header_idx: int
    row_channel_col: int
    destination_col: int
    transit_col: int | None
    note_col: int | None
    extra_note_cols: list[int]
    weight_cols: dict[int, tuple[Decimal, Decimal | None]]
    tax_by_col: dict[int, bool | None]


@dataclass
class MultiOriginSZSection:
    group_row_idx: int
    title_row_idx: int
    unit_row_idx: int
    next_header_idx: int
    row_channel_col: int
    destination_col: int
    price_col: int
    weight_range: tuple[Decimal, Decimal | None]
    transit_col: int | None
    note_col: int | None
    extra_note_cols: list[int]


@dataclass
class MirroredRouteBlock:
    route_title: str
    destination_col: int
    transit_col: int | None
    weight_cols: dict[int, tuple[Decimal, Decimal | None]]


@dataclass
class MirroredRouteSection:
    title_row_idx: int
    unit_row_idx: int
    next_header_idx: int
    blocks: list[MirroredRouteBlock]


def looks_like_shenzhen_kg_matrix_header(rows: list[list[str]], idx: int) -> bool:
    if idx + 2 >= len(rows):
        return False
    group_row = flatten_row_text(rows[idx])
    title_row = flatten_row_text(rows[idx + 1])
    unit_row = rows[idx + 2]
    if "收货仓库" not in group_row or "深圳" not in group_row:
        return False
    if "产品名称" not in title_row or "仓库代码" not in title_row:
        return False
    return any(parse_weight_range(normalize_text(cell)) is not None for cell in unit_row)


def looks_like_multi_origin_sz_matrix_header(rows: list[list[str]], idx: int) -> bool:
    if idx + 2 >= len(rows):
        return False
    group_row = flatten_row_text(rows[idx])
    title_row = flatten_row_text(rows[idx + 1])
    unit_row = flatten_row_text(rows[idx + 2])

    if "渠道名称" not in group_row or "仓库代码" not in group_row:
        return False
    if "深圳/东莞/中山" not in group_row or "目的港" not in group_row:
        return False
    if "包税" not in title_row or "不包税" not in title_row or "按方包税" not in title_row:
        return False
    if "12KG+" not in unit_row or "1CBM+" not in unit_row:
        return False
    return True


def dominant_route_title(tokens: list[str]) -> str | None:
    clean_tokens = [t for t in tokens if t and t not in {"航线名称", "参考时效"}]
    if not clean_tokens:
        return None
    counts: dict[str, int] = {}
    ordered: list[str] = []
    for token in clean_tokens:
        if token not in counts:
            counts[token] = 0
            ordered.append(token)
        counts[token] += 1
    ordered.sort(key=lambda token: (-counts[token], clean_tokens.index(token)))
    title = ordered[0]
    return title if len(title) >= 4 else None


def find_mirrored_route_block_starts(title_row: list[str]) -> list[int]:
    starts: list[int] = []
    for idx, cell in enumerate(title_row):
        if "航线名称" in normalize_text(cell):
            starts.append(idx)
    return starts


def looks_like_mirrored_route_header(rows: list[list[str]], idx: int) -> bool:
    if idx + 1 >= len(rows):
        return False

    title_row = rows[idx]
    unit_row = rows[idx + 1]
    starts = find_mirrored_route_block_starts(title_row)
    if len(starts) < 2:
        return False

    width = max(len(title_row), len(unit_row))
    for pos, start_col in enumerate(starts):
        end_col = starts[pos + 1] if pos + 1 < len(starts) else width
        title_tokens = [
            normalize_text(title_row[col])
            for col in range(start_col, min(end_col, len(title_row)))
            if normalize_text(title_row[col])
        ]
        if dominant_route_title(title_tokens) is None:
            return False

        if not any("参考时效" in normalize_text(title_row[col]) for col in range(start_col, min(end_col, len(title_row)))):
            return False

        local_weight_cols = 0
        local_has_dest = False
        for col in range(start_col, min(end_col, len(unit_row))):
            cell = normalize_text(unit_row[col])
            if "分区" in cell:
                local_has_dest = True
            if parse_weight_range(cell) is not None:
                local_weight_cols += 1
        if not local_has_dest or local_weight_cols < 2:
            return False

    return True


def build_shenzhen_kg_matrix_sections(rows: list[list[str]]) -> list[ShenzhenKGMatrixSection]:
    start_indices: list[int] = []
    idx = 0
    while idx < len(rows):
        if looks_like_shenzhen_kg_matrix_header(rows, idx):
            start_indices.append(idx)
            idx += 3
            continue
        idx += 1

    sections: list[ShenzhenKGMatrixSection] = []
    for pos, start_idx in enumerate(start_indices):
        group_row = rows[start_idx]
        title_row = rows[start_idx + 1]
        unit_row = rows[start_idx + 2]
        next_header_idx = start_indices[pos + 1] if pos + 1 < len(start_indices) else len(rows)

        width = max(len(group_row), len(title_row), len(unit_row))
        expanded_groups = expand_header_row(group_row, width)
        expanded_titles = expand_header_row(title_row, width)
        expanded_units = expand_header_row(unit_row, width)

        weight_cols: dict[int, tuple[Decimal, Decimal | None]] = {}
        tax_by_col: dict[int, bool | None] = {}
        for col in range(width):
            unit_text = expanded_units[col]
            tax_text = expanded_titles[col]
            group_text = expanded_groups[col]
            parsed = parse_weight_range(unit_text)
            if parsed is None:
                continue
            if "深圳" not in group_text:
                continue
            if "包税" not in tax_text or "不包税" in tax_text or "按方" in tax_text:
                continue
            weight_cols[col] = parsed
            tax_by_col[col] = True

        if not weight_cols:
            continue

        row_channel_col = find_col(title_row, ("产品名称",)) or 0
        destination_col = find_col(title_row, ("仓库代码",))
        if destination_col is None:
            continue

        transit_col = find_col(group_row, ("参考时效", "提取", "签收"))
        note_col = find_last_col(group_row, NOTE_COL_HINTS)
        extra_note_candidates = [
            find_col(group_row, ("船司",)),
            find_col(group_row, ("船期",)),
            find_col(group_row, ("目的港",)),
            find_col(group_row, ("赔偿时效", "理赔时效")),
        ]
        extra_note_cols = [
            col
            for col in extra_note_candidates
            if col is not None and col != transit_col and col != note_col
        ]

        sections.append(
            ShenzhenKGMatrixSection(
                group_row_idx=start_idx,
                title_row_idx=start_idx + 1,
                unit_row_idx=start_idx + 2,
                next_header_idx=next_header_idx,
                row_channel_col=row_channel_col,
                destination_col=destination_col,
                transit_col=transit_col,
                note_col=note_col,
                extra_note_cols=extra_note_cols,
                weight_cols=weight_cols,
                tax_by_col=tax_by_col,
            )
        )
    return sections


def build_multi_origin_sz_sections(rows: list[list[str]]) -> list[MultiOriginSZSection]:
    start_indices: list[int] = []
    idx = 0
    while idx < len(rows):
        if looks_like_multi_origin_sz_matrix_header(rows, idx):
            start_indices.append(idx)
            idx += 3
            continue
        idx += 1

    sections: list[MultiOriginSZSection] = []
    for pos, start_idx in enumerate(start_indices):
        group_row = rows[start_idx]
        title_row = rows[start_idx + 1]
        unit_row = rows[start_idx + 2]
        next_header_idx = start_indices[pos + 1] if pos + 1 < len(start_indices) else len(rows)

        width = max(len(group_row), len(title_row), len(unit_row))
        expanded_groups = expand_header_row(group_row, width)
        expanded_titles = expand_header_row(title_row, width)
        expanded_units = expand_header_row(unit_row, width)

        price_col: int | None = None
        weight_range: tuple[Decimal, Decimal | None] | None = None
        for col in range(width):
            group_text = expanded_groups[col]
            title_text = expanded_titles[col]
            unit_text = expanded_units[col]
            parsed = parse_weight_range(unit_text)
            if parsed is None:
                continue
            if "深圳" not in group_text:
                continue
            if "包税" not in title_text or "不包税" in title_text or "按方" in title_text:
                continue
            if parsed != (Decimal("12"), None):
                continue
            price_col = col
            weight_range = parsed
            break

        if price_col is None or weight_range is None:
            continue

        row_channel_col = find_col(group_row, ("渠道名称",)) or 0
        destination_col = find_col(group_row, ("仓库代码",))
        if destination_col is None:
            continue

        transit_col = find_col(group_row, ("参考时效", "入仓参考时效", "提取", "签收"))
        note_col = find_last_col(group_row, ("备注",))
        if note_col is None:
            note_col = find_last_col(group_row, ("赔偿标准",))

        extra_note_cols: list[int] = []
        note_hints = ("目的港", "船期", "截单时间", "赔偿时效", "理赔时效", "赔偿标准", "备注")
        for col in range(width):
            if col in {row_channel_col, destination_col, price_col, transit_col, note_col}:
                continue
            cell_text = normalize_text(group_row[col]) if col < len(group_row) else ""
            if any(h in cell_text for h in note_hints):
                extra_note_cols.append(col)

        sections.append(
            MultiOriginSZSection(
                group_row_idx=start_idx,
                title_row_idx=start_idx + 1,
                unit_row_idx=start_idx + 2,
                next_header_idx=next_header_idx,
                row_channel_col=row_channel_col,
                destination_col=destination_col,
                price_col=price_col,
                weight_range=weight_range,
                transit_col=transit_col,
                note_col=note_col,
                extra_note_cols=extra_note_cols,
            )
        )

    return sections


def build_mirrored_route_sections(rows: list[list[str]]) -> list[MirroredRouteSection]:
    start_indices: list[int] = []
    idx = 0
    while idx < len(rows):
        if looks_like_mirrored_route_header(rows, idx):
            start_indices.append(idx)
            idx += 2
            continue
        idx += 1

    sections: list[MirroredRouteSection] = []
    for pos, start_idx in enumerate(start_indices):
        title_row = rows[start_idx]
        unit_row = rows[start_idx + 1]
        next_header_idx = start_indices[pos + 1] if pos + 1 < len(start_indices) else len(rows)
        width = max(len(title_row), len(unit_row))
        starts = find_mirrored_route_block_starts(title_row)
        blocks: list[MirroredRouteBlock] = []

        for block_pos, start_col in enumerate(starts):
            end_col = starts[block_pos + 1] if block_pos + 1 < len(starts) else width
            title_tokens = [
                normalize_text(title_row[col])
                for col in range(start_col, min(end_col, len(title_row)))
                if normalize_text(title_row[col])
            ]
            route_title = dominant_route_title(title_tokens)
            if not route_title:
                continue

            destination_col: int | None = None
            transit_col: int | None = None
            weight_cols: dict[int, tuple[Decimal, Decimal | None]] = {}

            for col in range(start_col, min(end_col, len(title_row))):
                if "参考时效" in normalize_text(title_row[col]):
                    transit_col = col
                    break

            for col in range(start_col, min(end_col, len(unit_row))):
                cell = normalize_text(unit_row[col])
                if "分区" in cell:
                    destination_col = col
                parsed = parse_weight_range(cell)
                if parsed is not None:
                    weight_cols[col] = parsed

            if destination_col is None or not weight_cols:
                continue

            blocks.append(
                MirroredRouteBlock(
                    route_title=route_title,
                    destination_col=destination_col,
                    transit_col=transit_col,
                    weight_cols=weight_cols,
                )
            )

        if not blocks:
            continue

        data_rows = 0
        for ridx in range(start_idx + 2, next_header_idx):
            row = rows[ridx]
            for block in blocks:
                destination_text = normalize_text(row[block.destination_col]) if block.destination_col < len(row) else ""
                if destination_text in DESTINATION_PLACEHOLDERS:
                    destination_text = ""
                if not destination_text or not is_valid_data_row(destination_text):
                    continue
                if any(col < len(row) and normalize_text(row[col]) not in DESTINATION_PLACEHOLDERS for col in block.weight_cols):
                    data_rows += 1
                    break
            if data_rows:
                break
        if data_rows == 0:
            continue

        sections.append(
            MirroredRouteSection(
                title_row_idx=start_idx,
                unit_row_idx=start_idx + 1,
                next_header_idx=next_header_idx,
                blocks=blocks,
            )
        )

    return sections


__all__ = [
    "MirroredRouteBlock",
    "MirroredRouteSection",
    "MultiOriginSZSection",
    "ShenzhenKGMatrixSection",
    "build_mirrored_route_sections",
    "build_multi_origin_sz_sections",
    "build_shenzhen_kg_matrix_sections",
    "looks_like_mirrored_route_header",
    "looks_like_multi_origin_sz_matrix_header",
    "looks_like_shenzhen_kg_matrix_header",
]
