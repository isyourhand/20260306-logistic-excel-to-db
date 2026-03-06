from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Callable

from logistics_ingest.shared.settings import EXCEL_PATTERNS


def month_folder_name(month_date: date) -> str:
    return month_date.strftime("%Y-%m")


def previous_month(month_date: date) -> date:
    first = month_date.replace(day=1)
    return (first - timedelta(days=1)).replace(day=1)


def determine_watch_dirs(source_root: Path, watch_previous_month: bool) -> list[Path]:
    today = date.today()
    month_names = [month_folder_name(today)]
    if watch_previous_month:
        prev = previous_month(today)
        if month_folder_name(prev) not in month_names:
            month_names.append(month_folder_name(prev))

    watch_dirs: list[Path] = []
    for month_name in month_names:
        path = source_root / month_name
        if path.exists() and path.is_dir():
            watch_dirs.append(path)
    return watch_dirs


def iter_candidate_files(watch_dirs: list[Path], *, filename_filter: Callable[[str], bool] | None = None) -> list[Path]:
    files: dict[str, Path] = {}
    for watch_dir in watch_dirs:
        for pattern in EXCEL_PATTERNS:
            for path in watch_dir.glob(pattern):
                if not path.is_file() or path.name.startswith("~$"):
                    continue
                if filename_filter is not None and not filename_filter(path.name):
                    continue
                files[str(path.resolve())] = path.resolve()
    return sorted(files.values(), key=lambda p: (str(p.parent).lower(), p.name.lower()))


def scan_candidate_files(
    source_root: Path,
    watch_previous_month: bool = True,
    *,
    filename_filter: Callable[[str], bool] | None = None,
) -> list[Path]:
    watch_dirs = determine_watch_dirs(source_root, watch_previous_month)
    return iter_candidate_files(watch_dirs, filename_filter=filename_filter)


__all__ = ["determine_watch_dirs", "iter_candidate_files", "scan_candidate_files"]
