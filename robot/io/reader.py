from __future__ import annotations

import csv

from dataclasses import dataclass
from typing import TYPE_CHECKING

from robot.domain import RUC


if TYPE_CHECKING:
    from pathlib import Path


@dataclass
class ReadStats:
    rows_read: int = 0
    valid: int = 0
    ignored: int = 0
    duplicates: int = 0


def read_rucs(path: Path, *, dedupe: bool = True) -> tuple[list[RUC], ReadStats]:
    stats = ReadStats()
    seen: set[str] = set()
    rucs: list[RUC] = []

    with path.open(newline="", encoding="utf-8-sig") as file_obj:
        for row in csv.reader(file_obj):
            stats.rows_read += 1
            if not row or not row[0].strip():
                stats.ignored += 1
                continue
            try:
                ruc = RUC(row[0])
            except ValueError:
                stats.ignored += 1
                continue
            dedupe_key = str(ruc)
            if dedupe and dedupe_key in seen:
                stats.duplicates += 1
                continue
            seen.add(dedupe_key)
            stats.valid += 1
            rucs.append(ruc)

    return rucs, stats
