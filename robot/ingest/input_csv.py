from __future__ import annotations

import csv

from dataclasses import dataclass
from typing import TYPE_CHECKING

from robot.core.types import RUC


if TYPE_CHECKING:
    from pathlib import Path
    from queue import Queue


@dataclass
class ReadStats:
    rows_read: int = 0
    valid: int = 0
    ignored: int = 0
    duplicates: int = 0
    skipped: int = 0
    enqueued: int = 0


def enqueue_rucs(
    path: Path,
    task_queue: Queue[RUC | None],
    *,
    dedupe: bool,
    checkpoint: set[str],
) -> ReadStats:
    stats = ReadStats()
    seen: set[str] = set()

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

            normalized = str(ruc)
            if dedupe and normalized in seen:
                stats.duplicates += 1
                continue

            seen.add(normalized)
            stats.valid += 1

            if normalized in checkpoint:
                stats.skipped += 1
                continue

            task_queue.put(ruc)
            stats.enqueued += 1

    return stats
