from __future__ import annotations

import csv

from typing import TYPE_CHECKING

from robot.domain.types import RUC
from robot.io.writer import SUCCESS_HEADERS


if TYPE_CHECKING:
    from pathlib import Path


def load_completed_rucs(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()

    with path.open(newline="", encoding="utf-8") as file_obj:
        reader = csv.reader(file_obj)
        header = next(reader, [])
        if header != SUCCESS_HEADERS:
            msg = f"invalid output header in {path}: expected {SUCCESS_HEADERS}"
            raise RuntimeError(msg)

        seen: set[str] = set()
        for line_no, row in enumerate(reader, start=2):
            if len(row) != len(SUCCESS_HEADERS):
                msg = (
                    f"invalid output row width in {path}:{line_no}: "
                    f"expected {len(SUCCESS_HEADERS)} columns"
                )
                raise RuntimeError(msg)

            ruc_raw, _, lines_raw, total_raw = row
            try:
                ruc = RUC(ruc_raw)
                lines = int(lines_raw)
                total = int(total_raw)
            except (TypeError, ValueError) as exc:
                msg = f"invalid output row data in {path}:{line_no}"
                raise RuntimeError(msg) from exc

            if lines < 0 or total < 0:
                msg = f"negative values are not allowed in {path}:{line_no}"
                raise RuntimeError(msg)

            seen.add(str(ruc))

    return seen
