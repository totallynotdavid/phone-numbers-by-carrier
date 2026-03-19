from __future__ import annotations

import csv
import threading

from typing import TYPE_CHECKING

from robot.domain import CarrierLines, Status


if TYPE_CHECKING:
    from pathlib import Path

    from robot.domain import Result


_HEADERS = {
    "counts-only": ["ruc", "carrier", "lines", "total_lines"],
    "detailed": [
        "ruc",
        "carrier",
        "lines",
        "total_lines",
        "status",
        "error_code",
        "error_detail",
    ],
}


def load_checkpoint(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()
    with path.open(newline="", encoding="utf-8") as file_obj:
        rows = list(csv.reader(file_obj))
    return {row[0] for row in rows[1:] if row}


class OutputWriter:
    def __init__(self, path: Path, mode: str, *, resume: bool) -> None:
        if mode not in _HEADERS:
            msg = f"unknown output mode {mode!r}"
            raise ValueError(msg)
        self._mode = mode
        self._lock = threading.Lock()
        is_new = not resume or not path.exists() or path.stat().st_size == 0
        open_mode = "a" if resume else "w"
        self._file = path.open(open_mode, newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        if is_new:
            self._writer.writerow(_HEADERS[mode])
            self._file.flush()

    def write(self, result: Result) -> None:
        rows = _rows_for_result(result, mode=self._mode)
        with self._lock:
            self._writer.writerows(rows)
            self._file.flush()

    def close(self) -> None:
        with self._lock:
            self._file.close()

    def __enter__(self) -> OutputWriter:
        return self

    def __exit__(self, *_) -> None:
        self.close()


def _rows_for_result(result: Result, *, mode: str) -> list[list[str | int]]:
    if result.status == Status.FAILED:
        if mode == "counts-only":
            return []
        return [
            [
                str(result.ruc),
                "",
                "",
                result.total_lines,
                result.status.value,
                result.error_code,
                result.error_detail,
            ]
        ]

    rows: list[list[str | int]] = []
    carriers = result.carrier_lines or (
        CarrierLines(carrier="unknown", lines=result.total_lines),
    )
    for carrier_item in carriers:
        carrier, lines = carrier_item.carrier, carrier_item.lines
        base_row: list[str | int] = [
            str(result.ruc),
            carrier,
            lines,
            result.total_lines,
        ]
        if mode == "detailed":
            rows.append(
                [
                    *base_row,
                    result.status.value,
                    result.error_code,
                    result.error_detail,
                ]
            )
        else:
            rows.append(base_row)
    return rows
