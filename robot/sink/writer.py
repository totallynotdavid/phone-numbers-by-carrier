from __future__ import annotations

import csv
import threading

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from robot.core.types import CarrierCount, LookupResult, Status


if TYPE_CHECKING:
    from pathlib import Path


SUCCESS_HEADERS = ["ruc", "carrier", "lines", "total_lines"]
ERROR_HEADERS = [
    "ruc",
    "error_code",
    "error_detail",
    "attempt",
    "session_id",
    "proxy_id",
    "timestamp",
]


class OutputWriter:
    def __init__(self, path: Path) -> None:
        self._lock = threading.Lock()

        self._success_file = path.open("a", newline="", encoding="utf-8")
        self._success_writer = csv.writer(self._success_file)
        if path.stat().st_size == 0:
            self._success_writer.writerow(SUCCESS_HEADERS)
            self._success_file.flush()

        error_path = path.with_suffix(".errors.csv")
        self._error_file = error_path.open("a", newline="", encoding="utf-8")
        self._error_writer = csv.writer(self._error_file)
        if error_path.stat().st_size == 0:
            self._error_writer.writerow(ERROR_HEADERS)
            self._error_file.flush()

    def write(self, result: LookupResult) -> None:
        success_rows, error_row = _rows_for_result(result)
        with self._lock:
            if success_rows:
                self._success_writer.writerows(success_rows)
                self._success_file.flush()
            if error_row is not None:
                self._error_writer.writerow(error_row)
                self._error_file.flush()

    def close(self) -> None:
        with self._lock:
            self._success_file.close()
            self._error_file.close()

    def __enter__(self) -> OutputWriter:
        return self

    def __exit__(self, *_) -> None:
        self.close()


def _rows_for_result(
    result: LookupResult,
) -> tuple[list[list[str | int]], list[str] | None]:
    if result.status == Status.FAILED:
        return [], [
            str(result.ruc),
            result.error_code,
            result.error_detail,
            str(result.attempt),
            result.session_id,
            result.proxy_id,
            datetime.now(UTC).isoformat(),
        ]

    rows: list[list[str | int]] = []
    carriers = result.carrier_counts or (
        CarrierCount(carrier="unknown", lines=result.total_lines),
    )
    for carrier_item in carriers:
        rows.append(
            [
                str(result.ruc),
                carrier_item.carrier,
                carrier_item.lines,
                result.total_lines,
            ]
        )
    return rows, None
