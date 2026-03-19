from __future__ import annotations

import csv
import threading

from typing import TYPE_CHECKING

from robot.domain import RUC, CarrierLines, Status


if TYPE_CHECKING:
    from pathlib import Path

    from robot.domain import Result


SUCCESS_HEADERS = ["ruc", "carrier", "lines", "total_lines"]
ERROR_HEADERS = ["ruc", "error_code", "error_detail"]


def load_checkpoint(path: Path) -> set[str]:
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

    def write(self, result: Result) -> None:
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
    result: Result,
) -> tuple[list[list[str | int]], list[str] | None]:
    if result.status == Status.FAILED:
        return [], [str(result.ruc), result.error_code, result.error_detail]

    rows: list[list[str | int]] = []
    carriers = result.carrier_lines or (
        CarrierLines(carrier="unknown", lines=result.total_lines),
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
