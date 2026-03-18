from __future__ import annotations

import csv
import threading

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pathlib import Path

    from robot.domain import Result


_HEADERS = {
    "counts-only": ["ruc", "registered_lines"],
    "detailed": ["ruc", "registered_lines", "status", "error_code", "error_detail"],
}


def load_checkpoint(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size == 0:
        return set()
    with path.open(newline="", encoding="utf-8") as file_obj:
        rows = list(csv.reader(file_obj))
    return {row[0] for row in rows[1:] if row}


class OutputWriter:
    def __init__(self, path: Path, mode: str) -> None:
        if mode not in _HEADERS:
            msg = f"unknown output mode {mode!r}"
            raise ValueError(msg)
        self._mode = mode
        self._lock = threading.Lock()
        is_new = not path.exists() or path.stat().st_size == 0
        self._file = path.open("a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        if is_new:
            self._writer.writerow(_HEADERS[mode])
            self._file.flush()

    def write(self, result: Result) -> None:
        if self._mode == "detailed":
            row = [
                result.ruc,
                result.registered_lines,
                result.status.value,
                result.error_code,
                result.error_detail,
            ]
        else:
            row = [result.ruc, result.registered_lines]
        with self._lock:
            self._writer.writerow(row)
            self._file.flush()

    def close(self) -> None:
        with self._lock:
            self._file.close()

    def __enter__(self) -> OutputWriter:
        return self

    def __exit__(self, *_) -> None:
        self.close()
