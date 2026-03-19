from __future__ import annotations

import time

from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass
class OpTimer:
    _start: float
    _stop: float | None = None

    @property
    def elapsed_ms(self) -> int:
        end = self._stop if self._stop is not None else time.perf_counter()
        return int((end - self._start) * 1000)


@contextmanager
def timed() -> Iterator[OpTimer]:
    timer = OpTimer(_start=time.perf_counter())
    try:
        yield timer
    finally:
        timer._stop = time.perf_counter()
