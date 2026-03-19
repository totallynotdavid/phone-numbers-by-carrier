from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from robot.domain.types import LookupResult


@dataclass(frozen=True)
class ResultMessage:
    result: LookupResult


@dataclass(frozen=True)
class WorkerDoneMessage:
    worker_id: int
    processed: int
    succeeded: int
    failed: int
