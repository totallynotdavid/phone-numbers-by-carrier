from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class RunConfig:
    input_csv: Path
    output_csv: Path
    page_size: int
    workers: int
    dedupe: bool
    debug: bool
    session_budget: int
    wait_min_s: float
    wait_max_s: float
    same_session_retries: int
    ban_cooldown_s: float
    env_file: str
