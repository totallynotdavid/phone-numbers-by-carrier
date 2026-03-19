from __future__ import annotations

import logging
import sys
import uuid


def configure_logging(*, debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]


def new_session_id() -> str:
    return uuid.uuid4().hex[:10]


def kv(**fields: object) -> str:
    parts: list[str] = []
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    return " ".join(parts)
