from __future__ import annotations

import logging
import time

from typing import TYPE_CHECKING, Any

from robot.domain.types import RUC, CarrierCount
from robot.obs.events import TOKEN_GENERATED
from robot.obs.logging import kv
from robot.providers.osiptel_http import OsiptelHttpClient, OsiptelResponse, PageRequest


if TYPE_CHECKING:
    from robot.providers.osiptel_browser import BrowserSession


logger = logging.getLogger(__name__)


def count_carrier_lines(
    *,
    session: BrowserSession,
    ruc: RUC,
    page_size: int,
) -> tuple[int, tuple[CarrierCount, ...]]:
    user_agent = session.user_agent()
    cookie_header = session.cookie_header()

    total: int | None = None
    start = 0
    draw = 2
    counts: dict[str, int] = {}

    with OsiptelHttpClient(
        proxy=session.proxy_config,
        user_agent=user_agent,
        cookie_header=cookie_header,
    ) as client:
        while True:
            token_started = time.perf_counter()
            token = session.generate_token()
            logger.info(
                "%s %s",
                TOKEN_GENERATED,
                kv(
                    session_id=session.session_id,
                    proxy_id=session.proxy_id,
                    elapsed_ms=int((time.perf_counter() - token_started) * 1000),
                    token_len=len(token),
                ),
            )
            payload = client.fetch(
                PageRequest(
                    ruc=str(ruc),
                    token=token,
                    draw=draw,
                    start=start,
                    length=page_size,
                )
            )

            if total is None:
                total = _total_records(payload)

            rows = payload.get("aaData") or []
            if not isinstance(rows, list):
                rows = []
            for carrier in _carrier_counts(rows):
                counts[carrier.carrier] = counts.get(carrier.carrier, 0) + carrier.lines

            if total == 0 or not rows:
                break

            start += len(rows)
            draw += 1
            if start >= total:
                break

    carrier_rows = tuple(
        CarrierCount(carrier=name, lines=lines)
        for name, lines in sorted(counts.items())
    )
    return total or 0, carrier_rows


def _total_records(payload: OsiptelResponse) -> int:
    value = payload.get("iTotalRecords")
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return 0


def _carrier_counts(rows: list[Any]) -> tuple[CarrierCount, ...]:
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, list):
            continue
        carrier = _as_text(_pick(row, 3))
        if not carrier:
            continue
        counts[carrier] = counts.get(carrier, 0) + 1
    return tuple(
        CarrierCount(carrier=name, lines=lines) for name, lines in counts.items()
    )


def _pick(row: list[Any], idx: int) -> Any:
    if idx < 0 or idx >= len(row):
        return ""
    return row[idx]


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    return ""
