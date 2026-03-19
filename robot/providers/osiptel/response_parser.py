from __future__ import annotations

import operator

from typing import Any

from robot.core.types import CarrierCount


def total_records(payload: dict[str, Any]) -> int:
    return int(payload.get("iTotalRecords", 0) or 0)


def carrier_counts(payload_rows: object) -> tuple[CarrierCount, ...]:
    counts: dict[str, int] = {}
    if not isinstance(payload_rows, list):
        return ()

    for row in payload_rows:
        carrier = _extract_carrier_name(row)
        counts[carrier] = counts.get(carrier, 0) + 1

    return tuple(
        CarrierCount(carrier=name, lines=lines)
        for name, lines in sorted(counts.items(), key=operator.itemgetter(0))
    )


def _extract_carrier_name(row: object) -> str:
    if isinstance(row, dict):
        value = row.get("operador")
        if isinstance(value, str):
            normalized = _normalize_carrier(value)
            if normalized:
                return normalized

        for key, value in row.items():
            if "operador" not in key.casefold():
                continue
            if isinstance(value, str):
                normalized = _normalize_carrier(value)
                if normalized:
                    return normalized

    if isinstance(row, list) and len(row) >= 4 and isinstance(row[3], str):
        normalized = _normalize_carrier(row[3])
        if normalized:
            return normalized

    return "unknown"


def _normalize_carrier(value: str) -> str:
    normalized = " ".join(value.strip().split())
    return normalized or "unknown"
