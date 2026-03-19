from __future__ import annotations

import json

from typing import TYPE_CHECKING, Any

from robot.core.errors import BanSignalError, ParseError, TransientTransportError
from robot.observability import timed
from robot.providers.osiptel.request_payload import build_payload


if TYPE_CHECKING:
    from seleniumbase import SB  # type: ignore[import-untyped]


def fetch_page(
    sb: SB,
    *,
    ruc: str,
    token: str,
    draw: int,
    start: int,
    length: int,
) -> tuple[dict[str, Any], int, int]:
    body_json = json.dumps(
        build_payload(
            ruc=ruc,
            token=token,
            draw=draw,
            start=start,
            length=length,
        )
    )
    script = f"""(() => {{
  const data = {body_json};
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(data)) {{
    params.append(k, v);
  }}
  const xhr = new XMLHttpRequest();
  xhr.open('POST', '/Consultas/GetAllCabeceraConsulta/', false);
  xhr.setRequestHeader('Accept', '*/*');
  xhr.setRequestHeader('Cache-Control', 'no-cache');
  xhr.setRequestHeader('Pragma', 'no-cache');
  xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
  xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded; charset=UTF-8');
  xhr.send(params.toString());
  return JSON.stringify({{ status: xhr.status, body: xhr.responseText || '' }});
}})()"""

    with timed() as timer:
        raw = sb.execute_script(script)

    if not isinstance(raw, str):
        msg = "osiptel request returned non-string payload"
        raise ParseError(msg)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        msg = "osiptel request wrapper returned invalid json"
        raise ParseError(msg) from exc

    status = int(parsed.get("status", 0) or 0)
    body = parsed.get("body", "")

    if status >= 500:
        msg = f"osiptel request failed status={status}"
        raise BanSignalError(msg)
    if status != 200:
        msg = f"osiptel request failed status={status}"
        raise TransientTransportError(msg)
    if not isinstance(body, str) or not body.strip():
        msg = "osiptel response body empty"
        raise BanSignalError(msg)

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        msg = "osiptel response is not valid json"
        raise ParseError(msg) from exc

    if not isinstance(payload, dict):
        msg = "osiptel response json is not an object"
        raise ParseError(msg)

    return payload, status, timer.elapsed_ms
