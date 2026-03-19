from __future__ import annotations

import time

from typing import TYPE_CHECKING, Any

from robot.core.errors import TransientTransportError
from robot.observability import timed


if TYPE_CHECKING:
    from seleniumbase import SB  # type: ignore[import-untyped]


_STATE_EXPR = """(() => ({
  ready: document.readyState || '',
  href: location.href || '',
  title: document.title || '',
  scripts: document.scripts ? document.scripts.length : -1,
  gc: typeof window.grecaptcha,
  key: (document.querySelector('#hiddenRecaptchaKey')||{}).value || ''
}))()"""


def wait_until_ready(sb: SB, *, timeout_s: float = 25.0, poll_s: float = 0.25) -> int:
    deadline = time.monotonic() + timeout_s
    last_state: dict[str, Any] = {}

    with timed() as timer:
        while time.monotonic() < deadline:
            state = sb.execute_script(_STATE_EXPR) or {}
            if isinstance(state, dict):
                last_state = state
            if (
                state.get("scripts", 0) >= 20
                and state.get("gc") == "object"
                and state.get("key")
            ):
                return timer.elapsed_ms
            time.sleep(poll_s)

    msg = (
        "osiptel page not ready "
        f"ready={last_state.get('ready', '')} "
        f"href={last_state.get('href', '')} "
        f"title={last_state.get('title', '')} "
        f"scripts={last_state.get('scripts', '')} "
        f"gc={last_state.get('gc', '')} "
        f"has_key={bool(last_state.get('key'))}"
    )
    raise TransientTransportError(msg)
