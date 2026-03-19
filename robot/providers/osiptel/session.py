from __future__ import annotations

import logging
import time

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from selenium.common.exceptions import WebDriverException
from seleniumbase import SB  # type: ignore[import-untyped]

from robot.core.errors import TransientTransportError
from robot.core.types import RUC, CarrierCount
from robot.observability import kv, new_session_id, timed
from robot.providers.osiptel.captcha import generate_token
from robot.providers.osiptel.page_ready import wait_until_ready
from robot.providers.osiptel.response_parser import carrier_counts, total_records
from robot.providers.osiptel.transport import fetch_page


if TYPE_CHECKING:
    from robot.providers.geonode.sticky_pool import ProxySessionConfig


logger = logging.getLogger(__name__)

HOME_URL = "https://checatuslineas.osiptel.gob.pe/"
_IP_JSONP_URL = "https://api.ipify.org?format=jsonp"


@dataclass(frozen=True)
class OsiptelSessionSettings:
    page_size: int
    chrome_binary: str = ""


class OsiptelSession:
    def __init__(
        self, proxy: ProxySessionConfig, settings: OsiptelSessionSettings
    ) -> None:
        self._proxy = proxy
        self._settings = settings
        self._session_id = new_session_id()
        self._sb_cm: SB | None = None
        self._sb: SB | None = None
        self.egress_ip: str = ""

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def proxy_id(self) -> str:
        return self._proxy.proxy_id

    def open(self) -> None:
        kwargs: dict[str, Any] = {
            "uc": True,
            "headed": True,
            "xvfb": True,
            "proxy": self._proxy.as_selenium_proxy(),
        }
        if self._settings.chrome_binary:
            kwargs["binary_location"] = self._settings.chrome_binary

        try:
            with timed() as timer:
                self._sb_cm = SB(**kwargs)
                self._sb = self._sb_cm.__enter__()
                self._sb.activate_cdp_mode(HOME_URL)
        except WebDriverException as exc:
            msg = f"failed to open browser session: {type(exc).__name__}: {exc}"
            raise TransientTransportError(msg) from exc

        logger.info(
            "session_open %s",
            kv(
                session_id=self._session_id,
                proxy_id=self._proxy.proxy_id,
                elapsed_ms=timer.elapsed_ms,
            ),
        )

        wait_elapsed = wait_until_ready(self._require_sb())
        logger.info(
            "session_wait_ready %s",
            kv(session_id=self._session_id, elapsed_ms=wait_elapsed),
        )

        self.egress_ip = self._resolve_ip()
        logger.info(
            "session_ready %s",
            kv(
                session_id=self._session_id,
                proxy_id=self._proxy.proxy_id,
                egress_ip=self.egress_ip,
            ),
        )

    def close(self) -> None:
        if self._sb_cm is None:
            return
        try:
            self._sb_cm.__exit__(None, None, None)
        finally:
            self._sb_cm = None
            self._sb = None

    def count_carrier_lines(self, ruc: RUC) -> tuple[int, tuple[CarrierCount, ...]]:
        sb = self._require_sb()
        total: int | None = None
        start = 0
        draw = 2
        counts: dict[str, int] = {}

        while True:
            token, token_elapsed = generate_token(sb)
            logger.info(
                "token_generated %s",
                kv(
                    session_id=self._session_id,
                    proxy_id=self._proxy.proxy_id,
                    elapsed_ms=token_elapsed,
                    token_len=len(token),
                ),
            )

            payload, _, fetch_elapsed = fetch_page(
                sb,
                ruc=str(ruc),
                token=token,
                draw=draw,
                start=start,
                length=self._settings.page_size,
            )

            if total is None:
                total = total_records(payload)

            rows = payload.get("aaData") or []
            for carrier in carrier_counts(rows):
                counts[carrier.carrier] = counts.get(carrier.carrier, 0) + carrier.lines

            logger.debug(
                "api_page %s",
                kv(
                    session_id=self._session_id,
                    proxy_id=self._proxy.proxy_id,
                    ruc=ruc,
                    draw=draw,
                    start=start,
                    rows=len(rows) if isinstance(rows, list) else 0,
                    total=total,
                    elapsed_ms=fetch_elapsed,
                ),
            )

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

    def _require_sb(self) -> SB:
        if self._sb is None:
            msg = "browser session not open"
            raise TransientTransportError(msg)
        return self._sb

    def _resolve_ip(self) -> str:
        sb = self._require_sb()
        start_expr = f"""(() => {{
  window.__ipVal = '';
  window.__ipErr = '';
  const cb = '__ipcb_' + Math.random().toString(36).slice(2);
  const script = document.createElement('script');
  function cleanup() {{
    try {{ delete window[cb]; }} catch (_) {{}}
    if (script.parentNode) {{
      script.parentNode.removeChild(script);
    }}
  }}
  window[cb] = function(payload) {{
    try {{
      window.__ipVal = (payload && payload.ip) ? String(payload.ip) : '';
    }} catch (_) {{
      window.__ipVal = '';
    }}
    cleanup();
  }};
  script.onerror = function() {{
    window.__ipErr = 'ip_jsonp_load_error';
    cleanup();
  }};
  script.src = '{_IP_JSONP_URL}&callback=' + cb;
  document.head.appendChild(script);
  return true;
}})()"""

        sb.execute_script(start_expr)
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            ip_value = sb.execute_script("(() => window.__ipVal || '')()") or ""
            if isinstance(ip_value, str) and ip_value:
                return ip_value

            ip_err = sb.execute_script("(() => window.__ipErr || '')()") or ""
            if ip_err:
                return ""
            time.sleep(0.2)
        return ""
