from __future__ import annotations

import time

from typing import TYPE_CHECKING

from robot.core.errors import CaptchaError
from robot.observability import timed


if TYPE_CHECKING:
    from seleniumbase import SB  # type: ignore[import-untyped]


_TOKEN_START_EXPR = """(() => {
  window.__rcTok = '';
  window.__rcErr = '';
  const key = (document.querySelector('#hiddenRecaptchaKey') || {}).value || '';
  const action = (document.querySelector('#hiddenAction') || {}).value || '';
  if (!window.grecaptcha || !key) {
    window.__rcErr = 'missing grecaptcha or key';
    return false;
  }
  window.grecaptcha.ready(function() {
    window.grecaptcha.execute(key, {action: action})
      .then(tok => window.__rcTok = tok || '')
      .catch(err => window.__rcErr = String(err));
  });
  return true;
})()"""


def generate_token(
    sb: SB, *, timeout_s: float = 20.0, poll_s: float = 0.25
) -> tuple[str, int]:
    with timed() as timer:
        started = bool(sb.execute_script(_TOKEN_START_EXPR))
        if not started:
            msg = "failed to start recaptcha token generation"
            raise CaptchaError(msg)

        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            token = sb.execute_script("(() => window.__rcTok || '')()") or ""
            token_err = sb.execute_script("(() => window.__rcErr || '')()") or ""
            if isinstance(token, str) and token.strip():
                return token.strip(), timer.elapsed_ms
            if token_err:
                msg = f"captcha token generation failed: {token_err}"
                raise CaptchaError(msg)
            time.sleep(poll_s)

    msg = "captcha token not generated in time"
    raise CaptchaError(msg)
