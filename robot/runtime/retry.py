from __future__ import annotations

from dataclasses import dataclass

from robot.errors import (
    BanSignalError,
    CaptchaError,
    ParseError,
    PermanentInputError,
    TransientTransportError,
)


@dataclass(frozen=True)
class RetryDecision:
    error_code: str
    retry_same_session: bool
    rotate_session: bool
    cooldown_proxy_s: float


def decide(exc: Exception, *, default_cooldown_s: float) -> RetryDecision:
    text = str(exc).lower()
    if isinstance(exc, PermanentInputError):
        return RetryDecision(
            error_code="permanent_input_error",
            retry_same_session=False,
            rotate_session=False,
            cooldown_proxy_s=0.0,
        )
    if isinstance(exc, CaptchaError):
        return RetryDecision(
            error_code="captcha_error",
            retry_same_session=False,
            rotate_session=True,
            cooldown_proxy_s=default_cooldown_s,
        )
    if isinstance(exc, BanSignalError):
        return RetryDecision(
            error_code="ban_signal",
            retry_same_session=False,
            rotate_session=True,
            cooldown_proxy_s=default_cooldown_s,
        )
    if isinstance(exc, ParseError):
        return RetryDecision(
            error_code="parse_error",
            retry_same_session=True,
            rotate_session=False,
            cooldown_proxy_s=0.0,
        )
    if isinstance(exc, TransientTransportError):
        return RetryDecision(
            error_code="transport_error",
            retry_same_session=True,
            rotate_session=False,
            cooldown_proxy_s=0.0,
        )

    if "connection refused" in text or "chrome not reachable" in text:
        return RetryDecision(
            error_code="driver_disconnect",
            retry_same_session=False,
            rotate_session=True,
            cooldown_proxy_s=default_cooldown_s,
        )

    return RetryDecision(
        error_code="provider_error",
        retry_same_session=False,
        rotate_session=False,
        cooldown_proxy_s=0.0,
    )
