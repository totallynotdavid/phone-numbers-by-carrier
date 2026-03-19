from __future__ import annotations

import logging
import time

from typing import TYPE_CHECKING

from robot.domain.errors import RobotError
from robot.domain.retry import decide_retry
from robot.domain.types import RUC, LookupResult, Status
from robot.obs.events import LOOKUP_FAILED, LOOKUP_OK
from robot.obs.logging import kv
from robot.providers.osiptel_flow import count_carrier_lines


logger = logging.getLogger(__name__)
MAX_ATTEMPTS_PER_RUC = 3

if TYPE_CHECKING:
    from robot.pipeline.session_runtime import SessionRuntime


def execute_lookup(
    *,
    run_id: str,
    worker_id: int,
    runtime: SessionRuntime,
    ruc: RUC,
    page_size: int,
    ban_cooldown_s: float,
) -> LookupResult:
    for attempt_no in range(1, MAX_ATTEMPTS_PER_RUC + 1):
        try:
            active = runtime.ensure_active()
            started = time.perf_counter()
            total, carrier_counts = count_carrier_lines(
                session=active.browser,
                ruc=ruc,
                page_size=page_size,
            )
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            logger.info(
                "%s %s",
                LOOKUP_OK,
                kv(
                    run_id=run_id,
                    worker_id=worker_id,
                    session_id=active.browser.session_id,
                    proxy_id=active.browser.proxy_id,
                    egress_ip=active.egress_ip,
                    ruc=ruc,
                    attempt=attempt_no,
                    elapsed_ms=elapsed_ms,
                    lines=total,
                    carriers=len(carrier_counts),
                ),
            )
            runtime.after_success()
            return LookupResult(
                ruc=ruc,
                status=Status.OK,
                total_lines=total,
                carrier_counts=carrier_counts,
                attempt=attempt_no,
                session_id=active.browser.session_id,
                proxy_id=active.browser.proxy_id,
            )
        except RobotError as exc:
            session_id = runtime.active_session_id()
            proxy_id = runtime.last_proxy_id
            egress_ip = runtime.active_egress_ip()
            decision = decide_retry(
                exc,
                default_cooldown_s=ban_cooldown_s,
            )
            logger.warning(
                "%s %s",
                LOOKUP_FAILED,
                kv(
                    run_id=run_id,
                    worker_id=worker_id,
                    session_id=session_id,
                    proxy_id=proxy_id,
                    egress_ip=egress_ip,
                    ruc=ruc,
                    attempt=attempt_no,
                    error_code=decision.error_code,
                    error_detail=str(exc),
                ),
            )
            if decision.rotate_session:
                runtime.close_active(cooldown_s=decision.cooldown_proxy_s)

            can_retry = attempt_no < MAX_ATTEMPTS_PER_RUC and decision.rotate_session
            if can_retry:
                continue
            return LookupResult(
                ruc=ruc,
                status=Status.FAILED,
                error_code=decision.error_code,
                error_detail=str(exc),
                attempt=attempt_no,
                session_id=session_id,
                proxy_id=proxy_id,
            )

    return LookupResult(
        ruc=ruc,
        status=Status.FAILED,
        error_code="exhausted_retries",
        error_detail="unexpected retry exhaustion",
        attempt=MAX_ATTEMPTS_PER_RUC,
        session_id=runtime.active_session_id(),
        proxy_id=runtime.last_proxy_id,
    )
