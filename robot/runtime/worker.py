from __future__ import annotations

import logging

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from robot.core import (
    RUC,
    LookupResult,
    RobotError,
    Status,
    WorkerSummary,
    decide_retry,
)
from robot.observability import kv, timed
from robot.runtime.session_manager import SessionManager, SessionPolicy


if TYPE_CHECKING:
    import queue

    from robot.providers.geonode.sticky_pool import StickyProxyPool


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerSettings:
    page_size: int
    session_budget: int
    wait_min_s: float
    wait_max_s: float
    same_session_retries: int
    ban_cooldown_s: float
    chrome_binary: str = ""


class ResultWriter(Protocol):
    def write(self, result: LookupResult) -> None: ...


class Worker:
    def __init__(
        self,
        *,
        worker_id: int,
        run_id: str,
        task_queue: queue.Queue[RUC | None],
        proxy_pool: StickyProxyPool,
        writer: ResultWriter,
        settings: WorkerSettings,
    ) -> None:
        self._worker_id = worker_id
        self._run_id = run_id
        self._task_queue = task_queue
        self._writer = writer
        self._settings = settings
        self._sessions = SessionManager(
            run_id=run_id,
            worker_id=worker_id,
            proxy_pool=proxy_pool,
            policy=SessionPolicy(
                page_size=settings.page_size,
                session_budget=settings.session_budget,
                wait_min_s=settings.wait_min_s,
                wait_max_s=settings.wait_max_s,
                chrome_binary=settings.chrome_binary,
            ),
        )

    def run(self) -> WorkerSummary:
        summary = WorkerSummary()
        while True:
            ruc = self._task_queue.get()
            if ruc is None:
                self._task_queue.task_done()
                break

            try:
                result = self._process_ruc(ruc)
                self._writer.write(result)
                summary.processed += 1
                if result.status == Status.OK:
                    summary.succeeded += 1
                else:
                    summary.failed += 1
            finally:
                self._task_queue.task_done()

        self._sessions.close(cooldown_s=0.0)
        return summary

    def _process_ruc(self, ruc: RUC) -> LookupResult:
        attempts = self._settings.same_session_retries + 2
        for attempt_no in range(1, attempts + 1):
            try:
                session = self._sessions.ensure_session()
                with timed() as timer:
                    total, carrier_counts = session.count_carrier_lines(ruc)
                logger.info(
                    "lookup_ok %s",
                    kv(
                        run_id=self._run_id,
                        worker_id=self._worker_id,
                        session_id=session.session_id,
                        proxy_id=session.proxy_id,
                        egress_ip=session.egress_ip,
                        ruc=ruc,
                        attempt=attempt_no,
                        elapsed_ms=timer.elapsed_ms,
                        lines=total,
                        carriers=len(carrier_counts),
                    ),
                )
                self._sessions.after_success()
                return LookupResult(
                    ruc=ruc,
                    status=Status.OK,
                    total_lines=total,
                    carrier_counts=carrier_counts,
                    attempt=attempt_no,
                    session_id=session.session_id,
                    proxy_id=session.proxy_id,
                )
            except RobotError as exc:
                decision = decide_retry(
                    exc,
                    default_cooldown_s=self._settings.ban_cooldown_s,
                )
                active_session = self._sessions.active_session
                logger.warning(
                    "lookup_failed %s",
                    kv(
                        run_id=self._run_id,
                        worker_id=self._worker_id,
                        session_id=""
                        if active_session is None
                        else active_session.session_id,
                        proxy_id=self._sessions.last_proxy_id,
                        egress_ip=""
                        if active_session is None
                        else active_session.egress_ip,
                        ruc=ruc,
                        attempt=attempt_no,
                        error_code=decision.error_code,
                        error_detail=str(exc),
                    ),
                )

                if decision.rotate_session:
                    self._sessions.close(cooldown_s=decision.cooldown_proxy_s)

                can_retry = attempt_no < attempts and (
                    decision.retry_same_session or decision.rotate_session
                )
                if can_retry:
                    continue

                return LookupResult(
                    ruc=ruc,
                    status=Status.FAILED,
                    error_code=decision.error_code,
                    error_detail=str(exc),
                    attempt=attempt_no,
                    session_id=""
                    if active_session is None
                    else active_session.session_id,
                    proxy_id=self._sessions.last_proxy_id,
                )

        return LookupResult(
            ruc=ruc,
            status=Status.FAILED,
            error_code="exhausted_retries",
            error_detail="unexpected retry exhaustion",
            attempt=attempts,
            session_id=""
            if self._sessions.active_session is None
            else self._sessions.active_session.session_id,
            proxy_id=self._sessions.last_proxy_id,
        )
