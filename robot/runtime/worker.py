from __future__ import annotations

import logging
import queue
import random
import time

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from robot.domain import RUC, Result, Status
from robot.errors import RobotError
from robot.observability import kv, timed
from robot.osiptel.browser import BrowserSession, BrowserSettings
from robot.runtime.retry import decide


if TYPE_CHECKING:
    from robot.runtime.proxies import ProxyLease, ProxyPool


logger = logging.getLogger(__name__)


@dataclass
class WorkerSettings:
    page_size: int
    session_budget: int
    wait_min_s: float
    wait_max_s: float
    same_session_retries: int
    ban_cooldown_s: float
    chrome_binary: str = ""


@dataclass
class WorkerSummary:
    processed: int = 0
    succeeded: int = 0
    failed: int = 0


class ResultWriter(Protocol):
    def write(self, result: Result) -> None: ...


class Worker:
    def __init__(
        self,
        *,
        worker_id: int,
        run_id: str,
        task_queue: queue.Queue[RUC],
        proxy_pool: ProxyPool,
        writer: ResultWriter,
        settings: WorkerSettings,
    ) -> None:
        self._worker_id = worker_id
        self._run_id = run_id
        self._task_queue = task_queue
        self._proxy_pool = proxy_pool
        self._writer = writer
        self._settings = settings
        self._lease: ProxyLease | None = None
        self._session: BrowserSession | None = None
        self._session_uses = 0
        self._last_proxy_id = ""

    def run(self) -> WorkerSummary:
        summary = WorkerSummary()
        while True:
            try:
                ruc = self._task_queue.get_nowait()
            except queue.Empty:
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
        self._close_session(cooldown_s=0.0)
        return summary

    def _process_ruc(self, ruc: RUC) -> Result:
        attempts = self._settings.same_session_retries + 2
        for attempt_no in range(1, attempts + 1):
            try:
                session = self._ensure_session()
                return self._execute_attempt(session, ruc, attempt_no=attempt_no)
            except RobotError as exc:
                maybe_result = self._handle_failure(
                    ruc, exc, attempt_no=attempt_no, attempts=attempts
                )
                if maybe_result is not None:
                    return maybe_result
                continue

        return Result(
            ruc=ruc,
            status=Status.FAILED,
            error_code="exhausted_retries",
            error_detail="unexpected retry exhaustion",
        )

    def _execute_attempt(
        self, session: BrowserSession, ruc: RUC, *, attempt_no: int
    ) -> Result:
        with timed() as timer:
            lines = session.count_lines(ruc)
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
                lines=lines,
            ),
        )
        self._session_uses += 1
        self._maybe_rotate_session_after_success()
        return Result(ruc=ruc, total_lines=lines, status=Status.OK)

    def _handle_failure(
        self, ruc: RUC, exc: RobotError, *, attempt_no: int, attempts: int
    ) -> Result | None:
        session = self._session
        decision = decide(exc, default_cooldown_s=self._settings.ban_cooldown_s)
        logger.warning(
            "lookup_failed %s",
            kv(
                run_id=self._run_id,
                worker_id=self._worker_id,
                session_id=session.session_id if session else "",
                proxy_id=session.proxy_id if session else self._last_proxy_id,
                egress_ip=session.egress_ip if session else "",
                ruc=ruc,
                attempt=attempt_no,
                error_code=decision.error_code,
                error_detail=str(exc),
            ),
        )

        if decision.rotate_session:
            self._close_session(cooldown_s=decision.cooldown_proxy_s)

        can_retry = attempt_no < attempts and (
            decision.retry_same_session or decision.rotate_session
        )
        if can_retry:
            return None

        return Result(
            ruc=ruc,
            status=Status.FAILED,
            error_code=decision.error_code,
            error_detail=str(exc),
        )

    def _ensure_session(self) -> BrowserSession:
        if self._session is not None:
            return self._session

        lease = self._proxy_pool.acquire(wait_s=60.0)
        self._last_proxy_id = lease.proxy.proxy_id
        session = BrowserSession(
            proxy=lease.proxy,
            settings=BrowserSettings(
                page_size=self._settings.page_size,
                chrome_binary=self._settings.chrome_binary,
            ),
        )
        opened = False
        try:
            session.open()
            opened = True
        finally:
            if not opened:
                self._proxy_pool.release(lease, cooldown_s=0.0)

        self._lease = lease
        self._session = session
        self._session_uses = 0
        return session

    def _maybe_rotate_session_after_success(self) -> None:
        if self._session is None:
            return
        if self._session_uses >= self._settings.session_budget:
            logger.info(
                "session_budget_reached %s",
                kv(
                    run_id=self._run_id,
                    worker_id=self._worker_id,
                    session_id=self._session.session_id,
                    proxy_id=self._session.proxy_id,
                    uses=self._session_uses,
                ),
            )
            self._close_session(cooldown_s=0.0)
            return

        sleep_s = random.uniform(self._settings.wait_min_s, self._settings.wait_max_s)
        logger.info(
            "session_wait %s",
            kv(
                run_id=self._run_id,
                worker_id=self._worker_id,
                session_id=self._session.session_id,
                proxy_id=self._session.proxy_id,
                wait_s=round(sleep_s, 3),
            ),
        )
        time.sleep(sleep_s)

    def _close_session(self, *, cooldown_s: float) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None
            self._session_uses = 0
        if self._lease is not None:
            self._proxy_pool.release(self._lease, cooldown_s=cooldown_s)
            self._lease = None
        self._last_proxy_id = ""
