from __future__ import annotations

import logging
import random
import time

from dataclasses import dataclass
from typing import TYPE_CHECKING

from robot.observability import kv
from robot.providers.osiptel.session import OsiptelSession, OsiptelSessionSettings


if TYPE_CHECKING:
    from robot.providers.geonode.sticky_pool import ProxyLease, StickyProxyPool


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionPolicy:
    page_size: int
    session_budget: int
    wait_min_s: float
    wait_max_s: float
    chrome_binary: str = ""


class SessionManager:
    def __init__(
        self,
        *,
        run_id: str,
        worker_id: int,
        proxy_pool: StickyProxyPool,
        policy: SessionPolicy,
    ) -> None:
        self._run_id = run_id
        self._worker_id = worker_id
        self._proxy_pool = proxy_pool
        self._policy = policy
        self._lease: ProxyLease | None = None
        self._session: OsiptelSession | None = None
        self._session_uses = 0
        self._last_proxy_id = ""

    @property
    def last_proxy_id(self) -> str:
        return self._last_proxy_id

    @property
    def active_session(self) -> OsiptelSession | None:
        return self._session

    def ensure_session(self) -> OsiptelSession:
        if self._session is not None:
            return self._session

        lease = self._proxy_pool.acquire(wait_s=60.0)
        self._last_proxy_id = lease.session.proxy_id

        session = OsiptelSession(
            proxy=lease.session,
            settings=OsiptelSessionSettings(
                page_size=self._policy.page_size,
                chrome_binary=self._policy.chrome_binary,
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

    def after_success(self) -> None:
        if self._session is None:
            return

        self._session_uses += 1
        if self._session_uses >= self._policy.session_budget:
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
            self.close(cooldown_s=0.0)
            return

        wait_s = random.uniform(self._policy.wait_min_s, self._policy.wait_max_s)
        logger.info(
            "session_wait %s",
            kv(
                run_id=self._run_id,
                worker_id=self._worker_id,
                session_id=self._session.session_id,
                proxy_id=self._session.proxy_id,
                wait_s=round(wait_s, 3),
            ),
        )
        time.sleep(wait_s)

    def close(self, *, cooldown_s: float) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None
            self._session_uses = 0
        if self._lease is not None:
            self._proxy_pool.release(self._lease, cooldown_s=cooldown_s)
            self._lease = None
