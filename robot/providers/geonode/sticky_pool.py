from __future__ import annotations

import time

from dataclasses import dataclass, field
from threading import Condition
from typing import TYPE_CHECKING

from robot.core.errors import TransientTransportError
from robot.providers.geonode.username import build_username


if TYPE_CHECKING:
    from robot.providers.geonode.config import GeoNodeConfig


@dataclass(frozen=True)
class ProxySessionConfig:
    proxy_id: str
    host: str
    port: str
    password: str
    username: str

    def as_selenium_proxy(self) -> str:
        return f"{self.username}:{self.password}@{self.host}:{self.port}"


@dataclass(frozen=True)
class ProxyLease:
    session: ProxySessionConfig
    slot_id: int


@dataclass
class _SlotState:
    slot_id: int
    in_use: bool = False
    cooldown_until: float = 0.0


@dataclass
class StickyProxyPool:
    config: GeoNodeConfig
    capacity: int
    _states: list[_SlotState] = field(init=False)
    _cv: Condition = field(default_factory=Condition, init=False)

    def __post_init__(self) -> None:
        if self.capacity < 1:
            msg = "proxy session capacity must be >= 1"
            raise ValueError(msg)
        self._states = [_SlotState(slot_id=i) for i in range(1, self.capacity + 1)]

    def acquire(self, *, wait_s: float = 30.0) -> ProxyLease:
        deadline = time.monotonic() + wait_s
        with self._cv:
            while True:
                now = time.monotonic()
                for state in self._states:
                    if state.in_use:
                        continue
                    if state.cooldown_until > now:
                        continue
                    state.in_use = True
                    proxy_id = f"proxy-1-slot-{state.slot_id}"
                    username = build_username(
                        self.config,
                        session_id=f"slot{state.slot_id}-{int(now)}",
                    )
                    session = ProxySessionConfig(
                        proxy_id=proxy_id,
                        host=self.config.host,
                        port=self.config.port,
                        password=self.config.password,
                        username=username,
                    )
                    return ProxyLease(session=session, slot_id=state.slot_id)

                remaining = deadline - now
                if remaining <= 0:
                    msg = "no sticky session slot available before timeout"
                    raise TransientTransportError(msg)
                self._cv.wait(timeout=remaining)

    def release(self, lease: ProxyLease, *, cooldown_s: float = 0.0) -> None:
        with self._cv:
            for state in self._states:
                if state.slot_id != lease.slot_id:
                    continue
                state.in_use = False
                if cooldown_s > 0:
                    state.cooldown_until = max(
                        state.cooldown_until,
                        time.monotonic() + cooldown_s,
                    )
                self._cv.notify_all()
                return

        msg = f"unknown sticky session slot {lease.slot_id}"
        raise RuntimeError(msg)
