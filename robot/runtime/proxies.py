from __future__ import annotations

import time

from dataclasses import dataclass, field
from os import getenv
from threading import Condition

from dotenv import load_dotenv


@dataclass(frozen=True)
class ProxyConfig:
    proxy_id: str
    user: str
    password: str
    host: str = "proxy.geonode.io"
    port: str = "9000"
    proxy_type: str = ""
    country: str = ""
    state: str = ""
    city: str = ""
    asn: str = ""
    strict_off: bool = False
    lifetime: int | None = None

    def with_session_username(self, session_id: str) -> str:
        base = f"{self.user}-type-{self.proxy_type}"
        if self.country:
            base += f"-country-{self.country}"
        if self.state:
            base += f"-state-{self.state}"
        if self.city:
            base += f"-city-{self.city}"
        if self.asn:
            base += f"-asn-{self.asn}"
        if self.strict_off:
            base += "-strict-off"
        base += f"-session-{session_id}"
        if self.lifetime is not None:
            base += f"-lifetime-{self.lifetime}"
        return base

    def as_selenium_proxy(self, session_id: str) -> str:
        username = self.with_session_username(session_id)
        return f"{username}:{self.password}@{self.host}:{self.port}"


@dataclass
class ProxyLease:
    proxy: ProxyConfig


@dataclass
class _ProxyState:
    proxy: ProxyConfig
    in_use: bool = False
    cooldown_until: float = 0.0


@dataclass
class ProxyPool:
    proxies: list[ProxyConfig]
    _states: list[_ProxyState] = field(init=False)
    _cv: Condition = field(default_factory=Condition, init=False)

    def __post_init__(self) -> None:
        if not self.proxies:
            msg = "proxy pool cannot be empty"
            raise ValueError(msg)
        self._states = [_ProxyState(proxy=p) for p in self.proxies]

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
                    return ProxyLease(proxy=state.proxy)
                remaining = deadline - now
                if remaining <= 0:
                    msg = "no proxy available before timeout"
                    raise RuntimeError(msg)
                self._cv.wait(timeout=remaining)

    def release(self, lease: ProxyLease, *, cooldown_s: float = 0.0) -> None:
        with self._cv:
            for state in self._states:
                if state.proxy.proxy_id != lease.proxy.proxy_id:
                    continue
                state.in_use = False
                if cooldown_s > 0:
                    state.cooldown_until = max(
                        state.cooldown_until, time.monotonic() + cooldown_s
                    )
                self._cv.notify_all()
                return
        msg = f"unknown proxy lease {lease.proxy.proxy_id}"
        raise RuntimeError(msg)


def build_pool_from_env(*, env_file: str = ".env") -> ProxyPool:
    load_dotenv(env_file, override=False)
    list_raw = getenv("GEONODE_PROXY_LIST", "").strip()
    if list_raw:
        proxies = _parse_proxy_list(list_raw)
        return ProxyPool(proxies=proxies)

    user = getenv("GEONODE_USER", "")
    password = getenv("GEONODE_PASS", "")
    host = getenv("GEONODE_HOST", "proxy.geonode.io")
    port = getenv("GEONODE_PORT", "9000")
    proxy_type = getenv("GEONODE_TYPE", "")
    country = getenv("GEONODE_COUNTRY", "")
    state = getenv("GEONODE_STATE", "")
    city = getenv("GEONODE_CITY", "")
    asn = getenv("GEONODE_ASN", "")
    strict_off = getenv("GEONODE_STRICT_OFF", "").lower() in {"1", "true", "yes"}
    lifetime_raw = getenv("GEONODE_LIFETIME", "").strip()
    lifetime = int(lifetime_raw) if lifetime_raw else None

    if not user or not password:
        msg = "missing GEONODE_USER or GEONODE_PASS"
        raise RuntimeError(msg)
    if proxy_type not in {"residential", "datacenter", "mix"}:
        msg = "GEONODE_TYPE must be one of residential|datacenter|mix"
        raise RuntimeError(msg)

    proxy = ProxyConfig(
        proxy_id="proxy-1",
        user=user,
        password=password,
        host=host,
        port=port,
        proxy_type=proxy_type,
        country=country,
        state=state,
        city=city,
        asn=asn,
        strict_off=strict_off,
        lifetime=lifetime,
    )
    return ProxyPool(proxies=[proxy])


def _parse_proxy_list(raw: str) -> list[ProxyConfig]:
    proxies: list[ProxyConfig] = []
    for idx, item in enumerate(raw.split(","), start=1):
        token = item.strip()
        if not token:
            continue
        try:
            credentials, endpoint = token.split("@", 1)
            user, password = credentials.split(":", 1)
            host, port = endpoint.split(":", 1)
        except ValueError as exc:
            msg = f"invalid GEONODE_PROXY_LIST entry: {token!r}"
            raise RuntimeError(msg) from exc

        proxies.append(
            ProxyConfig(
                proxy_id=f"proxy-{idx}",
                user=user,
                password=password,
                host=host,
                port=port,
                proxy_type="residential",
            )
        )

    if not proxies:
        msg = "GEONODE_PROXY_LIST is set but empty"
        raise RuntimeError(msg)
    return proxies
