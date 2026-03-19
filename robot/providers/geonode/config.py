from __future__ import annotations

from dataclasses import dataclass
from os import getenv
from typing import Literal, cast

from dotenv import load_dotenv


_GATEWAY_HOST_BY_NAME: dict[str, str] = {
    "fr": "proxy.geonode.io",
    "fr_whitelist": "prod-proxy.geonode.io",
    "us": "us.proxy.geonode.io",
    "sg": "sg.proxy.geonode.io",
}
_DEFAULT_STICKY_PORT = "10000"
ProxyType = Literal["residential", "datacenter", "mix"]


@dataclass(frozen=True)
class GeoNodeConfig:
    user: str
    password: str
    host: str
    port: str
    proxy_type: ProxyType
    country: str
    state: str
    city: str
    asn: str
    strict_off: bool
    lifetime: int


def load_geonode_config(*, env_file: str) -> GeoNodeConfig:
    load_dotenv(env_file, override=False)

    user = getenv("GEONODE_USER", "")
    password = getenv("GEONODE_PASS", "")
    gateway = getenv("GEONODE_GATEWAY", "fr")
    proxy_type_raw = getenv("GEONODE_TYPE", "residential")
    country = getenv("GEONODE_COUNTRY", "")
    state = getenv("GEONODE_STATE", "")
    city = getenv("GEONODE_CITY", "")
    asn = getenv("GEONODE_ASN", "")
    strict_off = getenv("GEONODE_STRICT_OFF", "").lower() in {"1", "true", "yes"}
    lifetime_raw = getenv("GEONODE_LIFETIME", "").strip()
    lifetime = int(lifetime_raw) if lifetime_raw else 10

    if not user or not password:
        msg = "missing GEONODE_USER or GEONODE_PASS"
        raise RuntimeError(msg)
    if gateway not in _GATEWAY_HOST_BY_NAME:
        msg = "GEONODE_GATEWAY must be one of " + "|".join(
            sorted(_GATEWAY_HOST_BY_NAME)
        )
        raise RuntimeError(msg)
    if proxy_type_raw not in {"residential", "datacenter", "mix"}:
        msg = "GEONODE_TYPE must be one of residential|datacenter|mix"
        raise RuntimeError(msg)
    if lifetime < 3 or lifetime > 1440:
        msg = "GEONODE_LIFETIME must be between 3 and 1440 minutes"
        raise RuntimeError(msg)

    proxy_type = cast("ProxyType", proxy_type_raw)
    return GeoNodeConfig(
        user=user,
        password=password,
        host=_GATEWAY_HOST_BY_NAME[gateway],
        port=_DEFAULT_STICKY_PORT,
        proxy_type=proxy_type,
        country=country,
        state=state,
        city=city,
        asn=asn,
        strict_off=strict_off,
        lifetime=lifetime,
    )
