from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from robot.providers.geonode.config import GeoNodeConfig


def build_username(config: GeoNodeConfig, *, session_id: str) -> str:
    value = f"{config.user}-type-{config.proxy_type}"
    if config.country:
        value += f"-country-{config.country}"
    if config.state:
        value += f"-state-{config.state}"
    if config.city:
        value += f"-city-{config.city}"
    if config.asn:
        value += f"-asn-{config.asn}"
    if config.strict_off:
        value += "-strict-off"
    value += f"-session-{session_id}"
    value += f"-lifetime-{config.lifetime}"
    return value
