from __future__ import annotations

import json
import logging
import time

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypedDict, cast

import httpx

from robot.domain.errors import BanSignalError, ParseError, TransientTransportError
from robot.obs.events import FETCH_PAGE_OK, FETCH_PAGE_START
from robot.obs.logging import kv


if TYPE_CHECKING:
    from robot.providers.geonode import ProxySessionConfig


API_URL = "https://checatuslineas.osiptel.gob.pe/Consultas/GetAllCabeceraConsulta/"
logger = logging.getLogger(__name__)


class OsiptelResponse(TypedDict, total=False):
    iTotalRecords: int
    aaData: list[list[Any]]


@dataclass(frozen=True)
class PageRequest:
    ruc: str
    token: str
    draw: int
    start: int
    length: int


def build_payload(req: PageRequest) -> dict[str, str]:
    payload: dict[str, str] = {}
    columns = ["indice", "modalidad", "numeroservicio", "operador"]
    for index, name in enumerate(columns):
        payload[f"columns[{index}][data]"] = str(index)
        payload[f"columns[{index}][name]"] = name
        payload[f"columns[{index}][searchable]"] = "false"
        payload[f"columns[{index}][orderable]"] = "false"
        payload[f"columns[{index}][search][value]"] = ""
        payload[f"columns[{index}][search][regex]"] = "false"

    payload.update(
        {
            "order[0][column]": "0",
            "order[0][dir]": "asc",
            "draw": str(req.draw),
            "start": str(req.start),
            "length": str(req.length),
            "search[value]": "",
            "search[regex]": "false",
            "models[IdTipoDoc]": "2",
            "models[NumeroDocumento]": req.ruc,
            "models[Captcha]": "true",
            "models[ReCaptcha]": req.token,
            "models[GoogleCaptchaToken]": req.token,
            "models[GoogleCaptchaTokenOLD]": "",
        }
    )
    return payload


def build_headers(*, user_agent: str, cookie_header: str) -> dict[str, str]:
    headers = {
        "Accept": "*/*",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://checatuslineas.osiptel.gob.pe",
        "Referer": "https://checatuslineas.osiptel.gob.pe/",
        "User-Agent": user_agent,
    }
    if cookie_header:
        headers["Cookie"] = cookie_header
    return headers


def resolve_egress_ip(proxy: ProxySessionConfig) -> str:
    url = "http://ip-api.com/json"
    try:
        with httpx.Client(proxy=proxy.as_http_proxy_url(), timeout=15.0) as client:
            response = client.get(url)
        if response.status_code != 200:
            return ""
        payload = response.json()
        if not isinstance(payload, dict):
            return ""
        value = payload.get("query")
        if isinstance(value, str):
            return value
        return ""
    except (httpx.HTTPError, ValueError):
        return ""


class OsiptelHttpClient:
    def __init__(
        self,
        *,
        proxy: ProxySessionConfig,
        user_agent: str,
        cookie_header: str,
        timeout_s: float = 25.0,
    ) -> None:
        self._proxy = proxy
        self._headers = build_headers(
            user_agent=user_agent, cookie_header=cookie_header
        )
        self._timeout_s = timeout_s
        self._client: httpx.Client | None = None

    def __enter__(self) -> OsiptelHttpClient:
        self._client = httpx.Client(
            proxy=self._proxy.as_http_proxy_url(),
            timeout=self._timeout_s,
        )
        return self

    def __exit__(self, *_) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def fetch(self, req: PageRequest) -> OsiptelResponse:
        client = self._require_client()
        body = build_payload(req)
        started = time.perf_counter()
        logger.info(
            "%s %s",
            FETCH_PAGE_START,
            kv(ruc=req.ruc, draw=req.draw, start=req.start, length=req.length),
        )
        try:
            response = client.post(API_URL, data=body, headers=self._headers)
        except httpx.HTTPError as exc:
            msg = f"osiptel request transport failed: {type(exc).__name__}: {exc}"
            raise TransientTransportError(msg) from exc

        status = response.status_code
        if status >= 500:
            response_text = response.text.replace("\n", " ").strip()[:160]
            msg = (
                "osiptel request failed "
                f"status={status} draw={req.draw} start={req.start} length={req.length} "
                f"ruc={req.ruc} body={response_text}"
            )
            logger.warning(msg)
            raise BanSignalError(msg)
        if status != 200:
            msg = f"osiptel request failed status={status}"
            raise TransientTransportError(msg)

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            msg = "osiptel response is not valid json"
            raise ParseError(msg) from exc

        if not isinstance(payload, dict):
            msg = "osiptel response json is not an object"
            raise ParseError(msg)
        logger.info(
            "%s %s",
            FETCH_PAGE_OK,
            kv(
                ruc=req.ruc,
                draw=req.draw,
                start=req.start,
                length=req.length,
                status=status,
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            ),
        )
        return cast("OsiptelResponse", payload)

    def _require_client(self) -> httpx.Client:
        if self._client is None:
            msg = "http client not open"
            raise RuntimeError(msg)
        return self._client
