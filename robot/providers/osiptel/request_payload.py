from __future__ import annotations


_COLUMNS = ["indice", "modalidad", "numeroservicio", "operador"]


def build_payload(
    ruc: str,
    token: str,
    *,
    draw: int,
    start: int,
    length: int,
) -> dict[str, str]:
    payload: dict[str, str] = {}
    for index, name in enumerate(_COLUMNS):
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
            "draw": str(draw),
            "start": str(start),
            "length": str(length),
            "search[value]": "",
            "search[regex]": "false",
            "models[IdTipoDoc]": "2",
            "models[NumeroDocumento]": str(ruc),
            "models[Captcha]": "true",
            "models[ReCaptcha]": token,
            "models[GoogleCaptchaToken]": token,
            "models[GoogleCaptchaTokenOLD]": "",
        }
    )
    return payload
