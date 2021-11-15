from functools import partial

import httpx as http
import polling2
from jsonrpcclient import (
    Ok,
    request_uuid,
    parse,
)

from app.core.config import DBT_URL
from app.core.logging import logger


def run_analytics():
    return _send_rpc(method="cli_args", params={"cli": "run"})


def poll(request_token: str):
    return polling2.poll(
        partial(
            _send_rpc,
            method="poll",
            params={"request_token": request_token, "logs": False},
        ),
        check_success=lambda result: result.get("state") == "success",
        step=30,
        max_tries=10,
    )


def _send_rpc(method: str, params: dict) -> dict:
    response = http.post(DBT_URL, json=request_uuid(method, params=params))
    parsed = parse(response.json())
    if not isinstance(parsed, Ok):
        logger.error(f"Error from dbt server: {parsed.message}")
        raise Exception(f"Error from dbt server: {parsed.message}")

    return parsed.result
