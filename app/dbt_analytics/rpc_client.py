import httpx as http
from jsonrpcclient import (
    Ok,
    request_uuid,
    parse,
)

from app.core.config import DBT_URL
from app.core.logging import logger


def run_analytics():
    response = http.post(DBT_URL, json=request_uuid("cli_args", params={"cli": "run"}))
    parsed = parse(response.json())
    if isinstance(parsed, Ok):
        logger.info(f"Started analytics run: {parsed.result}")
    else:
        logger.error(f"Error from dbt server: {parsed.message}")
