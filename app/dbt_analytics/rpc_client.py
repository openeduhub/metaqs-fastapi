from httpx import AsyncClient
from jsonrpcclient import (
    Ok,
    request_uuid,
    parse,
)

from app.core.logging import logger

DBT_SERVER = "http://dbt:8580/jsonrpc"


async def run_analytics(http: AsyncClient):
    response = await http.post(
        DBT_SERVER, json=request_uuid("cli_args", params={"cli": "run"})
    )
    parsed = parse(response.json())
    if isinstance(parsed, Ok):
        logger.info(f"Started analytics run: {parsed.result}")
    else:
        logger.error(f"Error from dbt server: {parsed.message}")
