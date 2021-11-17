from fastapi import (
    Depends,
    FastAPI,
    Security,
)
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException
from starlette.middleware.cors import CORSMiddleware
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY
from starlette_context.middleware import RawContextMiddleware

from app.api import router as api_router
from app.api.auth import authenticated
from app.core.config import (
    ALLOWED_HOSTS,
    API_VERSION,
    DEBUG,
    LOG_LEVEL,
    PROJECT_NAME,
)
from app.core.errors import (
    http_422_error_handler,
    http_error_handler,
)
from app.elastic.utils import (
    close_elastic_connection,
    connect_to_elastic,
)
from app.http import close_client
from app.pg.pg_utils import (
    get_postgres,
    close_postgres_connection,
)
from app.pg.postgres import Postgres

description = """
## Links

### Exploratory analysis and result visualization

* [**Apache Superset** (Visualization Dashboard)](http://141.5.104.94:8083/login/)

### Further documentation

* [**Data Build Tool (dbt)** (Data Processing pipelines in SQL)](http://141.5.104.94:8081/#!/overview)
"""

fastapi_app = FastAPI(title=PROJECT_NAME, description=description, debug=DEBUG)

fastapi_app.add_middleware(RawContextMiddleware)

fastapi_app.add_event_handler("startup", connect_to_elastic)
fastapi_app.add_event_handler("shutdown", close_elastic_connection)
fastapi_app.add_event_handler("shutdown", close_postgres_connection)
fastapi_app.add_event_handler("shutdown", close_client)

fastapi_app.add_exception_handler(HTTPException, http_error_handler)
fastapi_app.add_exception_handler(HTTP_422_UNPROCESSABLE_ENTITY, http_422_error_handler)


class Ping(BaseModel):
    status: str = Field(
        default="not ok", description="Ping output. Should be 'ok' in happy case.",
    )


@fastapi_app.get(
    "/_ping",
    description="Ping function for automatic health check.",
    response_model=Ping,
    tags=["Healthcheck"],
)
async def ping_api():
    return {"status": "ok"}


@fastapi_app.get(
    "/pg-version",
    response_model=dict,
    dependencies=[Security(authenticated)],
    tags=["Authenticated"],
)
async def pg_version(postgres: Postgres = Depends(get_postgres),):
    async with postgres.pool.acquire() as conn:
        version = await conn.fetchval("select version()")
        return {"version": version}


fastapi_app.include_router(api_router, prefix=f"/api/{API_VERSION}")

for route in fastapi_app.routes:
    if isinstance(route, APIRoute):
        route.operation_id = route.name


app = CORSMiddleware(
    app=fastapi_app,
    allow_origins=ALLOWED_HOSTS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count"],
)


if __name__ == "__main__":
    import os
    import uvicorn

    conf = {
        "host": "0.0.0.0",
        "port": 80,
        "reload": True,
        "reload_dirs": [f"{os.getcwd()}/app"],
        "log_level": LOG_LEVEL,
    }
    print(f"starting uvicorn with config: {conf}")

    uvicorn.run("app.main:app", **conf)
