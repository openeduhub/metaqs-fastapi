from fastapi import FastAPI
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException

from starlette.middleware.cors import CORSMiddleware
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from app.api import router as api_router
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

app = FastAPI(title=PROJECT_NAME, debug=DEBUG)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_HOSTS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count"],
)

app.add_event_handler("startup", connect_to_elastic)
app.add_event_handler("shutdown", close_elastic_connection)

app.add_exception_handler(HTTPException, http_error_handler)
app.add_exception_handler(HTTP_422_UNPROCESSABLE_ENTITY, http_422_error_handler)

app.include_router(api_router, prefix=f"/api/{API_VERSION}")


class Ping(BaseModel):
    status: str = Field(
        default="not ok", description="Ping output. Should be 'ok' in happy case.",
    )


@app.get(
    "/_ping",
    description="Ping function for automatic health check.",
    response_model=Ping,
)
async def ping():
    return {"status": "ok"}


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
