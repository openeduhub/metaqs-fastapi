from importlib import import_module

# from fastapi import APIRouter

from app.core.config import API_VERSION

# from .debug.v1 import router as debug_router
from .util import (
    materials_filter_params,
    PaginationParams,
    pagination_params,
)

api = import_module(f".{API_VERSION}.api", package=__name__)

router = api.router
# router = APIRouter()
# router.include_router(api.router)
# router.include_router(debug_router, prefix="/debug")
