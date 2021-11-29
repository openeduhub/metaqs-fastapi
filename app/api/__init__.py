from importlib import import_module

from app.core.config import API_VERSION

from .util import (
    materials_filter_params,
    PaginationParams,
    pagination_params,
)

api = import_module(f".{API_VERSION}", package=__name__)

real_time_router = api.real_time_router
analytics_router = api.analytics_router
