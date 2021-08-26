from fastapi import (
    APIRouter,
    BackgroundTasks,
    Security,
)
from starlette.status import HTTP_200_OK

import app.crud.stats as crud_stats
from app.api.auth import authenticated

router = APIRouter()


@router.get(
    "/stats/material-types",
    response_model=dict,
    status_code=HTTP_200_OK,
    tags=["Statistics"],
)
async def get_material_types():
    material_types = await crud_stats.get_material_types()
    return material_types


@router.get(
    "/stats/material-type",
    response_model=dict,
    status_code=HTTP_200_OK,
    tags=["Statistics"],
)
async def get_material_type_stats():
    material_type_stats = await crud_stats.get_material_type_stats()
    return material_type_stats


@router.get(
    "/stats/search",
    response_model=dict,
    status_code=HTTP_200_OK,
    tags=["Statistics"],
)
async def get_search_stats():
    search_stats = await crud_stats.get_search_stats()
    return search_stats


@router.post(
    "/refresh-stats",
    dependencies=[Security(authenticated)],
    status_code=HTTP_200_OK,
    tags=["Statistics"],
)
async def post_refresh_stats(background_tasks: BackgroundTasks):
    background_tasks.add_task(crud_stats.run_search_stats)
