from typing import (
    Dict,
    List,
)

from fastapi import APIRouter
from starlette.status import HTTP_200_OK

import app.crud.stats as crud_stats

router = APIRouter()


@router.get(
    "/stats/material-types", response_model=dict, status_code=HTTP_200_OK, tags=["Statistics"],
)
async def get_material_types():
    material_types = await crud_stats.get_material_types()
    return material_types


@router.get(
    "/stats/material-type", response_model=dict, status_code=HTTP_200_OK, tags=["Statistics"],
)
async def get_material_type_stats():
    material_type_stats = await crud_stats.get_material_type_stats()
    return material_type_stats
