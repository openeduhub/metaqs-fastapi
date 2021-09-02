from datetime import datetime
from typing import (
    Callable,
    List,
    Optional,
    Coroutine,
)
from uuid import UUID

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Query,
    Response,
    Security,
    Path,
    Depends,
)
from starlette.status import (
    HTTP_200_OK,
    HTTP_202_ACCEPTED,
    HTTP_404_NOT_FOUND,
)
from starlette_context import context

import app.crud.collection as crud_collection
import app.crud.seeds as crud_seeds
import app.crud.stats as crud_stats
from app.core.config import PORTAL_ROOT_ID
from app.api.auth import authenticated
from app.models.stats import StatsResponse
from app.pg.pg_utils import get_postgres
from app.pg.postgres import Postgres

router = APIRouter()


@router.get(
    "/stats/material-types",
    response_model=List[str],
    status_code=HTTP_200_OK,
    tags=["Statistics"],
)
async def get_material_types(response: Response):
    material_types = await crud_stats.get_material_types()

    response.headers["X-Query-Count"] = str(len(context.get("elastic_queries")))
    return material_types


@router.get(
    "/stats/search/material-type",
    response_model=dict,
    status_code=HTTP_200_OK,
    tags=["Statistics"],
)
async def get_search_stats_by_material_type(
    *, query_str: str = Query(..., min_length=3, max_length=50), response: Response
):
    search_stats = await crud_stats.get_search_stats_by_material_type(query_str)

    response.headers["X-Query-Count"] = str(len(context.get("elastic_queries")))
    return search_stats


@router.get(
    "/stats/{noderef_id}/material-type",
    response_model=dict,
    status_code=HTTP_200_OK,
    tags=["Statistics"],
)
async def get_material_type_stats(
    *,
    noderef_id: UUID = Path(..., examples=crud_collection.PORTALS),
    response: Response,
):
    material_type_stats = await crud_stats.get_material_type_stats(
        root_noderef_id=noderef_id
    )

    response.headers["X-Total-Count"] = str(len(material_type_stats))
    response.headers["X-Query-Count"] = str(len(context.get("elastic_queries")))
    return material_type_stats


def _dispatch_portal_tasks(
    noderef_id: UUID, f: Callable[[UUID], Coroutine], background_tasks: BackgroundTasks,
):
    if str(noderef_id) == PORTAL_ROOT_ID:
        for _, v in crud_collection.PORTALS.items():
            if v["value"] == PORTAL_ROOT_ID:
                continue
            background_tasks.add_task(f, noderef_id=v["value"])
    else:
        background_tasks.add_task(f, noderef_id=noderef_id)


@router.post(
    "/run-stats/{noderef_id}",
    dependencies=[Security(authenticated)],
    status_code=HTTP_202_ACCEPTED,
    tags=["Statistics"],
)
async def post_run_stats(
    *,
    noderef_id: UUID = Path(..., examples=crud_collection.PORTALS),
    background_tasks: BackgroundTasks,
):
    _dispatch_portal_tasks(
        noderef_id=noderef_id,
        f=crud_stats.run_stats,
        background_tasks=background_tasks,
    )


@router.post(
    "/seed-stats/{noderef_id}",
    dependencies=[Security(authenticated)],
    status_code=HTTP_202_ACCEPTED,
    tags=["Statistics"],
)
async def post_seed_stats(
    *,
    noderef_id: UUID = Path(..., examples=crud_collection.PORTALS),
    background_tasks: BackgroundTasks,
):
    _dispatch_portal_tasks(
        noderef_id=noderef_id,
        f=crud_seeds.seed_stats,
        background_tasks=background_tasks,
    )


@router.get(
    "/read-stats/{noderef_id}",
    response_model=StatsResponse,
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Statistics"],
)
async def get_read_stats(
    *,
    noderef_id: UUID = Path(
        ...,
        examples={
            k: v
            for k, v in crud_collection.PORTALS.items()
            if not v["value"] is PORTAL_ROOT_ID
        },
    ),
    at: Optional[datetime] = Query(
        None,
        examples={
            "latest": {"value": None},
            "filtered": {"value": datetime.now().strftime("%Y-%m-%dT%H:%M")},
        },
    ),
    postgres: Postgres = Depends(get_postgres),
):
    async with postgres.pool.acquire() as conn:
        stats = await crud_stats.read_stats(conn=conn, noderef_id=noderef_id, at=at)
        return stats


@router.get(
    "/read-stats/{noderef_id}/timeline",
    response_model=List[datetime],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Statistics"],
)
async def get_read_stats_timeline(
    *,
    noderef_id: UUID = Path(
        ...,
        examples={
            k: v
            for k, v in crud_collection.PORTALS.items()
            if not v["value"] is PORTAL_ROOT_ID
        },
    ),
    postgres: Postgres = Depends(get_postgres),
):
    async with postgres.pool.acquire() as conn:
        timeline = await crud_stats.read_stats_timeline(
            conn=conn, noderef_id=noderef_id
        )
        return timeline
