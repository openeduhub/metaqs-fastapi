from fastapi import (
    APIRouter,
    BackgroundTasks,
    Security,
)
from starlette.status import (
    HTTP_202_ACCEPTED,
    HTTP_204_NO_CONTENT,
)

import app.crud.collection as crud_collection
import app.crud.seeds as crud_seeds
from app.api.auth import authenticated

router = APIRouter()


@router.post(
    "/clear-stats",
    dependencies=[Security(authenticated)],
    status_code=HTTP_204_NO_CONTENT,
    tags=["Statistics", "Authenticated"],
)
async def clear_stats():
    await crud_seeds.clear_stats()


@router.post(
    "/seed-stats",
    dependencies=[Security(authenticated)],
    status_code=HTTP_202_ACCEPTED,
    tags=["Statistics", "Authenticated"],
)
async def seed_stats(*, background_tasks: BackgroundTasks):
    portals = await crud_collection.get_portals()
    for portal_id in portals.keys():
        background_tasks.add_task(crud_seeds.seed_stats, noderef_id=portal_id)
