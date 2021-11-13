from fastapi import (
    APIRouter,
    BackgroundTasks,
    Security,
)
from starlette.status import HTTP_202_ACCEPTED

import app.dbt_analytics.analytics as analytics
from app.api.auth import authenticated

router = APIRouter()


@router.post(
    "/run-analytics",
    dependencies=[Security(authenticated)],
    status_code=HTTP_202_ACCEPTED,
    tags=["Analytics", "Authenticated"],
)
async def run_analytics(*, background_tasks: BackgroundTasks):
    background_tasks.add_task(analytics.run)
