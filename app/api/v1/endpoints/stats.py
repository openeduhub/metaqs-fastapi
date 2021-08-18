from fastapi import (
    APIRouter,
)
from starlette.status import (
    HTTP_200_OK,
)

from app.crud.stats import get_stats

router = APIRouter()


@router.get(
    "/stats",
    response_model=dict,
    status_code=HTTP_200_OK,
    tags=["Statistics"],
)
async def get_descendant_collections_materials_counts():
    stats = await get_stats()
    return stats
