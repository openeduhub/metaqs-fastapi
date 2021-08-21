from typing import List

from fastapi import APIRouter
from starlette.status import HTTP_200_OK

from app.core.config import PORTAL_ROOT_ID
import app.crud.collection as crud_collection
import app.crud.stats as crud_stats
from app.crud.util import build_collection_tree

router = APIRouter()


@router.get(
    "/stats", response_model=List[dict], status_code=HTTP_200_OK, tags=["Statistics"],
)
async def get_stats():
    root_noderef_id = PORTAL_ROOT_ID

    portals = await crud_collection.get_portals(root_noderef_id=root_noderef_id)
    stats = await crud_stats.get_stats()

    tree = await build_collection_tree(portals=portals, root_noderef_id=root_noderef_id)

    return tree
