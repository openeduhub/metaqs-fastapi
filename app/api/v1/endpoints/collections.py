from typing import List
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    Path,
)
from starlette.exceptions import HTTPException
from starlette.responses import Response
from starlette.status import (
    HTTP_200_OK,
    HTTP_404_NOT_FOUND,
)

from app.api.util import (
    collections_filter_params,
    materials_filter_params,
    # PaginationParams,
    # pagination_params,
    # sort_params,
)

import app.crud.collection as crud_collection
from app.core.config import PORTAL_ROOT_ID
from app.crud import (
    MissingCollectionAttributeFilter,
    MissingMaterialAttributeFilter,
)
from app.crud.util import build_collection_tree
from app.models.elastic import ElasticResourceAttribute
from app.models.collection import (
    Collection,
    CollectionAttribute,
)
from app.models.collection_stats import CollectionMaterialsCount
from app.models.learning_material import LearningMaterial


router = APIRouter()


class CollectionNotFoundException(HTTPException):
    def __init__(self, noderef_id):
        super().__init__(
            status_code=HTTP_404_NOT_FOUND,
            detail=f"Collection with id '{noderef_id}' not found",
        )


@router.get(
    "/collections", tags=["Collections"],
)
async def get_collections():
    return await crud_collection.get_noderef_ids()


@router.get(
    "/collections/{noderef_id}/pending-materials/{missing_attr}",
    response_model=List[LearningMaterial],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"},},
    tags=["Collections"],
)
async def get_child_materials_with_missing_attributes(
    *,
    noderef_id: UUID = Path(..., example="94f22c9b-0d3a-4c1c-8987-4c8e83f3a92e"),
    missing_attr_filter: MissingMaterialAttributeFilter = Depends(
        materials_filter_params
    ),
    response: Response,
):
    materials = await crud_collection.get_child_materials_with_missing_attributes(
        noderef_id=noderef_id, missing_attr_filter=missing_attr_filter,
    )
    response.headers["X-Total-Count"] = str(len(materials))
    return materials


@router.get(
    "/collections/{noderef_id}/pending-subcollections/{missing_attr}",
    response_model=List[Collection],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"},},
    tags=["Collections"],
)
async def get_child_collections_with_missing_attributes(
    *,
    noderef_id: UUID = Path(..., example="94f22c9b-0d3a-4c1c-8987-4c8e83f3a92e"),
    missing_attr_filter: MissingCollectionAttributeFilter = Depends(
        collections_filter_params
    ),
    response: Response,
):
    collections = await crud_collection.get_child_collections_with_missing_attributes(
        noderef_id=noderef_id, missing_attr_filter=missing_attr_filter,
    )
    response.headers["X-Total-Count"] = str(len(collections))
    return collections


@router.get(
    "/collections/{noderef_id}/stats/descendant-collections-materials-counts",
    response_model=List[CollectionMaterialsCount],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"},},
    tags=["Collections"],
)
async def get_descendant_collections_materials_counts(
    *,
    noderef_id: UUID = Path(..., example="94f22c9b-0d3a-4c1c-8987-4c8e83f3a92e"),
    response: Response,
):
    descendant_collections = await crud_collection.get_many(
        ancestor_id=noderef_id,
        source_fields=[
            ElasticResourceAttribute.NODEREF_ID,
            CollectionAttribute.PATH,
            CollectionAttribute.TITLE,
        ],
    )
    materials_counts = await crud_collection.get_descendant_collections_materials_counts(
        ancestor_id=noderef_id,
    )

    descendant_collections = {
        collection.noderef_id: collection.title for collection in descendant_collections
    }
    stats = []
    errors = []
    for record in materials_counts.results:
        try:
            title = descendant_collections.pop(record.noderef_id)
        except KeyError:
            errors.append(record.noderef_id)
            continue

        stats.append(
            CollectionMaterialsCount(
                noderef_id=record.noderef_id,
                title=title,
                materials_count=record.materials_count,
            )
        )

    stats = [
        *[
            CollectionMaterialsCount(
                noderef_id=noderef_id, title=title, materials_count=0,
            )
            for (noderef_id, title) in descendant_collections.items()
        ],
        *stats,
    ]

    response.headers["X-Total-Count"] = str(len(stats))
    response.headers["X-Total-Errors"] = str(len(errors))
    return stats


@router.get(
    "/collections/tree",
    response_model=List[dict],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"},},
    tags=["Collections"],
)
async def get_portals(
    *, response: Response,
):
    root_noderef_id = PORTAL_ROOT_ID
    portals = await crud_collection.get_portals(root_noderef_id=root_noderef_id)
    response.headers["X-Total-Count"] = str(len(portals))
    tree = await build_collection_tree(portals=portals, root_noderef_id=root_noderef_id)
    return tree
