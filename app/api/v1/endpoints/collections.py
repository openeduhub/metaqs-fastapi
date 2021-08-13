from typing import List
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
from app.crud import (
    MissingCollectionAttributeFilter,
    MissingMaterialAttributeFilter,
)
from app.models.collection import Collection
from app.models.learning_material import LearningMaterial


router = APIRouter()


class CollectionNotFoundException(HTTPException):
    def __init__(self, node_ref_id):
        super().__init__(
            status_code=HTTP_404_NOT_FOUND,
            detail=f"Collection with id '{node_ref_id}' not found",
        )


@router.get(
    "/collections", tags=["Collections"],
)
async def get_collections():
    return await crud_collection.get_node_ref_ids()


# @router.get(
#     "/collections/{node_ref_id}", tags=["Collections"],
# )
# async def get_collection(node_ref_id: str):
#     collection = await crud_collection.get_single(node_ref_id=node_ref_id)
#     return {"collection": collection.as_dict()}


@router.get(
    "/collections/{node_ref_id}/pending-materials/{missing_attr}",
    response_model=List[LearningMaterial],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"},},
    tags=["Collections"],
)
async def get_child_materials_with_missing_attributes(
    *,
    node_ref_id: str = Path(..., example="94f22c9b-0d3a-4c1c-8987-4c8e83f3a92e"),
    missing_attr_filter: MissingMaterialAttributeFilter = Depends(
        materials_filter_params
    ),
    response: Response,
):
    materials = await crud_collection.get_child_materials_with_missing_attributes(
        collection_id=node_ref_id, missing_attr_filter=missing_attr_filter,
    )
    response.headers["X-Total-Count"] = str(len(materials))
    return materials


@router.get(
    "/collections/{node_ref_id}/pending-subcollections/{missing_attr}",
    response_model=List[Collection],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"},},
    tags=["Collections"],
)
async def get_child_collections_with_missing_attributes(
    *,
    node_ref_id: str = Path(..., example="94f22c9b-0d3a-4c1c-8987-4c8e83f3a92e"),
    missing_attr_filter: MissingCollectionAttributeFilter = Depends(
        collections_filter_params
    ),
    response: Response,
):
    collections = await crud_collection.get_child_collections_with_missing_attributes(
        collection_id=node_ref_id, missing_attr_filter=missing_attr_filter,
    )
    response.headers["X-Total-Count"] = str(len(collections))
    return collections
