from typing import (
    Dict,
    List,
    Optional,
    Set,
)
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
)
from starlette.responses import Response
from starlette.status import (
    HTTP_200_OK,
    HTTP_404_NOT_FOUND,
)
from starlette_context import context

import app.crud.collection as crud_collection
from app.api.util import (
    collections_filter_params,
    collection_response_fields,
    filter_response_fields,
    portal_id_with_root_param,
)
from app.crud import MissingCollectionAttributeFilter
from app.crud.util import build_portal_tree
from app.models.collection import (
    Collection,
    CollectionAttribute,
    PortalTreeNode,
)

router = APIRouter()


@router.get(
    "/collections", tags=["Collections"],
)
async def get_portals():
    return await crud_collection.get_portals()


@router.get(
    "/collections/{noderef_id}/tree",
    response_model=List[PortalTreeNode],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Collections"],
)
async def get_portal_tree(
    *, noderef_id: UUID = Depends(portal_id_with_root_param), response: Response,
):
    collections = await crud_collection.get_many_sorted(root_noderef_id=noderef_id)
    tree = await build_portal_tree(collections=collections, root_noderef_id=noderef_id)
    response.headers["X-Total-Count"] = str(len(collections))
    response.headers["X-Query-Count"] = str(len(context.get("elastic_queries")))
    return tree


@router.get(
    "/collections/{noderef_id}/pending-subcollections/{missing_attr}",
    response_model=List[Collection],
    response_model_exclude_unset=True,
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Collections"],
)
async def filter_collections_with_missing_attributes(
    *,
    noderef_id: UUID = Depends(portal_id_with_root_param),
    missing_attr_filter: MissingCollectionAttributeFilter = Depends(
        collections_filter_params
    ),
    response_fields: Optional[Set[CollectionAttribute]] = Depends(
        collection_response_fields
    ),
    response: Response,
):
    if response_fields:
        response_fields.add(CollectionAttribute.NODEREF_ID)

    collections = await crud_collection.get_child_collections_with_missing_attributes(
        noderef_id=noderef_id,
        missing_attr_filter=missing_attr_filter,
        source_fields=response_fields,
    )

    response.headers["X-Total-Count"] = str(len(collections))
    response.headers["X-Query-Count"] = str(len(context.get("elastic_queries")))
    return filter_response_fields(collections, response_fields=response_fields)
