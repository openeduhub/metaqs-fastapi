from typing import (
    List,
    Optional,
    Set,
)
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    Path,
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
    materials_filter_params,
    material_response_fields,
    # PaginationParams,
    # pagination_params,
    # sort_params,
)
from app.crud import (
    MissingCollectionAttributeFilter,
    MissingMaterialAttributeFilter,
)
from app.crud.util import build_portal_tree
from app.models.collection import (
    Collection,
    CollectionAttribute,
    PortalTreeNode,
)
from app.models.collection_stats import CollectionMaterialsCount
from app.models.learning_material import (
    LearningMaterial,
    LearningMaterialAttribute,
)


router = APIRouter()


@router.get(
    "/collections", tags=["Collections"],
)
async def get_portals():
    return await crud_collection.get_portals()


@router.get(
    "/collections/{noderef_id}/pending-materials/{missing_attr}",
    response_model=List[LearningMaterial],
    response_model_exclude_unset=True,
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Collections"],
)
async def get_child_materials_with_missing_attributes(
    *,
    noderef_id: UUID = Path(..., examples=crud_collection.PORTALS),
    missing_attr_filter: MissingMaterialAttributeFilter = Depends(
        materials_filter_params
    ),
    response_fields: Optional[Set[LearningMaterialAttribute]] = Depends(
        material_response_fields
    ),
    response: Response,
):
    if response_fields:
        response_fields.add(LearningMaterialAttribute.NODEREF_ID)

    materials = await crud_collection.get_child_materials_with_missing_attributes(
        noderef_id=noderef_id,
        missing_attr_filter=missing_attr_filter,
        source_fields=response_fields,
    )

    response.headers["X-Total-Count"] = str(len(materials))
    response.headers["X-Query-Count"] = str(len(context.get("elastic_queries")))
    return filter_response_fields(materials, response_fields=response_fields)


@router.get(
    "/collections/{noderef_id}/pending-subcollections/{missing_attr}",
    response_model=List[Collection],
    response_model_exclude_unset=True,
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Collections"],
)
async def get_child_collections_with_missing_attributes(
    *,
    noderef_id: UUID = Path(..., examples=crud_collection.PORTALS),
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


@router.get(
    "/collections/{noderef_id}/stats/descendant-collections-materials-counts",
    response_model=List[CollectionMaterialsCount],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Collections"],
)
async def get_descendant_collections_materials_counts(
    *,
    noderef_id: UUID = Path(..., examples=crud_collection.PORTALS),
    response: Response,
):
    descendant_collections = await crud_collection.get_many(
        ancestor_id=noderef_id,
        source_fields={
            CollectionAttribute.NODEREF_ID,
            CollectionAttribute.PATH,
            CollectionAttribute.TITLE,
        },
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
    response.headers["X-Query-Count"] = str(len(context.get("elastic_queries")))
    # response.headers["X-Total-Errors"] = str(len(errors))
    return stats


@router.get(
    "/collections/{noderef_id}/tree",
    response_model=List[PortalTreeNode],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Collections"],
)
async def get_portal_tree(
    *,
    noderef_id: UUID = Path(..., examples=crud_collection.PORTALS),
    response: Response,
):
    portals = await crud_collection.get_portals_sorted(root_noderef_id=noderef_id)
    tree = await build_portal_tree(portals=portals, root_noderef_id=noderef_id)
    response.headers["X-Total-Count"] = str(len(portals))
    response.headers["X-Query-Count"] = str(len(context.get("elastic_queries")))
    return tree
