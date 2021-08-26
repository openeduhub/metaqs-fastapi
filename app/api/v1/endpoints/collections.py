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

_portal_ids = {
    "Physik": {"value": "93f22c9b-0d3a-4c1c-8987-4c8e83f3a92e"},
    "Mathematik": {"value": "bd8be6d5-0fbe-4534-a4b3-773154ba6abc"},
    "Biologie": {"value": "15fce411-54d9-467f-8f35-61ea374a298d"},
    "Chemie": {"value": "4940d5da-9b21-4ec0-8824-d16e0409e629"},
    "Deutsch": {"value": "69f9ff64-93da-4d68-b849-ebdf9fbdcc77"},
    "DaZ": {"value": "26a336bf-51c8-4b91-9a6c-f1cf67fd4ae4"},
    "Englisch": {"value": "15dbd166-fd31-4e01-aabd-524cfa4d2783"},
    "Informatik": {"value": "742d8c87-e5a3-4658-86f9-419c2cea6574"},
    "Kunst": {"value": "6a3f5881-cce0-4e8d-b123-26392b3f1c19"},
    "Religion": {"value": "66c667bc-8777-4c57-b476-35f54ce9ff5d"},
    "Geschichte": {"value": "324f24e3-6687-4e89-b8dd-2bd0e20ff733"},
    "Medienbildung": {"value": "eef047a3-58ba-419c-ab7d-3d0cfd04bb4e"},
    "Politische Bildung": {"value": "ffd298b5-3a04-4d13-9d26-ddd5d3b5cedc"},
    "Sport": {"value": "ea776a48-b3f4-446c-b871-19f84b31d280"},
    "Darstellendes Spiel": {"value": "7998f334-9311-491e-9a58-72baf2a7efd2"},
    "Spanisch": {"value": "11bdb8a0-a9f5-4028-becc-cbf8e328dd4b"},
    "Tuerkisch": {"value": "26105802-9039-4add-bf21-07a0f89f6e70"},
    "Nachhaltigkeit": {"value": "d0ed50e6-a49f-4566-8f3b-c545cdf75067"},
    "OER": {"value": "a87c092d-e3b5-43ef-81db-757ab1967646"},
    "Zeitgemaesse Bildung": {"value": "a3291cd2-5fe4-444e-9b7b-65807d9b0024"},
    "Wirtschaft": {"value": "f0109e16-a8fc-48b5-9461-369571fd59f2"},
    "Geografie": {"value": "f1049950-bdda-45f5-9c73-38b51ea66c74"},
    "Paedagogik": {"value": "7e2a3536-8441-4328-8ee6-ab0068bb13f8"},
    "Franzoesisch": {"value": "86b990ef-0955-45ad-bdae-ec2623cf0e1a"},
    "Musik": {"value": "2eda0065-f69b-46c8-ae09-d258c8226a5e"},
    "Philosophie": {"value": "9d364fd0-4374-40b4-a153-3c722b9cda35"},
}


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
    noderef_id: UUID = Path(..., examples=_portal_ids),
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
    noderef_id: UUID = Path(..., examples=_portal_ids),
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
    noderef_id: UUID = Path(..., examples=_portal_ids),
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
