import json
from datetime import datetime
from typing import (
    List,
    Optional,
)
from uuid import UUID

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    Path,
    Query,
    Response,
    Security,
)
from starlette.status import (
    HTTP_200_OK,
    HTTP_202_ACCEPTED,
    HTTP_404_NOT_FOUND,
)
from starlette_context import context

import app.crud.collection as crud_collection
import app.crud.stats as crud_stats
from app.api.auth import authenticated
from app.core.config import PORTAL_ROOT_ID
from app.crud.util import (
    StatsNotFoundException,
    dispatch_portal_tasks,
)
from app.models.collection import (
    CollectionAttribute,
    CollectionMaterialsCount,
    PortalTreeNode,
)
from app.models.oeh_validation import MaterialFieldValidation
from app.models.stats import (
    CollectionValidationStats,
    MaterialValidationStats,
    StatType,
    StatsResponse,
    ValidationStatsResponse,
)
from app.pg.pg_utils import get_postgres
from app.pg.postgres import Postgres

router = APIRouter()


def noderef_id_param(
    *, noderef_id: UUID = Path(..., examples=crud_collection.PORTALS),
) -> UUID:
    return noderef_id


def at_datetime_param(
    *,
    at: Optional[datetime] = Query(
        None,
        examples={
            "latest": {"value": None},
            "filtered": {"value": datetime.now().strftime("%Y-%m-%dT%H:%M")},
        },
    ),
) -> datetime:
    return at


@router.get(
    "/stats/search/material-type",
    response_model=dict,
    status_code=HTTP_200_OK,
    tags=["Statistics"],
)
async def search_hits_by_material_type(
    *, query_str: str = Query(..., min_length=3, max_length=50), response: Response
):
    search_stats = await crud_stats.search_hits_by_material_type(query_str)

    response.headers["X-Query-Count"] = str(len(context.get("elastic_queries")))
    return search_stats


@router.get(
    "/stats/{noderef_id}/material-type",
    response_model=dict,
    status_code=HTTP_200_OK,
    tags=["Statistics"],
)
async def material_counts_by_type(
    *, noderef_id: UUID = Depends(noderef_id_param), response: Response,
):
    material_counts = await crud_stats.material_counts_by_type(
        root_noderef_id=noderef_id
    )

    response.headers["X-Total-Count"] = str(len(material_counts))
    response.headers["X-Query-Count"] = str(len(context.get("elastic_queries")))
    return material_counts


@router.get(
    "/collections/{noderef_id}/stats/descendant-collections-materials-counts",
    response_model=List[CollectionMaterialsCount],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Statistics"],
)
async def material_counts_tree(
    *,
    noderef_id: UUID = Path(
        ...,
        examples={
            "Alle Fachportale": {"value": PORTAL_ROOT_ID},
            **crud_collection.PORTALS,
        },
    ),
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
    materials_counts = await crud_collection.material_counts_by_descendant(
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


async def _read_stats(
    postgres: Postgres, stat_type: StatType, noderef_id: UUID, at: datetime = None
) -> dict:
    async with postgres.pool.acquire() as conn:
        row = await crud_stats.read_stats(
            conn=conn, stat_type=stat_type, noderef_id=noderef_id, at=at
        )

    # if not row:
    #     row = await crud_stats.read_stats_file(
    #         noderef_id=noderef_id, stat_type=stat_type
    #     )

    return row


@router.get(
    "/read-stats/{noderef_id}",
    response_model=StatsResponse,
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Statistics"],
)
async def read_stats(
    *,
    noderef_id: UUID = Depends(noderef_id_param),
    at: Optional[datetime] = Depends(at_datetime_param),
    postgres: Postgres = Depends(get_postgres),
):
    row = await _read_stats(
        postgres, stat_type=StatType.MATERIAL_TYPES, noderef_id=noderef_id, at=at
    )

    if not row:
        raise StatsNotFoundException

    if not isinstance(row["stats"], dict):
        row["stats"] = json.loads(row["stats"])

    return StatsResponse(derived_at=row["derived_at"], stats=row["stats"])


@router.get(
    "/read-stats/{noderef_id}/validation",
    response_model=List[ValidationStatsResponse[MaterialValidationStats]],
    response_model_exclude_unset=True,
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Statistics"],
)
async def read_stats_validation(
    *,
    noderef_id: UUID = Depends(noderef_id_param),
    at: Optional[datetime] = Depends(at_datetime_param),
    postgres: Postgres = Depends(get_postgres),
):
    row = await _read_stats(
        postgres, stat_type=StatType.VALIDATION_MATERIALS, noderef_id=noderef_id, at=at
    )

    if not row:
        raise StatsNotFoundException

    if not isinstance(row["stats"], list):
        row["stats"] = json.loads(row["stats"])

    response = [
        ValidationStatsResponse[MaterialValidationStats](
            noderef_id=stat["noderef_id"],
            validation_stats=MaterialValidationStats(
                title=MaterialFieldValidation(missing=stat["missing_title"]),
                keywords=MaterialFieldValidation(missing=stat["missing_keywords"]),
                subjects=MaterialFieldValidation(missing=stat["missing_subjects"]),
                description=MaterialFieldValidation(
                    missing=stat["missing_description"]
                ),
                license=MaterialFieldValidation(missing=stat["missing_license"]),
                educontext=MaterialFieldValidation(missing=stat["missing_educontext"]),
                ads_qualifier=MaterialFieldValidation(
                    missing=stat["missing_ads_qualifier"]
                ),
                material_type=MaterialFieldValidation(
                    missing=stat["missing_material_type"]
                ),
                object_type=MaterialFieldValidation(
                    missing=stat["missing_object_type"]
                ),
            ),
        )
        for stat in row["stats"]
    ]

    return response


@router.get(
    "/read-stats/{noderef_id}/validation/collections",
    response_model=List[ValidationStatsResponse[CollectionValidationStats]],
    response_model_exclude_unset=True,
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Statistics"],
)
async def read_stats_validation_collection(
    *,
    noderef_id: UUID = Depends(noderef_id_param),
    at: Optional[datetime] = Depends(at_datetime_param),
    postgres: Postgres = Depends(get_postgres),
):
    row = await _read_stats(
        postgres,
        stat_type=StatType.VALIDATION_COLLECTIONS,
        noderef_id=noderef_id,
        at=at,
    )

    if not row:
        raise StatsNotFoundException

    if not isinstance(row["stats"], list):
        row["stats"] = json.loads(row["stats"])

    response = [
        ValidationStatsResponse[CollectionValidationStats](
            noderef_id=stat["noderef_id"],
            validation_stats=CollectionValidationStats(
                title=stat["title"],
                keywords=stat["keywords"],
                description=stat["description"],
                educontext=stat["educontext"],
            ),
        )
        for stat in row["stats"]
    ]

    return response


@router.get(
    "/read-stats/{noderef_id}/portal-tree",
    response_model=List[PortalTreeNode],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Statistics"],
)
async def read_stats_portal_tree(
    *,
    noderef_id: UUID = Depends(noderef_id_param),
    at: Optional[datetime] = Depends(at_datetime_param),
    postgres: Postgres = Depends(get_postgres),
):
    row = await _read_stats(
        postgres, stat_type=StatType.PORTAL_TREE, noderef_id=noderef_id, at=at
    )

    if not row:
        raise StatsNotFoundException

    if not isinstance(row["stats"], list):
        row["stats"] = json.loads(row["stats"])

    response = [PortalTreeNode.construct(**node) for node in row["stats"]]

    return response


@router.get(
    "/read-stats/{noderef_id}/timeline",
    response_model=List[datetime],
    status_code=HTTP_200_OK,
    responses={HTTP_404_NOT_FOUND: {"description": "Collection not found"}},
    tags=["Statistics"],
)
async def read_stats_timeline(
    *,
    noderef_id: UUID = Depends(noderef_id_param),
    postgres: Postgres = Depends(get_postgres),
):
    async with postgres.pool.acquire() as conn:
        return await crud_stats.read_stats_timeline(conn=conn, noderef_id=noderef_id)


@router.post(
    "/run-stats",
    dependencies=[Security(authenticated)],
    status_code=HTTP_202_ACCEPTED,
    tags=["Statistics", "Authenticated"],
)
async def run_stats(*, background_tasks: BackgroundTasks):
    dispatch_portal_tasks(
        noderef_id=PORTAL_ROOT_ID,
        f=crud_stats.run_stats,
        background_tasks=background_tasks,
    )
