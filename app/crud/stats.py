# import asyncio
from collections import defaultdict
from pprint import pformat
from typing import Union
from uuid import UUID

from asyncpg import Connection
from elasticsearch_dsl.response import Response
from glom import merge

import app.crud.collection as crud_collection
from app.core.config import DEBUG

from app.elastic import Search
from app.elastic.utils import (
    merge_agg_response,
    merge_composite_agg_response,
)
from app.models.stats import StatType
from app.pg.queries import stats_latest
from app.core.logging import logger
from app.crud.elastic import ResourceType
from .elastic import (
    agg_materials_by_collection,
    agg_material_types,
    agg_material_types_by_collection,
    aggs_collection_validation,
    aggs_material_validation,
    query_collections,
    query_materials,
    search_materials,
)


async def run_stats_score(noderef_id: UUID, resource_type: ResourceType) -> dict:
    query, aggs = None, None
    if resource_type is ResourceType.COLLECTION:
        query, aggs = query_collections, aggs_collection_validation
    elif resource_type is ResourceType.MATERIAL:
        query, aggs = query_materials, aggs_material_validation

    s = Search().query(query(ancestor_id=noderef_id))
    for name, _agg in aggs.items():
        s.aggs.bucket(name, _agg)

    response: Response = s[:0].execute()

    if response.success():
        return {
            "total": response.hits.total.value,
            **{k: v["doc_count"] for k, v in response.aggregations.to_dict().items()},
        }


async def material_counts_by_type(root_noderef_id: UUID) -> dict:
    s = Search().query(query_materials(ancestor_id=root_noderef_id))
    s.aggs.bucket("material_types", agg_material_types_by_collection())
    s.aggs.bucket("totals", agg_materials_by_collection())

    response: Response = s[:0].execute()

    if response.success():

        def fold_material_types(carry, bucket):
            material_type = bucket["key"]["material_type"]
            if not material_type:
                material_type = "N/A"
            count = bucket["doc_count"]
            record = carry[bucket["key"]["noderef_id"]]
            record[material_type] = count

        # TODO: refactor algorithm
        stats = merge(
            response.aggregations.material_types.buckets,
            op=fold_material_types,
            init=lambda: defaultdict(dict),
        )

        totals = merge_composite_agg_response(
            response.aggregations.totals, key="noderef_id"
        )

        for noderef_id, counts in stats.items():
            counts["total"] = totals.get(noderef_id)

        return stats


async def search_hits_by_material_type(query_string: str) -> dict:
    s = Search().query(query_materials()).query(search_materials(query_string))
    s.aggs.bucket("material_types", agg_material_types())

    response: Response = s[:0].execute()

    if response.success():
        stats = merge_agg_response(response.aggregations.material_types)
        stats["total"] = sum(stats.values())
        return stats


async def run_stats_material_types(root_noderef_id: UUID) -> dict:
    portals = await crud_collection.get_many_sorted(root_noderef_id=root_noderef_id)
    material_counts = await material_counts_by_type(root_noderef_id=root_noderef_id)

    # TODO: refactor algorithm
    stats = {}
    for portal in portals:
        stats[str(portal.noderef_id)] = {
            "search": await search_hits_by_material_type(portal.title),
            "material_types": material_counts.get(str(portal.noderef_id), {}),
        }

    return stats


async def read_stats(
    conn: Connection, stat_type: StatType, noderef_id: UUID
) -> Union[dict, None]:
    row = await stats_latest(conn, stat_type, noderef_id)

    if row:
        if DEBUG:
            logger.debug(f"Read from postgres:\n{pformat(dict(row))}")

        return dict(row)
