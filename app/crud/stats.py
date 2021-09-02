import json
from collections import defaultdict
from datetime import datetime
from typing import List
from uuid import UUID

from aiofiles import open
from asyncpg import Connection
from elasticsearch_dsl.response import Response
from fastapi import HTTPException
from glom import (
    glom,
    merge,
    Iter,
)
from sqlalchemy import literal_column, select

import app.crud.collection as crud_collection
from app.core.config import (
    DATA_DIR,
    ELASTIC_MAX_SIZE,
)

# from app.core.util import slugify
from app.crud import compile_query
from app.crud.util import CollectionNotFoundException
from app.elastic import (
    Search,
    acomposite,
    aterms,
    qbool,
    qsimplequerystring,
    qterm,
    script,
)
from app.models.learning_material import LearningMaterialAttribute
from app.models.stats import StatsResponse
from app.pg.metadata import Stats
from app.pg.pg_utils import get_postgres
from app.pg.queries import stats_latest
from .elastic import (
    base_filter,
    ResourceType,
    type_filter,
)

_MATERIAL_TYPES_MAP_EN_DE = {
    "other web resource": "Andere Web Ressource",
    "other asset type": "Anderer Ressourcentyp",
    "other": "Anderes Material",
    "application": "Anwendung/Software",
    "worksheet": "Arbeitsblatt",
    "audio": "Audio",
    "audiovisual medium": "Audiovisuelles Medium",
    "image": "Bild",
    "data": "Daten",
    "exploration": "Entdeckendes Lernen",
    "experiment": "Experiment",
    "case_study": "Fallstudie",
    "glossary": "Glossar",
    "guide": "Handbuch",
    "map": "Karte",
    "course": "Kurs",
    "assessment": "Lernkontrolle",
    "educational Game": "Lernspiel",
    "model": "Modell",
    "open activity": "Offene Aktivität",
    "presentation": "Präsentation",
    "reference": "Primärmaterial/Quelle",
    "project": "Projekt",
    "broadcast": "Radio/TV",
    "enquiry-oriented activity": "Recherche-Auftrag",
    "role play": "Rollenspiel",
    "simulation": "Simulation",
    "text": "Text",
    "drill and practice": "Übung",
    "teaching module": "Unterrichtsbaustein",
    "lesson plan": "Unterrichtsplanung",
    "demonstration": "Veranschaulichung",
    "video": "Video",
    "weblog": "Weblog",
    "web page": "Website",
    "tool": "Werkzeug",
    "wiki": "Wiki",
}

_runtime_mapping_material_type = {
    "material_type": {
        "type": "keyword",
        "script": script(
            """
            if (doc.containsKey(params.field) && !doc[params.field].empty) {
                if (params.map.containsKey(doc[params.field].value)) {
                    emit(params.map.get(doc[params.field].value));
                } else {
                    emit(doc[params.field].value);
                }
            } else {
                    emit("N/A");
            }
            """,
            params={
                "field": "i18n.de_DE.ccm:educationallearningresourcetype.keyword",
                "map": _MATERIAL_TYPES_MAP_EN_DE,
            },
        ),
    }
}


def _base_query_material_types(ancestor_id: UUID = None) -> Search:
    qfilter = [*base_filter, *type_filter[ResourceType.MATERIAL]]
    if ancestor_id:
        qfilter.append(
            qterm(qfield=LearningMaterialAttribute.COLLECTION_PATH, value=ancestor_id)
        )

    s = (
        Search().query(qbool(filter=qfilter))
        # .extra(runtime_mappings=_runtime_mapping_material_type)
    )

    return s


async def get_material_types() -> List[str]:
    s = _base_query_material_types()
    s.aggs.bucket(
        "material_types",
        aterms(
            qfield=LearningMaterialAttribute.LEARNINGRESOURCE_TYPE,
            missing="N/A",
            size=ELASTIC_MAX_SIZE,
        ),
    )

    response: Response = s[:0].execute()

    if response.success():
        return glom(
            response.aggregations.material_types.buckets,
            # (Iter("key").map(lambda k: {slugify(k): k}).all(), merge,),
            Iter("key").all(),
        )


# async def material_types_lut() -> dict:
#     mt = await get_material_types()
#     return {v: k for k, v in mt.items()}


async def get_material_type_stats(
    root_noderef_id: UUID, size: int = ELASTIC_MAX_SIZE,
) -> dict:
    s = _base_query_material_types(ancestor_id=root_noderef_id)
    s.aggs.bucket(
        "material_types",
        acomposite(
            sources=[
                # {"material_type": aterms(qfield="material_type")},
                {
                    "material_type": aterms(
                        qfield=LearningMaterialAttribute.LEARNINGRESOURCE_TYPE,
                        missing_bucket=True,
                    )
                },
                {
                    "noderef_id": aterms(
                        qfield=LearningMaterialAttribute.COLLECTION_NODEREF_ID
                    )
                },
            ],
            size=size,
        ),
    )
    s.aggs.bucket(
        "totals",
        acomposite(
            sources=[
                {
                    "noderef_id": aterms(
                        qfield=LearningMaterialAttribute.COLLECTION_NODEREF_ID
                    )
                }
            ],
            size=size,
        ),
    )

    response: Response = s[:0].execute()

    if response.success():
        # lut = await material_types_lut()

        def fold_material_types(carry, bucket):
            # material_type = lut[stat["key"]["material_type"]]
            material_type = bucket["key"]["material_type"]
            if not material_type:
                material_type = "N/A"
            count = bucket["doc_count"]
            record = carry[bucket["key"]["noderef_id"]]
            record[material_type] = count

        stats = merge(
            response.aggregations.material_types.buckets,
            op=fold_material_types,
            init=lambda: defaultdict(dict),
        )

        def fold_totals(carry, bucket):
            carry[bucket["key"]["noderef_id"]] = bucket["doc_count"]

        totals = merge(
            response.aggregations.totals.buckets,
            op=fold_totals
        )

        for noderef_id, counts in stats.items():
            counts["total"] = totals.get(noderef_id)

        return stats


async def get_search_stats_by_material_type(query_string: str) -> dict:
    s = _base_query_material_types().query(
        qsimplequerystring(
            query=query_string,
            qfields=[
                LearningMaterialAttribute.TITLE,
                LearningMaterialAttribute.KEYWORDS,
                LearningMaterialAttribute.DESCRIPTION,
                LearningMaterialAttribute.CONTENT_FULLTEXT,
            ],
            default_operator="and"
        )
    )
    s.aggs.bucket(
        "material_types",
        aterms(
            qfield=LearningMaterialAttribute.LEARNINGRESOURCE_TYPE,
            missing="N/A",
            size=ELASTIC_MAX_SIZE,
        ),
    )

    response: Response = s[:0].execute()

    if response.success():
        # lut = await material_types_lut()
        stats = {"total": 0}
        for b in response.aggregations.material_types.buckets:
            # stats[lut[b["key"]]] = b["doc_count"]
            stats[b["key"]] = b["doc_count"]
            stats["total"] = stats["total"] + b["doc_count"]

        return stats


async def run_stats(noderef_id: UUID):
    portals = await crud_collection.get_portals_sorted(root_noderef_id=noderef_id)
    stats_material_types = await get_material_type_stats(root_noderef_id=noderef_id)

    stats = {}
    for portal in portals:
        stats[str(portal.noderef_id)] = {
            "search": await get_search_stats_by_material_type(portal.title),
            "material_types": stats_material_types.get(str(portal.noderef_id), {}),
        }

    postgres = await get_postgres()
    async with postgres.pool.acquire() as conn:
        query = (
            Stats.insert()
            .values(noderef_id=noderef_id, stats=stats, derived_at=datetime.now())
            .returning(literal_column("*"))
        )
        compiled_query, params, _ = compile_query(query)
        row = await conn.fetchrow(compiled_query, *params)

    try:
        async with open(DATA_DIR / str(row["noderef_id"]), mode="w") as f:
            await f.write(
                StatsResponse(
                    derived_at=row["derived_at"], stats=json.loads(row["stats"])
                ).json()
            )
    except OSError:
        raise HTTPException(status_code=500)


async def read_stats(
    conn: Connection, noderef_id: UUID, at: datetime = None
) -> StatsResponse:
    query = stats_latest(noderef_id, at=at)
    compiled_query, params, _ = compile_query(query)
    row = await conn.fetchrow(compiled_query, *params)

    if row:
        return StatsResponse(
            derived_at=row["derived_at"], stats=json.loads(row["stats"])
        )

    try:
        async with open(DATA_DIR / str(noderef_id), mode="r") as f:
            content = await f.read()
    except FileNotFoundError:
        raise CollectionNotFoundException(noderef_id)

    return StatsResponse.parse_raw(content)


async def read_stats_timeline(conn: Connection, noderef_id: UUID) -> List[datetime]:
    query = (
        select(Stats.c.derived_at)
        .where(Stats.c.noderef_id == noderef_id)
        .order_by(Stats.c.derived_at.desc())
    )
    compiled_query, params, _ = compile_query(query)
    rows = await conn.fetch(compiled_query, *params)

    if rows:
        return [row["derived_at"] for row in rows]

    raise CollectionNotFoundException(noderef_id)
