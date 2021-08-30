from collections import defaultdict
from datetime import datetime
from uuid import UUID

from aiofiles import open
from elasticsearch_dsl.response import Response
from fastapi import HTTPException
from glom import (
    glom,
    merge,
    Iter,
)

import app.crud.collection as crud_collection
from app.core.config import (
    DATA_DIR,
    ELASTIC_MAX_SIZE,
)
from app.core.util import slugify
from app.crud.util import CollectionNotFoundException
from app.elastic import (
    Search,
    qbool,
    qsimplequerystring,
    qterm,
    acomposite,
    aterms,
    script,
)
from app.models.learning_material import LearningMaterialAttribute
from app.models.stats import StatsResponse
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
        Search()
        .query(qbool(filter=qfilter))
        .extra(runtime_mappings=_runtime_mapping_material_type)
    )

    return s


async def get_material_types() -> dict:
    s = _base_query_material_types()
    s.aggs.bucket(
        "material_types",
        aterms(qfield="material_type", missing="N/A", size=ELASTIC_MAX_SIZE),
    )

    response: Response = s[:0].execute()

    if response.success():
        return glom(
            response.aggregations.material_types.buckets,
            (Iter("key").map(lambda k: {slugify(k): k}).all(), merge,),
        )


async def material_types_lut() -> dict:
    mt = await get_material_types()
    return {v: k for k, v in mt.items()}


async def get_material_type_stats(
    root_noderef_id: UUID, size: int = ELASTIC_MAX_SIZE,
) -> dict:
    s = _base_query_material_types(ancestor_id=root_noderef_id)
    s.aggs.bucket(
        "stats",
        acomposite(
            sources=[
                {"material_type": aterms(qfield="material_type")},
                {
                    "noderef_id": aterms(
                        qfield=LearningMaterialAttribute.COLLECTION_NODEREF_ID
                    )
                },
            ],
            size=size,
        ),
    )

    response: Response = s[:0].execute()

    if response.success():
        lut = await material_types_lut()

        def group_results(carry, stat):
            material_type = lut[stat["key"]["material_type"]]
            count = stat["doc_count"]
            record = carry[stat["key"]["noderef_id"]]
            record[material_type] = count
            record["total"] = record["total"] + count

        return merge(
            response.aggregations.stats.buckets,
            op=group_results,
            init=lambda: defaultdict(lambda: {"total": 0}),
        )


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
        )
    )
    s.aggs.bucket(
        "material_types",
        aterms(qfield="material_type", missing="N/A", size=ELASTIC_MAX_SIZE),
    )

    response: Response = s[:0].execute()

    if response.success():
        lut = await material_types_lut()
        stats = {"total": 0}
        for b in response.aggregations.material_types.buckets:
            stats[lut[b["key"]]] = b["doc_count"]
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

    timestamp = datetime.now()

    try:
        async with open(DATA_DIR / str(noderef_id), mode="w") as f:
            await f.write(
                StatsResponse.parse_obj({"stats": stats, "timestamp": timestamp}).json()
            )
    except OSError:
        raise HTTPException(status_code=500)


async def read_stats(noderef_id: UUID) -> StatsResponse:
    try:
        async with open(DATA_DIR / str(noderef_id), mode="r") as f:
            content = await f.read()
    except FileNotFoundError:
        raise CollectionNotFoundException(noderef_id)

    return StatsResponse.parse_raw(content)
