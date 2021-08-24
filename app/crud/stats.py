from collections import defaultdict

from elasticsearch_dsl.response import Response
from glom import (
    glom,
    merge,
    Iter,
)

from app.core.config import ELASTIC_MAX_SIZE
from app.core.util import slugify
from app.elastic import (
    Search,
    qbool,
    acomposite,
    aterms,
    script,
)
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
            }
            """,
            params={
                "field": "i18n.de_DE.ccm:educationallearningresourcetype.keyword",
                "map": _MATERIAL_TYPES_MAP_EN_DE,
            },
        ),
    }
}


async def get_material_types() -> dict:
    s = (
        Search()
        .query(qbool(filter=[*type_filter[ResourceType.MATERIAL], *base_filter,]))
        .extra(runtime_mappings=_runtime_mapping_material_type)
    )
    s.aggs.bucket(
        "material_types", aterms(field="material_type", size=ELASTIC_MAX_SIZE)
    )

    response: Response = s[:0].execute()

    if response.success():
        return glom(
            response.aggregations.material_types.buckets,
            (
                Iter("key")
                .map(lambda k: {slugify(k): k}).all(),
                merge,
            ),
        )


async def get_material_type_stats(size: int = ELASTIC_MAX_SIZE) -> dict:
    s = (
        Search()
        .query(qbool(filter=[*type_filter[ResourceType.MATERIAL], *base_filter,]))
        .extra(runtime_mappings=_runtime_mapping_material_type)
    )
    s.aggs.bucket(
        "material_types", aterms(field="material_type", size=ELASTIC_MAX_SIZE)
    )
    s.aggs.bucket(
        "stats",
        acomposite(
            sources=[
                {"material_type": aterms(field="material_type")},
                {"noderef_id": aterms(field="collections.nodeRef.id.keyword")},
            ],
            size=size,
        ),
    )

    response: Response = s[:0].execute()

    if response.success():

        def group_results(carry, stat):
            material_type = stat["key"]["material_type"]
            count = stat["doc_count"]
            record = carry[stat["key"]["noderef_id"]]
            record[material_type] = count
            record["total"] = record["total"] + count

        return merge(
            response.aggregations.stats.buckets,
            op=group_results,
            init=lambda: defaultdict(lambda: {"total": 0}),
        )
