from enum import Enum
from typing import Optional
from uuid import UUID

from elasticsearch_dsl.aggs import Agg
from elasticsearch_dsl.query import Query, Q
from elasticsearch_dsl.response import AggResponse

from app.core.config import ELASTIC_MAX_SIZE
from app.elastic import (
    acomposite,
    afilter,
    amissing,
    aterms,
    qbool,
    qboolor,
    qmatch,
    qnotexists,
    qsimplequerystring,
    qterm,
    qterms,
    script,
)
from app.elastic.utils import handle_text_field
from app.models.collection import CollectionAttribute
from app.models.elastic import ElasticResourceAttribute
from app.models.learning_material import LearningMaterialAttribute
from app.models.oeh_validation import OehValidationError

MATERIAL_TYPES_MAP_EN_DE = {
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

runtime_mappings_material_type = {
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
                "field": handle_text_field(
                    LearningMaterialAttribute.LEARNINGRESOURCE_TYPE_DE
                ),
                "map": MATERIAL_TYPES_MAP_EN_DE,
            },
        ),
    }
}

# TODO: parameterize Attribute model
runtime_mappings_collection_validation = {
    "char_count_title": {
        "type": "long",
        "script": script(
            """
            if (doc.containsKey(params.field) && !doc[params.field].empty) {
                emit(doc[params.field].value.length());
            }
            """,
            params={"field": handle_text_field(CollectionAttribute.TITLE)},
        ),
    },
    "token_count_keywords": {
        "type": "long",
        "script": script(
            """
            if (doc.containsKey(params.field) && !doc[params.field].empty) {
                emit(doc[params.field].length);
            }
            """,
            params={"field": handle_text_field(CollectionAttribute.KEYWORDS)},
        ),
    },
    "char_count_description": {
        "type": "long",
        "script": script(
            """
            if (doc.containsKey(params.field) && !doc[params.field].empty) {
                emit(doc[params.field].value.length());
            }
            """,
            params={"field": handle_text_field(CollectionAttribute.DESCRIPTION)},
        ),
    },
}


class ResourceType(str, Enum):
    COLLECTION = "COLLECTION"
    MATERIAL = "MATERIAL"


base_filter = [
    qterm(qfield=ElasticResourceAttribute.PERMISSION_READ, value="GROUP_EVERYONE"),
    qterm(qfield=ElasticResourceAttribute.EDU_METADATASET, value="mds_oeh"),
    qterm(qfield=ElasticResourceAttribute.PROTOCOL, value="workspace"),
]

type_filter = {
    ResourceType.COLLECTION: [
        qterm(qfield=ElasticResourceAttribute.TYPE, value="ccm:map"),
    ],
    ResourceType.MATERIAL: [
        qterm(qfield=ElasticResourceAttribute.TYPE, value="ccm:io"),
    ],
}


# TODO: eliminate; use query_many instead
def get_many_base_query(
    resource_type: ResourceType, ancestor_id: Optional[UUID] = None,
) -> dict:
    query_dict = {"filter": [*base_filter, *type_filter[resource_type]]}

    if ancestor_id:
        prefix = "collections." if resource_type == ResourceType.MATERIAL else ""
        query_dict["should"] = [
            qmatch(**{f"{prefix}path": ancestor_id}),
            qmatch(**{f"{prefix}nodeRef.id": ancestor_id}),
        ]
        query_dict["minimum_should_match"] = 1

    return query_dict


def query_many(resource_type: ResourceType, ancestor_id: UUID = None) -> Query:
    qfilter = [*base_filter, *type_filter[resource_type]]
    if ancestor_id:
        if resource_type is ResourceType.COLLECTION:
            qfilter.append(qterm(qfield=CollectionAttribute.PATH, value=ancestor_id))
        elif resource_type is ResourceType.MATERIAL:
            qfilter.append(
                qterm(
                    qfield=LearningMaterialAttribute.COLLECTION_PATH, value=ancestor_id
                )
            )

    return qbool(filter=qfilter)


def query_collections(ancestor_id: UUID = None) -> Query:
    return query_many(ResourceType.COLLECTION, ancestor_id=ancestor_id)


def query_materials(ancestor_id: UUID = None) -> Query:
    return query_many(ResourceType.MATERIAL, ancestor_id=ancestor_id)


def query_missing_material_license() -> Query:
    qfield = LearningMaterialAttribute.LICENSES
    return qboolor(
        [
            qterms(qfield=qfield, values=["UNTERRICHTS_UND_LEHRMEDIEN", "NONE", ""],),
            qnotexists(qfield=qfield),
        ]
    )


def search_materials(query_str: str) -> Query:
    return qsimplequerystring(
        query=query_str,
        qfields=[
            LearningMaterialAttribute.TITLE,
            LearningMaterialAttribute.KEYWORDS,
            LearningMaterialAttribute.DESCRIPTION,
            LearningMaterialAttribute.CONTENT_FULLTEXT,
            LearningMaterialAttribute.SUBJECTS_DE,
            LearningMaterialAttribute.LEARNINGRESOURCE_TYPE_DE,
            LearningMaterialAttribute.EDUCONTEXT_DE,
            LearningMaterialAttribute.EDUENDUSERROLE_DE,
        ],
        default_operator="and",
    )


def agg_materials_by_collection(size: int = ELASTIC_MAX_SIZE) -> Agg:
    return acomposite(
        sources=[
            {
                "noderef_id": aterms(
                    qfield=LearningMaterialAttribute.COLLECTION_NODEREF_ID
                )
            }
        ],
        size=size,
    )


def agg_material_types(size: int = ELASTIC_MAX_SIZE) -> Agg:
    return aterms(
        qfield=LearningMaterialAttribute.LEARNINGRESOURCE_TYPE,
        missing="N/A",
        size=size,
    )


def agg_material_types_by_collection(size: int = ELASTIC_MAX_SIZE) -> Agg:
    return acomposite(
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
    )


def agg_collection_validation(size: int = ELASTIC_MAX_SIZE) -> Agg:
    agg = aterms(qfield=CollectionAttribute.NODEREF_ID, size=size)
    agg.bucket("missing_title", amissing(qfield=CollectionAttribute.TITLE))
    agg.bucket("short_title", afilter(Q("range", char_count_title={"gt": 0, "lt": 5})))
    agg.bucket("missing_keywords", amissing(qfield=CollectionAttribute.KEYWORDS))
    agg.bucket(
        "few_keywords", afilter(Q("range", token_count_keywords={"gt": 0, "lt": 3}))
    )
    agg.bucket("missing_description", amissing(qfield=CollectionAttribute.DESCRIPTION))
    agg.bucket(
        "short_description",
        afilter(Q("range", char_count_description={"gt": 0, "lt": 30})),
    )
    agg.bucket("missing_educontext", amissing(qfield=CollectionAttribute.EDUCONTEXT))

    return agg


def parse_agg_collection_validation_response(agg_response: AggResponse):
    return [
        {
            "noderef_id": bucket["key"],
            "title": list(
                filter(
                    None,
                    [
                        OehValidationError.MISSING
                        if bucket["missing_title"]["doc_count"]
                        else None,
                        OehValidationError.TOO_SHORT
                        if bucket["short_title"]["doc_count"]
                        else None,
                    ],
                )
            ),
            "keywords": list(
                filter(
                    None,
                    [
                        OehValidationError.MISSING
                        if bucket["missing_keywords"]["doc_count"]
                        else None,
                        OehValidationError.TOO_FEW
                        if bucket["few_keywords"]["doc_count"]
                        else None,
                    ],
                )
            ),
            "description": list(
                filter(
                    None,
                    [
                        OehValidationError.MISSING
                        if bucket["missing_description"]["doc_count"]
                        else None,
                        OehValidationError.TOO_SHORT
                        if bucket["short_description"]["doc_count"]
                        else None,
                    ],
                )
            ),
            "educontext": [OehValidationError.MISSING]
            if bucket["missing_educontext"]["doc_count"]
            else [],
        }
        for bucket in agg_response.to_dict()["buckets"]
    ]


def agg_material_validation(size: int = ELASTIC_MAX_SIZE) -> Agg:
    agg = aterms(qfield=LearningMaterialAttribute.COLLECTION_NODEREF_ID, size=size)
    agg.bucket("missing_title", amissing(qfield=LearningMaterialAttribute.TITLE))
    agg.bucket("missing_keywords", amissing(qfield=LearningMaterialAttribute.KEYWORDS))
    agg.bucket("missing_subjects", amissing(qfield=LearningMaterialAttribute.SUBJECTS))
    agg.bucket(
        "missing_description", amissing(qfield=LearningMaterialAttribute.DESCRIPTION)
    )
    agg.bucket("missing_license", afilter(query=query_missing_material_license()))
    agg.bucket(
        "missing_educontext", amissing(qfield=LearningMaterialAttribute.EDUCONTEXT)
    )
    agg.bucket(
        "missing_ads_qualifier", amissing(qfield=LearningMaterialAttribute.CONTAINS_ADS)
    )
    agg.bucket(
        "missing_material_type",
        amissing(qfield=LearningMaterialAttribute.LEARNINGRESOURCE_TYPE),
    )
    agg.bucket(
        "missing_object_type", amissing(qfield=LearningMaterialAttribute.OBJECT_TYPE)
    )

    return agg


def parse_agg_material_validation_response(agg_response: AggResponse):
    # TODO: refactor algorithm
    return [
        {
            "noderef_id": bucket["key"],
            **{
                k: v["doc_count"]
                for k, v in bucket.items()
                if k not in ("key", "doc_count")
            },
        }
        for bucket in agg_response.to_dict()["buckets"]
    ]
