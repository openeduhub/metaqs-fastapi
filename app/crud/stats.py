from typing import List

from elasticsearch_dsl.response import Response

from app.elastic import (
    Search,
    qbool,
    acomposite,
    A,
)
from .elastic import (
    base_filter,
    ResourceType,
    type_filter,
)


async def get_stats(size: int = 20000) -> List[dict]:

    s = Search().query(
        qbool(
            filter=[
                *type_filter[ResourceType.MATERIAL],
                *base_filter,
            ]
        )
    )
    s.aggs.bucket(
        "stats",
        acomposite(
            sources=[
                {"materialType": A("terms", field="i18n.de_DE.ccm:educationallearningresourcetype.keyword")},
                {"collection_id": A("terms", field="collections.nodeRef.id.keyword")}
            ],
            size=size,
        ),
    )

    response: Response = s[:0].execute()
    if response.success():
        return response.to_dict()["aggregations"]["stats"]["buckets"]
