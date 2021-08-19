from collections import defaultdict

from elasticsearch_dsl.response import Response
from glom import glom, merge

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


async def get_stats(size: int = 20000) -> dict:

    s = Search().query(
        qbool(filter=[*type_filter[ResourceType.MATERIAL], *base_filter,])
    )
    s.aggs.bucket(
        "stats",
        acomposite(
            sources=[
                {
                    "materialType": A(
                        "terms",
                        field="i18n.de_DE.ccm:educationallearningresourcetype.keyword",
                    )
                },
                {"noderef_id": A("terms", field="collections.nodeRef.id.keyword")},
            ],
            size=size,
        ),
    )

    response: Response = s[:0].execute()

    if response.success():

        def group_results(carry, stat):
            carry[glom(stat, "key.noderef_id")].append(
                {glom(stat, "key.materialType"): glom(stat, "doc_count")}
            )

        return merge(
            response.aggregations.stats.buckets,
            op=group_results,
            init=lambda: defaultdict(list),
        )
