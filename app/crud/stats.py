from collections import defaultdict

from elasticsearch_dsl.response import Response
from glom import glom, merge

from app.core.config import ELASTIC_MAX_SIZE
from app.elastic import (
    Search,
    qbool,
    acomposite,
    A, aterms,
)
from .elastic import (
    base_filter,
    ResourceType,
    type_filter,
)


async def get_stats(size: int = ELASTIC_MAX_SIZE) -> dict:

    s = Search().query(
        qbool(filter=[*type_filter[ResourceType.MATERIAL], *base_filter,])
    )

    agg_material_types = aterms(
        field="i18n.de_DE.ccm:educationallearningresourcetype.keyword"
    )
    s.aggs.bucket("material_types", agg_material_types)
    s.aggs.bucket(
        "stats",
        acomposite(
            sources=[
                {"material_type": agg_material_types},
                {"noderef_id": aterms(field="collections.nodeRef.id.keyword")},
            ],
            size=size,
        ),
    )

    response: Response = s[:0].execute()

    if response.success():

        def group_results(carry, stat):
            carry[glom(stat, "key.noderef_id")].append(
                {glom(stat, "key.material_type"): glom(stat, "doc_count")}
            )

        return merge(
            response.aggregations.stats.buckets,
            op=group_results,
            init=lambda: defaultdict(list),
        )
