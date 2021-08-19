from typing import (
    List,
    Optional,
)

from elasticsearch_dsl.response import Response
from pydantic import BaseModel

from app.core.config import PORTAL_ROOT_ID
from app.elastic import (
    Search,
    qbool,
    qterm,
    acomposite,
    abucketsort,
    Q,
    A,
)
from app.models.elastic import (
    Attribute as BaseAttribute,
    DescendantCollectionsMaterialsCounts,
)
from app.models.collection import (
    Attribute as CollectionAttribute,
    Collection,
)
from app.models.learning_material import LearningMaterial
from .elastic import (
    base_filter,
    get_many_base_query,
    ResourceType,
    type_filter,
)
from .learning_material import (
    get_many as get_many_materials,
    MissingAttributeFilter as MissingMaterialAttributeFilter,
)


class MissingAttributeFilter(BaseModel):
    attr: CollectionAttribute

    def __call__(self, query_dict: dict):
        query_dict["must_not"] = Q("wildcard", **{self.attr.value: "*"})
        return query_dict


async def get_noderef_ids() -> List[str]:
    return [
        "bd8be6d5-0fbe-4534-a4b3-773154ba6abc",  # Mathematik
        "94f22c9b-0d3a-4c1c-8987-4c8e83f3a92e",  # Physik
    ]


async def get_single(noderef_id: str) -> Collection:
    return Collection(noderef_id=noderef_id)


async def get_many(
    ancestor_id: Optional[str] = None,
    missing_attr_filter: Optional[MissingAttributeFilter] = None,
    max_hits: Optional[int] = 5000,
    source_fields: Optional[List[str]] = None,
) -> List[Collection]:
    query_dict = get_many_base_query(
        resource_type=ResourceType.COLLECTION, ancestor_id=ancestor_id,
    )
    if missing_attr_filter:
        query_dict = missing_attr_filter(query_dict=query_dict)
    s = Search()
    s.query = qbool(**query_dict)
    response = s.source(source_fields if source_fields else Collection.source_fields())[
        :max_hits
    ].execute()
    if response.success():
        return [Collection.parse_elastic_hit(hit) for hit in response]


async def get_child_materials_with_missing_attributes(
    noderef_id: str,
    missing_attr_filter: MissingMaterialAttributeFilter,
    max_hits: Optional[int] = 5000,
) -> List[LearningMaterial]:
    return await get_many_materials(
        ancestor_id=noderef_id,
        missing_attr_filter=missing_attr_filter,
        max_hits=max_hits,
    )


async def get_child_collections_with_missing_attributes(
    noderef_id: str,
    missing_attr_filter: MissingAttributeFilter,
    max_hits: Optional[int] = 5000,
) -> List[Collection]:
    return await get_many(
        ancestor_id=noderef_id,
        missing_attr_filter=missing_attr_filter,
        max_hits=max_hits,
    )


async def get_descendant_collections_materials_counts(
    ancestor_id: str, size: int = 5000,
) -> DescendantCollectionsMaterialsCounts:

    s = Search().query(
        qbool(
            filter=[
                *type_filter[ResourceType.MATERIAL],
                *base_filter,
                qterm(**{"collections.path.keyword": ancestor_id}),
            ]
        )
    )
    s.aggs.bucket(
        "grouped_by_collection",
        acomposite(
            sources=[
                {"noderef_id": A("terms", field="collections.nodeRef.id.keyword")}
            ],
            size=size,
        ),
    ).pipeline(
        "sorted_by_count", abucketsort(sort=[{"_count": {"order": "asc"}}]),
    )

    response: Response = s[:0].execute()
    if response.success():
        return DescendantCollectionsMaterialsCounts.parse_elastic_response(response)


async def get_portals(root_noderef_id: str = PORTAL_ROOT_ID, size: int = 5000,) -> list:

    s = Search().query(
        qbool(
            filter=[
                *type_filter[ResourceType.COLLECTION],
                *base_filter,
                qterm(**{"path.keyword": root_noderef_id}),
            ]
        )
    )

    response: Response = s.source(
        [
            BaseAttribute.NODEREF_ID,
            BaseAttribute.PATH,
            CollectionAttribute.TITLE,
            CollectionAttribute.PARENT_ID,
        ]
    ).sort("fullpath.keyword")[:size].execute()

    if response.success():
        lut = {PORTAL_ROOT_ID: []}

        for hit in response:
            portal = Collection.parse_elastic_hit(hit)

            try:
                portal_node = {
                    "noderef_id": portal.noderef_id,
                    "title": portal.title,
                    "children": [],
                }
                lut[portal.parent_id].append(portal_node)
                lut[portal.noderef_id] = portal_node["children"]
            except KeyError:
                pass

        return lut[PORTAL_ROOT_ID]
