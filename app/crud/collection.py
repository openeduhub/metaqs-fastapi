from typing import (
    List,
    Optional,
)

from pydantic import BaseModel

from app.elastic import (
    Search,
    qbool,
    Q,
)
from app.models.collection import (
    Attribute as CollectionAttribute,
    Collection,
)
from app.models.learning_material import LearningMaterial
from .elastic import (
    get_many_base_query,
    ResourceType,
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
) -> List[Collection]:
    query_dict = get_many_base_query(
        resource_type=ResourceType.COLLECTION, ancestor_id=ancestor_id,
    )
    if missing_attr_filter:
        query_dict = missing_attr_filter(query_dict=query_dict)
    s = Search()
    s.query = qbool(**query_dict)
    response = s.source(Collection.source_fields())[:max_hits].execute()
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
