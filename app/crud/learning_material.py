from typing import (
    List,
    Optional,
)

from pydantic import BaseModel

from app.crud.elastic import (
    get_many_base_query,
    ResourceType,
)
from app.elastic import (
    Search,
    qterms,
    qbool,
    qnotexists,
    qboolor,
    Q,
)
from app.models.learning_material import (
    Attribute as MaterialAttribute,
    LearningMaterial,
)


class MissingAttributeFilter(BaseModel):
    attr: MaterialAttribute

    def __call__(self, query_dict: dict):
        if self.attr == MaterialAttribute.LICENSES:
            keyword = f"{self.attr.value}.keyword"
            query_dict["filter"].extend(
                [
                    qboolor(
                        [
                            qterms(
                                **{keyword: ["UNTERRICHTS_UND_LEHRMEDIEN", "NONE", ""]}
                            ),
                            qnotexists(field=self.attr.value),
                        ]
                    )
                ]
            )
        else:
            query_dict["must_not"] = Q("wildcard", **{self.attr.value: "*"})
        return query_dict


async def get_many(
    ancestor_id: Optional[str] = None,
    missing_attr_filter: Optional[MissingAttributeFilter] = None,
    max_hits: Optional[int] = 5000,
) -> List[LearningMaterial]:
    query_dict = get_many_base_query(
        resource_type=ResourceType.MATERIAL, ancestor_id=ancestor_id,
    )
    if missing_attr_filter:
        query_dict = missing_attr_filter(query_dict=query_dict)
    s = Search()
    s.query = qbool(**query_dict)
    response = s.source(LearningMaterial.source_fields())[:max_hits].execute()
    if response.success():
        return [LearningMaterial.parse_elastic_hit(hit) for hit in response]
