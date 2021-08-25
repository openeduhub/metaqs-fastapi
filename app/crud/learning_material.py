from typing import (
    List,
    Optional,
)
from uuid import UUID

from pydantic import BaseModel

from app.core.config import ELASTIC_MAX_SIZE
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
    qwildcard,
)
from app.models.learning_material import (
    LearningMaterial,
    LearningMaterialAttribute,
)


class MissingAttributeFilter(BaseModel):
    attr: LearningMaterialAttribute

    def __call__(self, query_dict: dict):
        if self.attr == LearningMaterialAttribute.LICENSES:
            query_dict["filter"].extend(
                [
                    qboolor(
                        [
                            qterms(
                                field=self.attr,
                                values=["UNTERRICHTS_UND_LEHRMEDIEN", "NONE", ""],
                            ),
                            qnotexists(field=self.attr.path),
                        ]
                    )
                ]
            )
        else:
            query_dict["must_not"] = qwildcard(field=self.attr, value="*")
        return query_dict


async def get_many(
    ancestor_id: Optional[UUID] = None,
    missing_attr_filter: Optional[MissingAttributeFilter] = None,
    max_hits: Optional[int] = ELASTIC_MAX_SIZE,
) -> List[LearningMaterial]:
    query_dict = get_many_base_query(
        resource_type=ResourceType.MATERIAL, ancestor_id=ancestor_id,
    )
    if missing_attr_filter:
        query_dict = missing_attr_filter(query_dict=query_dict)
    s = Search()
    s.query = qbool(**query_dict)
    response = s.source(LearningMaterial.source_fields)[:max_hits].execute()
    if response.success():
        return [LearningMaterial.parse_elastic_hit(hit) for hit in response]
