from typing import (
    List,
    Optional,
    Set,
)
from uuid import UUID

from pydantic import BaseModel

from app.core.config import ELASTIC_MAX_SIZE
from app.crud.elastic import (
    ResourceType,
    get_many_base_query,
    query_missing_material_license,
)
from app.elastic import (
    Field,
    Search,
    qbool,
    qwildcard,
)
from app.models.learning_material import (
    LearningMaterial,
    LearningMaterialAttribute,
)


MissingMaterialField = Field(
    "MissingMaterialField",
    [
        (f.name, (f.value, f.field_type))
        for f in [
            LearningMaterialAttribute.NAME,
            LearningMaterialAttribute.TITLE,
            LearningMaterialAttribute.KEYWORDS,
            LearningMaterialAttribute.EDUCONTEXT,
            LearningMaterialAttribute.SUBJECTS,
            LearningMaterialAttribute.WWW_URL,
            LearningMaterialAttribute.DESCRIPTION,
            LearningMaterialAttribute.LICENSES,
        ]
    ],
)


class MissingAttributeFilter(BaseModel):
    attr: MissingMaterialField

    def __call__(self, query_dict: dict):
        if self.attr == LearningMaterialAttribute.LICENSES:
            query_dict["filter"].append(query_missing_material_license())
        else:
            query_dict["must_not"] = qwildcard(qfield=self.attr, value="*")

        return query_dict


async def get_many(
    ancestor_id: Optional[UUID] = None,
    missing_attr_filter: Optional[MissingAttributeFilter] = None,
    source_fields: Optional[Set[LearningMaterialAttribute]] = None,
    max_hits: Optional[int] = ELASTIC_MAX_SIZE,
) -> List[LearningMaterial]:
    query_dict = get_many_base_query(
        resource_type=ResourceType.MATERIAL, ancestor_id=ancestor_id,
    )
    if missing_attr_filter:
        query_dict = missing_attr_filter.__call__(query_dict=query_dict)
    s = Search().query(qbool(**query_dict))

    response = s.source(
        source_fields if source_fields else LearningMaterial.source_fields
    )[:max_hits].execute()

    if response.success():
        return [LearningMaterial.parse_elastic_hit(hit) for hit in response]
