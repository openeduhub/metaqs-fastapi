from itertools import chain
from typing import (
    ClassVar,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
)

from glom import (
    glom,
    Coalesce,
    Iter,
)

# from pydantic import HttpUrl

from app.elastic.fields import (
    Field,
    FieldType,
)
from .base import ResponseModel
from .elastic import (
    ElasticResource,
    ElasticResourceAttribute,
)
from .util import EmptyStrToNone

_LEARNING_MATERIAL = TypeVar("_LEARNING_MATERIAL")


class _LearningMaterialAttribute(Field):
    TITLE = ("properties.cclom:title", FieldType.TEXT)
    KEYWORDS = ("properties.cclom:general_keyword", FieldType.TEXT)
    EDUCONTEXT = ("properties.ccm:educationalcontext", FieldType.TEXT)
    SUBJECTS = ("properties.ccm:taxonid", FieldType.TEXT)
    WWW_URL = ("properties.ccm:wwwurl", FieldType.TEXT)
    DESCRIPTION = ("properties.cclom:general_description", FieldType.TEXT)
    LICENSES = ("properties.ccm:commonlicense_key", FieldType.TEXT)
    COLLECTION_NODEREF_ID = ("collections.nodeRef.id", FieldType.TEXT)
    COLLECTION_PATH = ("collections.path", FieldType.TEXT)
    CONTENT_FULLTEXT = ("content.fulltext", FieldType.TEXT)
    LEARNINGRESOURCE_TYPE = (
        "properties.ccm:educationallearningresourcetype",
        FieldType.TEXT,
    )


LearningMaterialAttribute = Field(
    "LearningMaterialAttribute",
    [
        (f.name, (f.value, f.field_type))
        for f in chain(ElasticResourceAttribute, _LearningMaterialAttribute)
    ],
)


class LearningMaterialBase(ElasticResource):
    title: Optional[EmptyStrToNone] = None
    keywords: Optional[List[str]] = None
    educontext: Optional[List[str]] = None
    subjects: Optional[List[str]] = None
    www_url: Optional[str] = None
    description: Optional[EmptyStrToNone] = None
    licenses: Optional[EmptyStrToNone] = None

    source_fields: ClassVar[set] = {
        LearningMaterialAttribute.NODEREF_ID,
        LearningMaterialAttribute.TYPE,
        LearningMaterialAttribute.NAME,
        LearningMaterialAttribute.TITLE,
        LearningMaterialAttribute.KEYWORDS,
        LearningMaterialAttribute.EDUCONTEXT,
        LearningMaterialAttribute.SUBJECTS,
        LearningMaterialAttribute.WWW_URL,
        LearningMaterialAttribute.DESCRIPTION,
        LearningMaterialAttribute.LICENSES,
    }

    @classmethod
    def parse_elastic_hit_to_dict(cls: Type[_LEARNING_MATERIAL], hit: Dict,) -> dict:
        spec = {
            "title": Coalesce(LearningMaterialAttribute.TITLE.path, default=None),
            "keywords": (
                Coalesce(LearningMaterialAttribute.KEYWORDS.path, default=[]),
                Iter().all(),
            ),
            "educontext": (
                Coalesce(LearningMaterialAttribute.EDUCONTEXT.path, default=[]),
                Iter().all(),
            ),
            "subjects": (
                Coalesce(LearningMaterialAttribute.SUBJECTS.path, default=[]),
                Iter().all(),
            ),
            "www_url": Coalesce(LearningMaterialAttribute.WWW_URL.path, default=None),
            "description": (
                Coalesce(LearningMaterialAttribute.DESCRIPTION.path, default=[]),
                (Iter().all(), "\n".join),
            ),
            "licenses": (
                Coalesce(LearningMaterialAttribute.LICENSES.path, default=[]),
                (Iter().all(), "\n".join),
            ),
        }
        return {
            **super(LearningMaterialBase, cls).parse_elastic_hit_to_dict(hit),
            **glom(hit, spec),
        }


class LearningMaterial(ResponseModel, LearningMaterialBase):
    pass
