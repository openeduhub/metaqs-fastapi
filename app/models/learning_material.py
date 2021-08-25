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
from .elastic import ElasticResource
from .util import EmptyStrToNone

_LEARNING_MATERIAL = TypeVar("_LEARNING_MATERIAL")


class LearningMaterialAttribute(Field):
    TITLE = ("properties.cclom:title", FieldType.TEXT)
    KEYWORDS = ("properties.cclom:general_keyword", FieldType.TEXT)
    EDUCONTEXT = ("properties.ccm:educationalcontext", FieldType.TEXT)
    SUBJECTS = ("properties.ccm:taxonid", FieldType.TEXT)
    CONTENT_URL = ("properties.ccm:wwwurl", FieldType.TEXT)
    DESCRIPTION = ("properties.cclom:general_description", FieldType.TEXT)
    LICENSES = ("properties.ccm:commonlicense_key", FieldType.TEXT)
    COLLECTION_NODEREF_ID = ("collections.nodeRef.id", FieldType.TEXT)
    COLLECTION_PATH = ("collections.path", FieldType.TEXT)


class LearningMaterialBase(ElasticResource):
    title: Optional[EmptyStrToNone] = None
    keywords: Optional[List[str]] = None
    educontext: Optional[List[str]] = None
    subjects: Optional[List[str]] = None
    content_url: Optional[str] = None
    description: Optional[EmptyStrToNone] = None
    licenses: Optional[EmptyStrToNone] = None

    source_fields: ClassVar[list] = ElasticResource.source_fields
    source_fields.extend(
        [
            LearningMaterialAttribute.TITLE,
            LearningMaterialAttribute.KEYWORDS,
            LearningMaterialAttribute.EDUCONTEXT,
            LearningMaterialAttribute.SUBJECTS,
            LearningMaterialAttribute.CONTENT_URL,
            LearningMaterialAttribute.DESCRIPTION,
            LearningMaterialAttribute.LICENSES,
        ]
    )

    @classmethod
    def parse_elastic_hit_to_dict(cls: Type[_LEARNING_MATERIAL], hit: Dict,) -> dict:
        return {
            **super(LearningMaterialBase, cls).parse_elastic_hit_to_dict(hit),
            "title": glom(
                hit, Coalesce(LearningMaterialAttribute.TITLE.path, default=None)
            ),
            "keywords": glom(
                hit,
                (
                    Coalesce(LearningMaterialAttribute.KEYWORDS.path, default=[]),
                    Iter().all(),
                ),
            ),
            "educontext": glom(
                hit,
                (
                    Coalesce(LearningMaterialAttribute.EDUCONTEXT.path, default=[]),
                    Iter().all(),
                ),
            ),
            "subjects": glom(
                hit,
                (
                    Coalesce(LearningMaterialAttribute.SUBJECTS.path, default=[]),
                    Iter().all(),
                ),
            ),
            "content_url": glom(
                hit, Coalesce(LearningMaterialAttribute.CONTENT_URL.path, default=None)
            ),
            "description": glom(
                hit,
                (
                    Coalesce(LearningMaterialAttribute.DESCRIPTION.path, default=[]),
                    (Iter().all(), "\n".join),
                ),
            ),
            "licenses": glom(
                hit,
                (
                    Coalesce(LearningMaterialAttribute.LICENSES.path, default=[]),
                    (Iter().all(), "\n".join),
                ),
            ),
        }


class LearningMaterial(ResponseModel, LearningMaterialBase):
    pass
