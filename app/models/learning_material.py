from enum import Enum
from typing import (
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
from pydantic import HttpUrl

from .base import ResponseModel
from .elastic import (
    Attribute as BaseAttribute,
    ElasticResource,
)
from .util import EmptyStrToNone


_LEARNING_MATERIAL = TypeVar("_LEARNING_MATERIAL")


class Attribute(str, Enum):
    TITLE = "properties.cclom:title"
    KEYWORDS = "properties.cclom:general_keyword"
    EDUCONTEXT = "properties.ccm:educationalcontext"
    SUBJECTS = "properties.ccm:taxonid"
    WWWURL = "properties.ccm:wwwurl"
    DESCRIPTION = "properties.cclom:general_description"
    LICENSES = "properties.ccm:commonlicense_key"


class LearningMaterialBase(ElasticResource):

    title: Optional[EmptyStrToNone] = None
    keywords: Optional[List[str]] = None
    educontext: Optional[List[str]] = None
    subjects: Optional[List[str]] = None
    www_url: Optional[HttpUrl] = None
    description: Optional[EmptyStrToNone] = None
    licenses: Optional[EmptyStrToNone] = None

    @classmethod
    def source_fields(cls: Type[_LEARNING_MATERIAL]) -> List:
        fields = super().source_fields()
        fields.extend(
            [
                Attribute.TITLE,
                Attribute.KEYWORDS,
                Attribute.EDUCONTEXT,
                Attribute.SUBJECTS,
                Attribute.WWWURL,
                Attribute.DESCRIPTION,
                Attribute.LICENSES,
            ]
        )
        return fields

    @classmethod
    def parse_elastic_hit(
        cls: Type[_LEARNING_MATERIAL], hit: Dict,
    ) -> _LEARNING_MATERIAL:
        return cls.construct(
            noderef_id=glom(hit, BaseAttribute.NODEREF_ID),
            type=glom(hit, BaseAttribute.TYPE),
            path=glom(hit, (Coalesce(BaseAttribute.PATH, default=[]), Iter().all())),
            name=glom(hit, BaseAttribute.NAME),
            title=glom(hit, Coalesce(Attribute.TITLE, default=None)),
            keywords=glom(
                hit, (Coalesce(Attribute.KEYWORDS, default=[]), Iter().all())
            ),
            educontext=glom(
                hit, (Coalesce(Attribute.EDUCONTEXT, default=[]), Iter().all())
            ),
            subjects=glom(
                hit, (Coalesce(Attribute.SUBJECTS, default=[]), Iter().all())
            ),
            www_url=glom(hit, Coalesce(Attribute.WWWURL, default=None)),
            description=glom(
                hit,
                (
                    Coalesce(Attribute.DESCRIPTION, default=[]),
                    (Iter().all(), "\n".join),
                ),
            ),
            licenses=glom(
                hit,
                (Coalesce(Attribute.LICENSES, default=[]), (Iter().all(), "\n".join)),
            ),
        )


class LearningMaterial(ResponseModel, LearningMaterialBase):
    pass
