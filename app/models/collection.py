from enum import Enum
from typing import (
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
)
from uuid import UUID

from glom import (
    glom,
    Coalesce,
    Iter,
)

from .base import ResponseModel
from .elastic import (
    Attribute as BaseAttribute,
    ElasticResource,
)
from .util import EmptyStrToNone

_COLLECTION = TypeVar("_COLLECTION")


class Attribute(str, Enum):
    TITLE = "properties.cm:title"
    KEYWORDS = "properties.cclom:general_keyword"
    DESCRIPTION = "properties.cm:description"
    PARENT_ID = "parentRef.id"


class CollectionBase(ElasticResource):

    title: Optional[EmptyStrToNone] = None
    keywords: Optional[List[str]] = None
    description: Optional[EmptyStrToNone] = None
    parent_id: Optional[UUID] = None

    @classmethod
    def source_fields(cls: Type[_COLLECTION]) -> List:
        fields = super().source_fields()
        fields.extend(
            [Attribute.TITLE, Attribute.KEYWORDS, Attribute.DESCRIPTION,]
        )
        return fields

    @classmethod
    def parse_elastic_hit(cls: Type[_COLLECTION], hit: Dict,) -> _COLLECTION:
        collection = cls.construct(
            noderef_id=glom(hit, BaseAttribute.NODEREF_ID),
            type=glom(hit, Coalesce(BaseAttribute.TYPE, default=None)),
            path=glom(hit, (Coalesce(BaseAttribute.PATH, default=[]), Iter().all())),
            name=glom(hit, Coalesce(BaseAttribute.NAME, default=None)),
            title=glom(hit, Coalesce(Attribute.TITLE, default=None)),
            keywords=glom(
                hit, (Coalesce(Attribute.KEYWORDS, default=[]), Iter().all())
            ),
            description=glom(hit, Coalesce(Attribute.DESCRIPTION, default=None)),
            parent_id=glom(hit, Coalesce(Attribute.PARENT_ID, default=None)),
        )
        collection.parent_id = collection.path[-1]
        return collection


class Collection(ResponseModel, CollectionBase):
    pass
