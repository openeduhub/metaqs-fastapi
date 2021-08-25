from typing import (
    ClassVar,
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

from app.elastic.fields import (
    Field,
    FieldType,
)
from .base import ResponseModel
from .elastic import ElasticResource
from .util import EmptyStrToNone

_COLLECTION = TypeVar("_COLLECTION")


class CollectionAttribute(Field):
    TITLE = ("properties.cm:title", FieldType.TEXT)
    KEYWORDS = ("properties.cclom:general_keyword", FieldType.TEXT)
    DESCRIPTION = ("properties.cm:description", FieldType.TEXT)
    PATH = ("path", FieldType.TEXT)
    PARENT_ID = ("parentRef.id", FieldType.KEYWORD)


class CollectionBase(ElasticResource):
    title: Optional[EmptyStrToNone] = None
    keywords: Optional[List[str]] = None
    description: Optional[EmptyStrToNone] = None
    path: Optional[List[UUID]] = None
    parent_id: Optional[UUID] = None

    source_fields: ClassVar[list] = ElasticResource.source_fields
    source_fields.extend(
        [
            CollectionAttribute.TITLE,
            CollectionAttribute.KEYWORDS,
            CollectionAttribute.DESCRIPTION,
            CollectionAttribute.PATH,
            CollectionAttribute.PARENT_ID,
        ]
    )

    @classmethod
    def parse_elastic_hit_to_dict(cls: Type[_COLLECTION], hit: Dict,) -> dict:
        return {
            **super(CollectionBase, cls).parse_elastic_hit_to_dict(hit),
            "title": glom(hit, Coalesce(CollectionAttribute.TITLE.path, default=None)),
            "keywords": glom(
                hit,
                (Coalesce(CollectionAttribute.KEYWORDS.path, default=[]), Iter().all()),
            ),
            "description": glom(
                hit, Coalesce(CollectionAttribute.DESCRIPTION.path, default=None)
            ),
            "path": glom(
                hit,
                (Coalesce(CollectionAttribute.PATH.path, default=[]), Iter().all(),),
            ),
            "parent_id": glom(
                hit, Coalesce(CollectionAttribute.PARENT_ID.path, default=None)
            ),
        }

    @classmethod
    def parse_elastic_hit(cls: Type[_COLLECTION], hit: Dict,) -> _COLLECTION:
        collection = cls.construct(**cls.parse_elastic_hit_to_dict(hit))
        collection.parent_id = collection.path[-1]
        return collection


class Collection(ResponseModel, CollectionBase):
    pass
