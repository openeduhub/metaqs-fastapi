from enum import Enum
from typing import (
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
)
from uuid import UUID

from pydantic import Extra
from glom import (
    glom,
    Coalesce,
    Iter,
)

from .base import BaseModel
from .util import EmptyStrToNone

_ELASTIC_RESOURCE = TypeVar("_ELASTIC_RESOURCE")


class Attribute(str, Enum):
    NODEREFID = "nodeRef.id"
    TYPE = "type"
    PATH = "path"
    NAME = "properties.cm:name"


class ElasticConfig:
    allow_population_by_field_name = True
    extra = Extra.allow


class ElasticResource(BaseModel):

    node_ref_id: UUID
    type: Optional[EmptyStrToNone] = None
    path: Optional[List[UUID]] = None
    name: Optional[EmptyStrToNone] = None

    class Config(ElasticConfig):
        pass

    @classmethod
    def source_fields(cls: Type[_ELASTIC_RESOURCE],) -> List:
        return [
            Attribute.NODEREFID,
            Attribute.TYPE,
            Attribute.PATH,
            Attribute.NAME,
        ]

    @classmethod
    def parse_elastic_hit(
        cls: Type[_ELASTIC_RESOURCE], hit: Dict,
    ) -> _ELASTIC_RESOURCE:
        return cls.construct(
            node_ref_id=glom(hit, Attribute.NODEREFID),
            type=glom(hit, Coalesce(Attribute.TYPE, default=None)),
            path=glom(hit, (Coalesce(Attribute.PATH, default=[]), Iter().all())),
            name=glom(hit, Coalesce(Attribute.NAME, default=None)),
        )
