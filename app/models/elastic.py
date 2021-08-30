from abc import ABC
from typing import (
    ClassVar,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
)
from uuid import UUID

from elasticsearch_dsl.response import Response
from pydantic import (
    BaseModel as PydanticBaseModel,
    Extra,
)
from glom import (
    glom,
    Coalesce,
)

from app.elastic.fields import (
    Field,
    FieldType,
)
from .base import BaseModel
from .util import EmptyStrToNone

_ELASTIC_RESOURCE = TypeVar("_ELASTIC_RESOURCE")
_ELASTIC_AGG = TypeVar("_ELASTIC_AGG")
_BUCKET_AGG = TypeVar("_BUCKET_AGG")
_DESCENDANT_COLLECTIONS_MATERIALS_COUNTS = TypeVar(
    "_DESCENDANT_COLLECTIONS_MATERIALS_COUNTS"
)


class ElasticResourceAttribute(Field):
    NODEREF_ID = ("nodeRef.id", FieldType.KEYWORD)
    TYPE = ("type", FieldType.KEYWORD)
    NAME = ("properties.cm:name", FieldType.TEXT)
    PERMISSION_READ = ("permissions.read", FieldType.TEXT)
    EDU_METADATASET = ("properties.cm:edu_metadataset", FieldType.TEXT)
    PROTOCOL = ("nodeRef.storeRef.protocol", FieldType.TEXT)
    FULLPATH = ("fullpath", FieldType.TEXT)


class ElasticConfig:
    allow_population_by_field_name = True
    extra = Extra.allow


class ElasticResource(BaseModel):
    noderef_id: UUID
    type: Optional[EmptyStrToNone] = None
    name: Optional[EmptyStrToNone] = None

    source_fields: ClassVar[set] = {
        ElasticResourceAttribute.NODEREF_ID,
        ElasticResourceAttribute.TYPE,
        ElasticResourceAttribute.NAME,
    }

    class Config(ElasticConfig):
        pass

    @classmethod
    def parse_elastic_hit_to_dict(cls: Type[_ELASTIC_RESOURCE], hit: Dict,) -> dict:
        spec = {
            "noderef_id": ElasticResourceAttribute.NODEREF_ID.path,
            "type": Coalesce(ElasticResourceAttribute.TYPE.path, default=None),
            "name": Coalesce(ElasticResourceAttribute.NAME.path, default=None),
        }
        return glom(hit, spec)

    @classmethod
    def parse_elastic_hit(
        cls: Type[_ELASTIC_RESOURCE], hit: Dict,
    ) -> _ELASTIC_RESOURCE:
        return cls.construct(**cls.parse_elastic_hit_to_dict(hit))


class ElasticAggConfig:
    arbitrary_types_allowed = True
    allow_population_by_field_name = True
    extra = Extra.forbid


class ElasticAgg(BaseModel, ABC):
    class Config(ElasticAggConfig):
        pass

    @classmethod
    def parse_elastic_response(
        cls: Type[_ELASTIC_AGG], response: Response,
    ) -> _ELASTIC_AGG:
        raise NotImplementedError()


class BucketAgg(ElasticAgg, ABC):
    pass


class CollectionMaterialsCount(PydanticBaseModel):
    noderef_id: UUID
    materials_count: int


class DescendantCollectionsMaterialsCounts(BucketAgg):

    results: List[CollectionMaterialsCount]

    @classmethod
    def parse_elastic_response(
        cls: Type[_DESCENDANT_COLLECTIONS_MATERIALS_COUNTS], response: Response,
    ) -> _DESCENDANT_COLLECTIONS_MATERIALS_COUNTS:
        results = glom(
            response,
            (
                "aggregations.grouped_by_collection.buckets",
                [{"noderef_id": "key.noderef_id", "materials_count": "doc_count",}],
            ),
        )
        return cls.construct(
            results=[
                CollectionMaterialsCount.construct(**record) for record in results
            ],
        )
