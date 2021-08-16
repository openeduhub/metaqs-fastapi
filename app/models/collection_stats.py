from uuid import UUID

from .base import ResponseModel


class CollectionStat(ResponseModel):
    pass


class CollectionMaterialsCount(CollectionStat):
    noderef_id: UUID
    title: str
    materials_count: int
