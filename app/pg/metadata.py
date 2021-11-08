from sqlalchemy import (
    Column,
    ForeignKey,
    MetaData,
    Table,
)
from sqlalchemy.dialects.postgresql import (
    JSONB,
    TIMESTAMP,
    UUID,
)
from sqlalchemy.orm import declarative_base

metadata = MetaData(schema="analytics_raw")
Base = declarative_base(metadata=metadata)


collections = Table(
    "collections",
    Base.metadata,
    Column("id", UUID, primary_key=True),
    Column("doc", JSONB, nullable=False),
    Column("derived_at", TIMESTAMP, nullable=False),
)


materials = Table(
    "materials",
    Base.metadata,
    Column("id", UUID, primary_key=True),
    Column("doc", JSONB, nullable=False),
    Column("derived_at", TIMESTAMP, nullable=False),
)


class Collection(Base):
    __table__ = collections


class Material(Base):
    __table__ = materials


# collection_material = Table(
#     "collection_material",
#     metadata,
#     Column(
#         "collection_id",
#         UUID,
#         ForeignKey("collections.id"),
#         primary_key=True,
#         nullable=False,
#     ),
#     Column(
#         "material_id",
#         UUID,
#         ForeignKey("materials.id"),
#         primary_key=True,
#         nullable=False,
#     ),
# )
