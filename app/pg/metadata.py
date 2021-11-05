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

metadata = MetaData(schema="analytics_raw")


Collections = Table(
    "collections",
    metadata,
    Column("id", UUID, primary_key=True),
    Column("doc", JSONB, nullable=False),
    Column("derived_at", TIMESTAMP, nullable=False),
)


Materials = Table(
    "materials",
    metadata,
    Column("id", UUID, primary_key=True),
    Column("doc", JSONB, nullable=False),
    Column("derived_at", TIMESTAMP, nullable=False),
)


CollectionMaterial = Table(
    "collection_material",
    metadata,
    Column(
        "collection_id",
        UUID,
        ForeignKey("collections.id"),
        primary_key=True,
        nullable=False,
    ),
    Column(
        "material_id",
        UUID,
        ForeignKey("materials.id"),
        primary_key=True,
        nullable=False,
    ),
)
