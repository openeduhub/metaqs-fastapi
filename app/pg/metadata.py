from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    JSONB,
    SMALLINT,
    TIMESTAMP,
    UUID,
)

metadata = MetaData()


async def get_metadata():
    return metadata


Stats = Table(
    "stats",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("noderef_id", UUID),
    Column("stats", JSONB),
    Column("derived_at", TIMESTAMP),
    Column("created_at", TIMESTAMP),
)
