from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    Table,
)
from sqlalchemy.dialects.postgresql import (
    ENUM,
    JSONB,
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
    Column("stat_type", ENUM),
    Column("stats", JSONB),
    Column("derived_at", TIMESTAMP),
    Column("created_at", TIMESTAMP),
)
