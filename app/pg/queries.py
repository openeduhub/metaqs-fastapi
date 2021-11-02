from uuid import UUID

from asyncpg import (
    Connection,
    Record,
)

from app.models.stats import StatType
from .metadata import Stats
from .pg_utils import compile_query


async def stats_latest(
    conn: Connection, stat_type: StatType, noderef_id: UUID
) -> Record:
    query = (
        Stats.select()
        .where(Stats.c.noderef_id == noderef_id)
        .where(Stats.c.stat_type == stat_type.value)
    )

    query = query.order_by(Stats.c.derived_at.desc()).limit(1)

    compiled_query, params, _ = compile_query(query)
    return await conn.fetchrow(compiled_query, *params)
