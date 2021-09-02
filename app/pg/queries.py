from datetime import datetime
from uuid import UUID

from asyncpg import Connection
from fastapi.encoders import jsonable_encoder

from app.crud import compile_query
from app.models.stats import StatsResponse
from .metadata import Stats


def stats_latest(noderef_id: UUID, at: datetime = None):
    query = (
        Stats.select()
        .where(Stats.c.noderef_id == noderef_id)
        .order_by(Stats.c.derived_at.desc())
        .limit(1)
    )

    if at:
        query = query.where(Stats.c.derived_at <= at)

    return query


def stats_earliest(noderef_id: UUID):
    return (
        Stats.select()
        .where(Stats.c.noderef_id == noderef_id)
        .order_by(Stats.c.derived_at.asc())
        .limit(1)
    )


async def seed_record(conn: Connection, noderef_id: UUID, stats: StatsResponse):
    query = Stats.insert().values(
        noderef_id=noderef_id,
        stats=jsonable_encoder(stats.stats),
        derived_at=stats.derived_at,
    )
    compiled_query, params, _ = compile_query(query)
    return await conn.execute(compiled_query, *params)
