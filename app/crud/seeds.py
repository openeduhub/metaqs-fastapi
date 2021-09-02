import json
from datetime import timedelta
from uuid import UUID

from app.crud import compile_query
from app.models.stats import StatsResponse
from app.pg.metadata import Stats
from app.pg.pg_utils import get_postgres
from app.pg.queries import stats_earliest


async def seed_stats(noderef_id: UUID, size: int = 10):
    postgres = await get_postgres()
    async with postgres.pool.acquire() as conn:
        compiled_query, params, _ = compile_query(stats_earliest(noderef_id))
        row = await conn.fetchrow(compiled_query, *params)

        if not row:
            return

        stats = StatsResponse(
            derived_at=row["derived_at"], stats=json.loads(row["stats"])
        )

        for days in range(1, size + 1):
            query = Stats.insert().values(
                noderef_id=noderef_id,
                stats=stats.stats,
                derived_at=stats.derived_at - timedelta(days=days),
            )
            compiled_query, params, _ = compile_query(query)
            await conn.execute(compiled_query, *params)
