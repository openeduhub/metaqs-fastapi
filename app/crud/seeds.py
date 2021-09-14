from datetime import timedelta
from uuid import UUID

from app.pg.pg_utils import get_postgres
from app.pg.queries import (
    stats_earliest,
    stats_insert,
)


async def seed_stats(noderef_id: UUID, size: int = 10):
    postgres = await get_postgres()
    async with postgres.pool.acquire() as conn:
        row = await stats_earliest(conn, noderef_id=noderef_id)

        if not row:
            return

        # TODO: refactor algorithm
        for days in range(1, size + 1):
            row = await stats_insert(
                conn,
                noderef_id=noderef_id,
                stats=row["stats"],
                derived_at=row["derived_at"] - timedelta(days=days),
            )
