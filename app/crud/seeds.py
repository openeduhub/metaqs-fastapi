from uuid import UUID

from app.pg.pg_utils import get_postgres
from app.pg.queries import (
    stats_clear,
    stats_duplicate_backwards,
)


async def clear_stats():
    postgres = await get_postgres()
    async with postgres.pool.acquire() as conn:
        await stats_clear(conn)


async def seed_stats(noderef_id: UUID, size: int = 10):
    postgres = await get_postgres()
    async with postgres.pool.acquire() as conn:
        for _ in range(size):
            await stats_duplicate_backwards(conn, noderef_id=noderef_id)
