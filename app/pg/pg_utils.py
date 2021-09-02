import asyncpg

from app.core.config import (
    DATABASE_URL,
    MAX_CONNECTIONS_COUNT,
    MIN_CONNECTIONS_COUNT,
)
from .postgres import postgres


async def get_postgres():
    if not postgres.pool:
        await connect_to_postgres()
    return postgres


async def connect_to_postgres():
    postgres.pool = await asyncpg.create_pool(
        str(DATABASE_URL),
        min_size=MIN_CONNECTIONS_COUNT,
        max_size=MAX_CONNECTIONS_COUNT,
    )


async def close_postgres_connection():
    if postgres.pool:
        await postgres.pool.close()
