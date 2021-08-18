import asyncpg

from app.core.config import (
    DATABASE_URL,
    MAX_CONNECTIONS_COUNT,
    MIN_CONNECTIONS_COUNT,
)
from .database import db


async def get_database():
    if not db.pool:
        await connect_to_postgres()
    return db


async def connect_to_postgres():
    db.pool = await asyncpg.create_pool(
        str(DATABASE_URL),
        min_size=MIN_CONNECTIONS_COUNT,
        max_size=MAX_CONNECTIONS_COUNT,
    )


async def close_postgres_connection():
    if db.pool:
        await db.pool.close()
