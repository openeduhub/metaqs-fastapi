from elasticsearch_dsl import connections

from app.core.config import (
    ELASTICSEARCH_URL,
    ELASTICSEARCH_TIMEOUT,
)


async def connect_to_elastic():
    connections.create_connection(
        hosts=[ELASTICSEARCH_URL], timeout=ELASTICSEARCH_TIMEOUT
    )


async def close_elastic_connection():
    pass
