from datetime import datetime

from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session

import app.dbt_analytics.rpc_client as dbt
from app.core.config import (
    DATABASE_URL,
    PORTAL_ROOT_ID,
)
from app.core.logging import logger
from app.crud.elastic import (
    query_collections,
    query_materials,
)
from app.elastic import Search
from app.http import get_client as get_http_client
from app.pg.metadata import (
    Collection,
    Material,
    collections,
    materials,
)

engine = create_engine(str(DATABASE_URL), future=True)


async def run():
    derived_at = datetime.now()

    logger.info(f"Starting analytics import at: {derived_at}")

    truncated_collection_ids = None
    truncated_material_ids = None

    with Session(engine) as session:
        truncated_collection_ids = session.execute(
            delete(collections).where(1 == 1).returning(collections.c.id)
        ).all()
        logger.info(
            f"Analytics: collections truncated with {len(truncated_collection_ids)} affected"
        )

        truncated_material_ids = session.execute(
            delete(materials).where(1 == 1).returning(materials.c.id)
        ).all()
        logger.info(
            f"Analytics: materials truncated with {len(truncated_material_ids)} affected"
        )

        _import_collections(session=session, derived_at=derived_at)
        logger.info(
            f"Analytics: after collections import: {len(session.new)} resources added to session"
        )

        _import_materials(session=session, derived_at=derived_at)
        logger.info(
            f"Analytics: after materials import: {len(session.new)} resources added to session"
        )

        session.commit()

    logger.info(f"Finished analytics import at: {datetime.now()}")

    http = await get_http_client()

    await dbt.run_analytics(http)

    logger.info(f"Finished analytics run at: {datetime.now()}")


def _import_collections(session: Session, derived_at: datetime):
    s = (
        Search()
        .query(query_collections(ancestor_id=PORTAL_ROOT_ID))
        .source(includes=["type", "aspects", "properties.*", "nodeRef.*", "path"])
    )

    for hit in s.scan():
        session.add(
            Collection(id=hit.nodeRef["id"], doc=hit.to_dict(), derived_at=derived_at)
        )


def _import_materials(session: Session, derived_at: datetime):
    s = (
        Search()
        .query(query_materials(ancestor_id=PORTAL_ROOT_ID))
        .source(
            includes=[
                "type",
                "aspects",
                "properties.*",
                "nodeRef.*",
                "collections.nodeRef.id",
            ]
        )
    )

    for hit in s.scan():
        session.add(
            Material(id=hit.nodeRef["id"], doc=hit.to_dict(), derived_at=derived_at)
        )
