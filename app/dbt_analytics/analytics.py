from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.dbt_analytics.rpc_client as dbt
from app.core.config import DATABASE_URL
from app.core.logging import logger
from .resource_import import (
    import_collections,
    import_materials,
)

engine = create_engine(str(DATABASE_URL), future=True)


def run():
    derived_at = datetime.now()

    logger.info(f"Starting analytics import at: {derived_at}")

    with Session(engine) as session:
        _backup_previous_run(session)

        import_collections(session=session, derived_at=derived_at)
        logger.info(
            f"Analytics: after collections import: {len(session.new)} resources added to session"
        )

        import_materials(session=session, derived_at=derived_at)
        logger.info(
            f"Analytics: after materials import: {len(session.new)} resources added to session"
        )

        session.commit()

    logger.info(f"Finished analytics import at: {datetime.now()}")

    dbt.run_analytics()


def _backup_previous_run(session: Session):
    logger.info(f"Analytics: copying previous import data to backup tables")

    session.execute(
        """
        drop table if exists raw.collections_previous_run cascade;
        create table raw.collections_previous_run
        as table raw.collections;
        truncate raw.collections;
        """
    )
    logger.info(f"Analytics: copied collections to collections_previous_run")

    session.execute(
        """
        drop table if exists raw.materials_previous_run cascade;
        create table raw.materials_previous_run
        as table raw.materials;
        truncate raw.materials;
        """
    )
    logger.info(f"Analytics: copied materials to materials_previous_run")
