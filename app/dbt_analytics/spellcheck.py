from datetime import datetime

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
from pylanguagetool import api as languagetool
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.dbt_analytics.rpc_client as dbt
from app.core.config import (
    DATABASE_URL,
    LANGUAGETOOL_ENABLED_CATEGORIES,
    LANGUAGETOOL_URL,
)
from app.core.logging import logger
from app.pg.metadata import (
    spellcheck,
    spellcheck_queue,
)

engine = create_engine(str(DATABASE_URL), future=True)


def run():
    logger.info(f"Spellcheck: starting processing at: {datetime.now()}")

    with Session(engine) as session:

        rows = list(session.execute(sa.select(spellcheck_queue)))

        for i, row in enumerate(rows):
            response = _spellcheck(row.text_content)

            if "matches" in response and response["matches"]:
                t = spellcheck
                stmt = sapg.insert(t).values(
                    resource_id=row.resource_id,
                    resource_type=row.resource_type,
                    resource_field=row.resource_field,
                    text_content=row.text_content,
                    derived_at=row.derived_at,
                    error=response,
                )
                session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=[t.c.resource_id, t.c.resource_field],
                        set_=dict(
                            text_content=stmt.excluded.text_content,
                            derived_at=stmt.excluded.derived_at,
                            error=stmt.excluded.error,
                        ),
                    )
                )

            session.execute(
                sa.delete(spellcheck_queue)
                .where(spellcheck_queue.c.resource_id == row.resource_id)
                .where(spellcheck_queue.c.resource_field == row.resource_field)
            )

            if i > 0 and i % 100 == 0:
                session.commit()
                logger.info(f"Spellcheck: {i} spellchecks completed")

        session.commit()

    logger.info(f"Spellcheck: processing finished at: {datetime.now()}")

    result = dbt.run_spellcheck()
    logger.info(f"Analytics: spellcheck run started {result}")


def _spellcheck(text, lang="de-DE"):
    response = languagetool.check(
        text,
        api_url=LANGUAGETOOL_URL,
        lang=lang,
        enabled_categories=",".join(LANGUAGETOOL_ENABLED_CATEGORIES),
        enabled_only=True,
    )

    return response
