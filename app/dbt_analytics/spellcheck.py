from datetime import datetime

import sqlalchemy as sa
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
    Spellcheck,
    spellcheck_queue,
)

engine = create_engine(str(DATABASE_URL), future=True)


def run():
    logger.info(f"Starting spellcheck run at: {datetime.now()}")

    with Session(engine) as session:

        for i, row in enumerate(session.execute(sa.select(spellcheck_queue))):
            response = _spellcheck(row.text_content)
            if "matches" in response and response["matches"]:
                session.add(Spellcheck(**row._asdict(), error=response,))

            if i > 0 and i % 100 == 0:
                session.commit()
                logger.info(f"Running spellcheck: {i} spellchecks completed")

        session.commit()

    logger.info(f"Finished spellcheck run at: {datetime.now()}")


def _spellcheck(text, lang="de-DE"):
    response = languagetool.check(
        text,
        api_url=LANGUAGETOOL_URL,
        lang=lang,
        enabled_categories=",".join(LANGUAGETOOL_ENABLED_CATEGORIES),
        enabled_only=True,
    )

    return response
