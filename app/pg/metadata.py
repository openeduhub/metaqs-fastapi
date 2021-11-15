import sqlalchemy as sa
from sqlalchemy.orm import declarative_base

from app.core.config import DATABASE_URL

engine = sa.create_engine(str(DATABASE_URL), future=True)
Base = declarative_base()


class Collection(Base):
    __table__ = sa.Table(
        "collections", Base.metadata, schema="raw", autoload_with=engine,
    )


class Material(Base):
    __table__ = sa.Table(
        "materials", Base.metadata, schema="raw", autoload_with=engine,
    )


spellcheck_queue = sa.Table(
    "spellcheck_queue",
    Base.metadata,
    sa.PrimaryKeyConstraint("resource_id", "resource_field"),
    schema="staging",
    autoload_with=engine,
)


spellcheck = sa.Table(
    "spellcheck",
    Base.metadata,
    sa.PrimaryKeyConstraint("resource_id", "resource_field"),
    schema="store",
    autoload_with=engine,
)
