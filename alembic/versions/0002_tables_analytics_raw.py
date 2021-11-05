"""tables_analytics_raw

Revision ID: 0002
Revises: 0001
Create Date: 1970-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "collections",
        sa.Column("id", postgresql.UUID(), nullable=False),
        sa.Column("doc", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("derived_at", postgresql.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema="analytics_raw",
    )
    op.create_table(
        "materials",
        sa.Column("id", postgresql.UUID(), nullable=False),
        sa.Column("doc", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("derived_at", postgresql.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema="analytics_raw",
    )
    op.create_table(
        "collection_material",
        sa.Column("collection_id", postgresql.UUID(), nullable=False),
        sa.Column("material_id", postgresql.UUID(), nullable=False),
        sa.PrimaryKeyConstraint("collection_id", "material_id"),
        sa.ForeignKeyConstraint(("collection_id",), ["analytics_raw.collections.id"],),
        sa.ForeignKeyConstraint(("material_id",), ["analytics_raw.materials.id"],),
        schema="analytics_raw",
    )


def downgrade():
    op.drop_table("collection_material", schema="analytics_raw")
    op.drop_table("materials", schema="analytics_raw")
    op.drop_table("collections", schema="analytics_raw")
