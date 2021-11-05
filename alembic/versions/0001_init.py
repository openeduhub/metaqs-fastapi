"""init

Revision ID: 0001
Revises: 
Create Date: 1970-01-01 00:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    conn.execute(
        """
create schema if not exists analytics_raw;
"""
    )


def downgrade():
    conn = op.get_bind()
    conn.execute(
        """
drop schema if exists analytics_raw cascade;
"""
    )
