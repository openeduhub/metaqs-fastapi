"""table_stats

Revision ID: 0002
Revises: 
Create Date: 1970-01-01 00:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    conn.execute("""
create type stat_type as enum
(
    'search',
    'material-types',
    'validation-collections',
    'validation-materials'
);

create table stats
(
    id         serial primary key,
    noderef_id uuid      not null,
    stat_type  stat_type not null,
    stats      jsonb     not null,
    derived_at timestamp not null,
    created_at timestamp not null default now()
);

create index idx_stats_noderef_id_derived_at
    on stats (noderef_id, derived_at);
""")


def downgrade():
    conn = op.get_bind()
    conn.execute("""
drop table stats;
drop type stat_type;
""")
