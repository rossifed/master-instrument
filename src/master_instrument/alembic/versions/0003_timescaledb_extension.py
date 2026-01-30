from alembic import op

revision = "0003_timescaledb_extension"
down_revision = "0002_temporal_extension"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")    


def downgrade():
    op.execute("DROP EXTENSION IF EXISTS timescaledb;")
