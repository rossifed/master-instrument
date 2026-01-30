from alembic import op

revision = "0002_temporal_extension"
down_revision = "0001_master_schema"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS temporal_tables;")


def downgrade():
    op.execute("DROP EXTENSION IF EXISTS temporal_tables;")
