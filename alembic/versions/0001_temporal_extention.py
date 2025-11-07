from alembic import op

revision = "0001_temporal_extension"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS temporal_tables;")


def downgrade():
    op.execute("DROP EXTENSION IF EXISTS temporal_tables;")
