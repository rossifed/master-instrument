from alembic import op
import sqlalchemy as sa

revision = "0002_create_ref_data_schema"
down_revision = "0001_temporal_extension"

def upgrade():
    op.execute(sa.text("CREATE SCHEMA IF NOT EXISTS ref_data"))

def downgrade():
    op.execute(sa.text("DROP SCHEMA IF EXISTS ref_data CASCADE"))