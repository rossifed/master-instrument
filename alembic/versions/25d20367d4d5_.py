"""empty message

Revision ID: 25d20367d4d5
Revises: e8e9285ce749
Create Date: 2025-11-04 11:27:47.166248

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '25d20367d4d5'
down_revision: Union[str, Sequence[str], None] = 'e8e9285ce749'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
  
    op.create_table('quote_mapping',
    sa.Column('internal_quote_id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('external_quote_id', sa.String(length=100), nullable=False),
    sa.Column('source', sa.String(length=20), nullable=False),
    sa.PrimaryKeyConstraint('internal_quote_id', name=op.f('pk_quote_mapping')),
    sa.UniqueConstraint('source', 'external_quote_id', name=op.f('uq_quote_mapping__source')),
    schema='ref_data'
    )
   
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('quote_mapping', schema='ref_data')
   
    # ### end Alembic commands ###
