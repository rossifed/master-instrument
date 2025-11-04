"""empty message

Revision ID: ae5e9071ccb8
Revises: 65d70954b588
Create Date: 2025-11-04 19:30:06.461421

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ae5e9071ccb8'
down_revision: Union[str, Sequence[str], None] = '65d70954b588'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('venue',
    sa.Column('venue_id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('mnemonic', sa.String(length=50), nullable=True),
    sa.Column('venue_type_id', sa.Integer(), nullable=False),
    sa.Column('country_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['country_id'], ['ref_data.country.country_id'], name=op.f('fk_venue__country_id__country')),
    sa.ForeignKeyConstraint(['venue_type_id'], ['ref_data.venue_type.venue_type_id'], name=op.f('fk_venue__venue_type_id__venue_type')),
    sa.PrimaryKeyConstraint('venue_id', name=op.f('pk_venue')),
    schema='ref_data'
    )
   


def downgrade() -> None:
    """Downgrade schema."""
   
    op.drop_table('venue', schema='ref_data')
   
