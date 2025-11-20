"""Add surrogate key to wb_ad_campaigns_1d

Revision ID: db70043980c1
Revises: b47e1e925786
Create Date: 2025-06-19 03:46:08.108723

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'db70043980c1'
down_revision: Union[str, None] = 'b47e1e925786'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('wb_ad_campaigns_1d', sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=True), schema='silver')

    # Удаляем старый первичный ключ (по response_uuid)
    op.drop_constraint('wb_ad_campaigns_1d_pkey', 'wb_ad_campaigns_1d', schema='silver', type_='primary')

    # Делаем новую колонку id not null и назначаем primary key
    op.alter_column('wb_ad_campaigns_1d', 'id', nullable=False, schema='silver')
    op.create_primary_key('pk_wb_ad_campaigns_1d', 'wb_ad_campaigns_1d', ['id'], schema='silver')



def downgrade() -> None:
    op.drop_constraint('pk_wb_ad_campaigns_1d', 'wb_ad_campaigns_1d', schema='silver', type_='primary')

    # Восстанавливаем старый первичный ключ
    op.create_primary_key('wb_ad_campaigns_1d_pkey', 'wb_ad_campaigns_1d', ['response_uuid'], schema='silver')

    op.drop_column('wb_ad_campaigns_1d', 'id', schema='silver')

