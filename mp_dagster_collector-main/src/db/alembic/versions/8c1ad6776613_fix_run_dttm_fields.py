"""Fix run_dttm fields

Revision ID: 8c1ad6776613
Revises: 2dceb7d75b02
Create Date: 2025-07-09 10:29:17.917223

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8c1ad6776613'
down_revision: Union[str, None] = '2dceb7d75b02'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade():
    # Rename column in wb_adv_product_rates_1d
    op.alter_column(
        table_name='wb_adv_product_rates_1d',
        column_name='run_ddtm',
        new_column_name='run_dttm',
        schema='silver',
        existing_type=None,  # let Alembic detect
        existing_nullable=False,
    )

    # Rename column in wb_adv_campaigns_1d
    op.alter_column(
        table_name='wb_adv_campaigns_1d',
        column_name='run_ddtm',
        new_column_name='run_dttm',
        schema='silver',
        existing_type=None,
        existing_nullable=False,
    )


def downgrade():
    # Revert column name in wb_adv_product_rates_1d
    op.alter_column(
        table_name='wb_adv_product_rates_1d',
        column_name='run_dttm',
        new_column_name='run_ddtm',
        schema='silver',
        existing_type=None,
        existing_nullable=False,
    )

    # Revert column name in wb_adv_campaigns_1d
    op.alter_column(
        table_name='wb_adv_campaigns_1d',
        column_name='run_dttm',
        new_column_name='run_ddtm',
        schema='silver',
        existing_type=None,
        existing_nullable=False,
    )