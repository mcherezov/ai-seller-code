"""add logisti_type in wb_sales_1d

Revision ID: eb679aeb0a74
Revises: 6a2a142aa6e5
Create Date: 2025-08-02 00:12:59.207289

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'eb679aeb0a74'
down_revision: Union[str, None] = '6a2a142aa6e5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Удаляем старый PK
    op.drop_constraint(
        'wb_sales_1d_pkey',
        'wb_sales_1d',
        schema='silver',
        type_='primary'
    )

    # Добавляем новое поле
    op.add_column('wb_sales_1d',
        sa.Column('logistic_type', sa.String(), nullable=True),
        schema='silver'
    )

    # Создаем новый составной PK
    op.create_primary_key(
        'wb_sales_1d_pkey',
        'wb_sales_1d',
        [
            'request_uuid', 'sr_id', 'payment_reason',
            'nm_id', 'supplier_article', 'order_date',
            'warehouse_name', 'country', 'logistic_type'
        ],
        schema='silver'
    )


def downgrade():
    # Удаляем новый PK
    op.drop_constraint(
        'wb_sales_1d_pkey',
        'wb_sales_1d',
        schema='silver',
        type_='primary'
    )

    # Удаляем колонку
    op.drop_column('wb_sales_1d', 'logistic_type', schema='silver')

    # Восстанавливаем старый PK
    op.create_primary_key(
        'wb_sales_1d_pkey',
        'wb_sales_1d',
        [
            'request_uuid', 'sr_id', 'payment_reason',
            'nm_id', 'supplier_article', 'order_date',
            'warehouse_name', 'country'
        ],
        schema='silver'
    )