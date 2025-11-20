"""UPD constraint in silver.wb_paid_storage_1d

Revision ID: a58b3af412c8
Revises: 864096e414b3
Create Date: 2025-07-06 13:50:03.794043

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a58b3af412c8'
down_revision: Union[str, None] = '864096e414b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Удаляем старый PK
    op.drop_constraint('pk_paid_storage_1d', 'wb_paid_storage_1d', schema='silver', type_='primary')

    # Добавляем новый PK с calc_type
    op.create_primary_key(
        'pk_paid_storage_1d',
        'wb_paid_storage_1d',
        ['response_dttm', 'date', 'warehouse_name', 'income_id', 'barcode', 'calc_type'],
        schema='silver'
    )


def downgrade():
    # Откатываем обратно: удаляем PK с calc_type
    op.drop_constraint('pk_paid_storage_1d', 'wb_paid_storage_1d', schema='silver', type_='primary')

    # Восстанавливаем старый PK без calc_type
    op.create_primary_key(
        'pk_paid_storage_1d',
        'wb_paid_storage_1d',
        ['response_dttm', 'date', 'warehouse_name', 'income_id', 'barcode'],
        schema='silver'
    )
