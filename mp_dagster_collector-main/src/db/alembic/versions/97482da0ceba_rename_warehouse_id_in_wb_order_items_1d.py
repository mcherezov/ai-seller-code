"""Rename warehouse_id in wb_order_items_1d

Revision ID: 97482da0ceba
Revises: 94bca0bd9146
Create Date: 2025-07-03 14:25:18.044291

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '97482da0ceba'
down_revision: Union[str, None] = '94bca0bd9146'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Переименование и изменение типа столбца в одну операцию
    op.alter_column(
        table_name='wb_order_items_1d',
        schema='silver',
        column_name='warehouse_id',
        new_column_name='warehouse_name',
        existing_type=sa.Integer(),
        type_=sa.String(),
        nullable=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Откат: возвращаем имя и тип столбца обратно
    op.alter_column(
        table_name='wb_order_items_1d',
        schema='silver',
        column_name='warehouse_name',
        new_column_name='warehouse_id',
        existing_type=sa.String(),
        type_=sa.Integer(),
        nullable=False,
    )