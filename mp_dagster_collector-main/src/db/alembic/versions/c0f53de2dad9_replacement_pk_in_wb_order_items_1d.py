"""replacement PK in wb_order_items_1d

Revision ID: c0f53de2dad9
Revises: 97482da0ceba
Create Date: 2025-07-03 15:04:10.168009

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c0f53de2dad9'
down_revision: Union[str, None] = '97482da0ceba'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) Снятие старого первичного ключа на request_uuid
    op.drop_constraint(
        constraint_name="wb_order_items_1d_pkey",
        table_name="wb_order_items_1d",
        schema="silver",
        type_="primary",
    )
    # 2) Разрешаем nullable для request_uuid (теперь не PK)
    op.alter_column(
        table_name="wb_order_items_1d",
        schema="silver",
        column_name="request_uuid",
        nullable=True,
    )
    # 3) Создаём новый первичный ключ только на sr_id
    op.create_primary_key(
        constraint_name="wb_order_items_1d_pkey",
        table_name="wb_order_items_1d",
        schema="silver",
        columns=["sr_id"],
    )


def downgrade() -> None:
    # 1) Снимаем PK на sr_id
    op.drop_constraint(
        constraint_name="wb_order_items_1d_pkey",
        table_name="wb_order_items_1d",
        schema="silver",
        type_="primary",
    )
    # 2) Восстанавливаем nullable=False для request_uuid
    op.alter_column(
        table_name="wb_order_items_1d",
        schema="silver",
        column_name="request_uuid",
        nullable=False,
    )
    # 3) Возвращаем старый PK на request_uuid
    op.create_primary_key(
        constraint_name="wb_order_items_1d_pkey",
        table_name="wb_order_items_1d",
        schema="silver",
        columns=["request_uuid"],
    )