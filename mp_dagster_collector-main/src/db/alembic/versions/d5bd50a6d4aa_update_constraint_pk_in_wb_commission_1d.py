"""Update constraint PK in wb_commission_1d

Revision ID: d5bd50a6d4aa
Revises: e683ae92517a
Create Date: 2025-07-04 11:00:47.214262

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd5bd50a6d4aa'
down_revision: Union[str, None] = 'e683ae92517a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) Убираем «старый» primary key
    op.drop_constraint(
        constraint_name="wb_commission_1d_pkey",
        table_name="wb_commission_1d",
        schema="silver",
        type_="primary",
    )

    # 2) Создаём новый составной primary key
    op.create_primary_key(
        "pk_wb_commission_1d",
        "wb_commission_1d",
        ["request_uuid", "date", "subject_id"],
        schema="silver",
    )

    # 3) Добавляем уникальное ограничение по (date, subject_id) для ON CONFLICT
    op.create_unique_constraint(
        "uq_wb_commission_1d_date_subject",
        "wb_commission_1d",
        ["date", "subject_id"],
        schema="silver",
    )


def downgrade() -> None:
    # 1) Удаляем уникальное ограничение
    op.drop_constraint(
        "uq_wb_commission_1d_date_subject",
        "wb_commission_1d",
        schema="silver",
        type_="unique",
    )

    # 2) Удаляем составной PK
    op.drop_constraint(
        "pk_wb_commission_1d",
        "wb_commission_1d",
        schema="silver",
        type_="primary",
    )

    # 3) Восстанавливаем прежний PK по одному полю request_uuid
    op.create_primary_key(
        "wb_commission_1d_pkey",
        "wb_commission_1d",
        ["request_uuid"],
        schema="silver",
    )
