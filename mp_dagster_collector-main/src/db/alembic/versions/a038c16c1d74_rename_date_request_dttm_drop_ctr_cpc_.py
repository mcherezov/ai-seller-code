"""rename date→request_dttm, drop ctr/cpc/cr in wb_adv_product_stats_1h

Revision ID: a038c16c1d74
Revises: 44e609b6c7a2
Create Date: 2025-08-06 20:40:14.378085

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a038c16c1d74'
down_revision: Union[str, None] = '44e609b6c7a2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



_SCHEMA = "silver"
_TABLE  = "wb_adv_product_stats_1h"


def upgrade() -> None:
    """Применяем изменения схемы."""
    with op.batch_alter_table(_TABLE, schema=_SCHEMA) as batch:
        # 1. Переименовываем колонку date → request_dttm
        batch.alter_column(
            "date",                         # старое имя
            new_column_name="request_dttm", # новое имя
            existing_type=sa.DateTime(timezone=True),
        )

        # 2. Удаляем метрики
        batch.drop_column("ctr")
        batch.drop_column("cpc")
        batch.drop_column("cr")


def downgrade() -> None:
    """Откат миграции (метрики останутся пустыми)."""
    with op.batch_alter_table(_TABLE, schema=_SCHEMA) as batch:
        # 1. Возвращаем метрики
        batch.add_column(sa.Column("ctr", sa.Float(), nullable=False))
        batch.add_column(sa.Column("cpc", sa.Float(), nullable=False))
        batch.add_column(sa.Column("cr",  sa.Float(), nullable=False))

        # 2. Возвращаем прежнее имя колонки
        batch.alter_column(
            "request_dttm",
            new_column_name="date",
            existing_type=sa.DateTime(timezone=True),
        )