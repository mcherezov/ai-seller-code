"""rename date→request_dttm, drop ctr/cpc/cr

Revision ID: 44e609b6c7a2
Revises: b65847010268
Create Date: 2025-08-06 14:30:04.135135

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '44e609b6c7a2'
down_revision: Union[str, None] = 'b65847010268'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_SCHEMA = "silver"
_TABLE  = "wb_adv_product_stats_1d"


# ————————————————————————————————————————————————————————————————
# upgrade: применяем изменения
# ————————————————————————————————————————————————————————————————
def upgrade() -> None:
    with op.batch_alter_table(_TABLE, schema=_SCHEMA) as batch:
        # 1. переименование PK-колонки
        batch.alter_column(
            "date",
            new_column_name="request_dttm",
            existing_type=sa.DateTime(timezone=True),
        )

        # 2. удаляем метрики
        batch.drop_column("ctr")
        batch.drop_column("cpc")
        batch.drop_column("cr")


# ————————————————————————————————————————————————————————————————
# downgrade: откатываем изменения (данные метрик не восстановятся)
# ————————————————————————————————————————————————————————————————
def downgrade() -> None:
    with op.batch_alter_table(_TABLE, schema=_SCHEMA) as batch:
        # 1. возвращаем метрики
        batch.add_column(sa.Column("ctr", sa.Float(), nullable=False))
        batch.add_column(sa.Column("cpc", sa.Float(), nullable=False))
        batch.add_column(sa.Column("cr",  sa.Float(), nullable=False))

        # 2. возвращаем старое имя
        batch.alter_column(
            "request_dttm",
            new_column_name="date",
            existing_type=sa.DateTime(timezone=True),
        )
