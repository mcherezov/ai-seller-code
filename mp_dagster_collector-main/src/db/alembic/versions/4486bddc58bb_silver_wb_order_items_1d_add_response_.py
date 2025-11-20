"""silver.wb_order_items_1d: add response_dttm, business_dttm

Revision ID: 4486bddc58bb
Revises: 2b0d2f2e549e
Create Date: 2025-08-19 16:05:43.659518

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4486bddc58bb'
down_revision: Union[str, None] = '2b0d2f2e549e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SILVER_SCHEMA = "silver"
SILVER_TABLE  = "wb_order_items_1d"


def upgrade() -> None:
    # 1) Добавляем колонки как NULLABLE для безопасного бэкфилла
    op.add_column(
        SILVER_TABLE,
        sa.Column("response_dttm", sa.TIMESTAMP(timezone=True), nullable=True),
        schema=SILVER_SCHEMA,
    )
    op.add_column(
        SILVER_TABLE,
        sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=True),
        schema=SILVER_SCHEMA,
    )

    # 2) Бэкфилл:
    #    response_dttm = inserted_at
    #    business_dttm = (inserted_at в MSK) → минус 1 день → обнулить время → обратно в timestamptz
    op.execute(f"""
        UPDATE {SILVER_SCHEMA}.{SILVER_TABLE}
        SET
            response_dttm = COALESCE(response_dttm, inserted_at),
            business_dttm = COALESCE(
                business_dttm,
                timezone(
                    'Europe/Moscow',
                    date_trunc(
                        'day',
                        (COALESCE(inserted_at, now()) AT TIME ZONE 'Europe/Moscow') - interval '1 day'
                    )
                )
            )
        WHERE response_dttm IS NULL OR business_dttm IS NULL
    """)

    # 3) Делаем NOT NULL
    op.alter_column(
        SILVER_TABLE, "response_dttm",
        schema=SILVER_SCHEMA,
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=False,
    )
    op.alter_column(
        SILVER_TABLE, "business_dttm",
        schema=SILVER_SCHEMA,
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=False,
    )

    # 4) Комментарии (по желанию)
    op.execute(f"COMMENT ON COLUMN {SILVER_SCHEMA}.{SILVER_TABLE}.response_dttm IS 'timestamp ответа API.'")
    op.execute(f"COMMENT ON COLUMN {SILVER_SCHEMA}.{SILVER_TABLE}.business_dttm  IS 'бизнес-время.'")


def downgrade() -> None:
    # Снимаем NOT NULL и удаляем колонки
    op.alter_column(
        SILVER_TABLE, "business_dttm",
        schema=SILVER_SCHEMA,
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=True,
    )
    op.alter_column(
        SILVER_TABLE, "response_dttm",
        schema=SILVER_SCHEMA,
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=True,
    )
    op.drop_column(SILVER_TABLE, "business_dttm", schema=SILVER_SCHEMA)
    op.drop_column(SILVER_TABLE, "response_dttm",  schema=SILVER_SCHEMA)