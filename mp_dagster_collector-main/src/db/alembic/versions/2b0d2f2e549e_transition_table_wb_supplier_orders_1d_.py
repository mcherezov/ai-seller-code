"""transition table wb_supplier_orders_1d to new mixin

Revision ID: 2b0d2f2e549e
Revises: 7094da664c82
Create Date: 2025-08-19 15:03:04.253806

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2b0d2f2e549e'
down_revision: Union[str, None] = '7094da664c82'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



SCHEMA = "bronze"
TABLE  = "wb_supplier_orders_1d"


def upgrade() -> None:
    # 1) Добавляем новые колонки (временно допускаем NULL)
    op.add_column(
        TABLE,
        sa.Column("run_schedule_dttm", sa.TIMESTAMP(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        TABLE,
        sa.Column("receive_dttm", sa.TIMESTAMP(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        TABLE,
        sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=True),
        schema=SCHEMA,
    )

    # 2) Бэкофилл значений
    # 2.1 run_schedule_dttm = run_dttm
    op.execute(f"""
        UPDATE {SCHEMA}.{TABLE}
        SET run_schedule_dttm = run_dttm
        WHERE run_schedule_dttm IS NULL
    """)

    # 2.2 receive_dttm = response_dttm
    op.execute(f"""
        UPDATE {SCHEMA}.{TABLE}
        SET receive_dttm = COALESCE(receive_dttm, response_dttm)
        WHERE receive_dttm IS NULL
    """)

    # 2.3 business_dttm (point-in-time, daily):
    # начало вчерашнего дня в часовом поясе Europe/Moscow
    # robust к таймзоне БД: считаем «локальное MSK» и конвертируем обратно в timestamptz
    op.execute(f"""
        UPDATE {SCHEMA}.{TABLE}
        SET business_dttm = COALESCE(
            business_dttm,
            timezone('Europe/Moscow',
                date_trunc('day', (run_schedule_dttm AT TIME ZONE 'Europe/Moscow') - interval '1 day')
            )
        )
        WHERE business_dttm IS NULL
    """)

    # 3) Ужесточаем ограничения (по Mixin2)
    op.alter_column(
        TABLE, "run_schedule_dttm",
        schema=SCHEMA,
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=False,
    )
    op.alter_column(
        TABLE, "business_dttm",
        schema=SCHEMA,
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=False,
    )
    # receive_dttm остаётся nullable=True

    # 4) Комментарии для соответствия спецификации
    op.execute(f"COMMENT ON COLUMN {SCHEMA}.{TABLE}.run_schedule_dttm IS 'Плановое время запуска DAG Run (МСК). Одинаковое для записей с одним run_uuid.'")
    op.execute(f"COMMENT ON COLUMN {SCHEMA}.{TABLE}.receive_dttm     IS 'timestamp получения ответа, с таймзоной, время нашего сервера (MSK).'")
    op.execute(f"COMMENT ON COLUMN {SCHEMA}.{TABLE}.business_dttm    IS 'Дата, указывающая на период лога/снэпшота (point-in-time: начало периода в MSK).'")

def downgrade() -> None:
    # Снимаем NOT NULL
    op.alter_column(
        TABLE, "business_dttm",
        schema=SCHEMA,
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=True,
    )
    op.alter_column(
        TABLE, "run_schedule_dttm",
        schema=SCHEMA,
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=True,
    )

    # Удаляем колонки
    op.drop_column(TABLE, "business_dttm", schema=SCHEMA)
    op.drop_column(TABLE, "receive_dttm",  schema=SCHEMA)
    op.drop_column(TABLE, "run_schedule_dttm", schema=SCHEMA)