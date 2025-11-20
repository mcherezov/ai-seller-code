"""bronze: add run_schedule_dttm, business_dttm, receive_dttm; enforce NOT NULL; backfill wb_adv_fullstats_1d

Revision ID: e551502ee06f
Revises: 2c42c03723ef
Create Date: 2025-08-22 08:21:47.269583

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e551502ee06f'
down_revision: Union[str, None] = '2c42c03723ef'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


BRONZE = "bronze"
TABLES = [
    "wb_adv_fullstats_1d",
    "wb_supplier_orders_1d",
    "wb_adv_promotions_1h",
]

def upgrade():
    # 1) Добавляем недостающие поля (как NULLable, чтобы можно было безопасно проставить данные)
    for t in TABLES:
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ADD COLUMN IF NOT EXISTS run_schedule_dttm TIMESTAMPTZ;
        """)
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ADD COLUMN IF NOT EXISTS business_dttm TIMESTAMPTZ;
        """)
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ADD COLUMN IF NOT EXISTS receive_dttm TIMESTAMPTZ;
        """)

        # Приводим default для response_code к 0 (NOT NULL уже есть по старой схеме)
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ALTER COLUMN response_code SET DEFAULT 0;
        """)

        # response_dttm мог быть NULL — заполняем из request_dttm
        op.execute(f"""
            UPDATE {BRONZE}.{t}
            SET response_dttm = request_dttm
            WHERE response_dttm IS NULL;
        """)

        # receive_dttm новое поле — заполняем из request_dttm
        op.execute(f"""
            UPDATE {BRONZE}.{t}
            SET receive_dttm = request_dttm
            WHERE receive_dttm IS NULL;
        """)

    # 2) Бэкфилл только для wb_adv_fullstats_1d
    op.execute(f"""
        UPDATE {BRONZE}.wb_adv_fullstats_1d
        SET run_schedule_dttm = run_dttm
        WHERE run_schedule_dttm IS NULL;
    """)
    op.execute(f"""
        UPDATE {BRONZE}.wb_adv_fullstats_1d
        SET business_dttm = run_dttm - INTERVAL '1 day'
        WHERE business_dttm IS NULL;
    """)

    # 3) Запрещаем NULL согласно целевой схеме
    for t in TABLES:
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ALTER COLUMN run_schedule_dttm SET NOT NULL;
        """)
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ALTER COLUMN business_dttm SET NOT NULL;
        """)
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ALTER COLUMN response_dttm SET NOT NULL;
        """)
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ALTER COLUMN receive_dttm SET NOT NULL;
        """)


def downgrade():
    # Минимальный откат: снимаем NOT NULL и убираем DEFAULT (колонки не удаляем)
    for t in TABLES:
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ALTER COLUMN receive_dttm DROP NOT NULL;
        """)
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ALTER COLUMN response_dttm DROP NOT NULL;
        """)
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ALTER COLUMN business_dttm DROP NOT NULL;
        """)
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ALTER COLUMN run_schedule_dttm DROP NOT NULL;
        """)
        op.execute(f"""
            ALTER TABLE {BRONZE}.{t}
            ALTER COLUMN response_code DROP DEFAULT;
        """)
    # Колонки не удаляем, т.к. часть таблиц могла иметь их ранее.