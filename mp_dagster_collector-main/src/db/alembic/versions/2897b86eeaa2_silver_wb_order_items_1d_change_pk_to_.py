"""silver.wb_order_items_1d: change PK to (business_dttm, sr_id)

Revision ID: 2897b86eeaa2
Revises: 4486bddc58bb
Create Date: 2025-08-19 16:25:46.865679

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2897b86eeaa2'
down_revision: Union[str, None] = '4486bddc58bb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SCHEMA = "silver"
TABLE  = "wb_order_items_1d"

NEW_IDX = "ux_wb_order_items_1d_business_dttm_sr_id"
OLD_IDX = "ux_wb_order_items_1d_sr_id_last_change"  # для downgrade


def upgrade() -> None:
    # 0) Предварительные гарантии
    # A. Убедиться, что столбцы NOT NULL (на всякий случай)
    op.alter_column(TABLE, "business_dttm", schema=SCHEMA,
                    existing_type=sa.TIMESTAMP(timezone=True), nullable=False)
    op.alter_column(TABLE, "sr_id", schema=SCHEMA,
                    existing_type=sa.String(), nullable=False)

    # B. Проверка на дубликаты пары (business_dttm, sr_id)
    op.execute(f"""
    DO $$
    DECLARE v_cnt bigint;
    BEGIN
      SELECT COUNT(*) INTO v_cnt
      FROM (
        SELECT business_dttm, sr_id, COUNT(*) AS c
        FROM {SCHEMA}.{TABLE}
        GROUP BY 1,2
        HAVING COUNT(*) > 1
      ) d;
      IF v_cnt > 0 THEN
        RAISE EXCEPTION 'Cannot change PK: found % duplicate (business_dttm, sr_id) pairs', v_cnt;
      END IF;
    END $$;
    """)

    # 1) Создаём уникальный индекс CONCURRENTLY на (business_dttm, sr_id)
    #    Вне транзакции
    with op.get_context().autocommit_block():
        op.execute(f"""
            CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {NEW_IDX}
            ON {SCHEMA}.{TABLE} (business_dttm, sr_id)
        """)

    # 2) Переключаем первичный ключ
    #    (DROP старый PK и ADD CONSTRAINT ... USING INDEX новый)
    op.execute(f'ALTER TABLE {SCHEMA}.{TABLE} DROP CONSTRAINT IF EXISTS wb_order_items_1d_pkey')
    op.execute(f'ALTER TABLE {SCHEMA}.{TABLE} ADD CONSTRAINT wb_order_items_1d_pkey PRIMARY KEY USING INDEX {NEW_IDX}')


def downgrade() -> None:
    # Возврат на старый PK: (sr_id, last_change_date)

    # 1) Убедимся, что столбцы NOT NULL (на случай ручных правок)
    op.alter_column(TABLE, "last_change_date", schema=SCHEMA,
                    existing_type=sa.TIMESTAMP(timezone=True), nullable=False)
    op.alter_column(TABLE, "sr_id", schema=SCHEMA,
                    existing_type=sa.String(), nullable=False)

    # 2) Проверка на дубликаты (sr_id, last_change_date)
    op.execute(f"""
    DO $$
    DECLARE v_cnt bigint;
    BEGIN
      SELECT COUNT(*) INTO v_cnt
      FROM (
        SELECT sr_id, last_change_date, COUNT(*) AS c
        FROM {SCHEMA}.{TABLE}
        GROUP BY 1,2
        HAVING COUNT(*) > 1
      ) d;
      IF v_cnt > 0 THEN
        RAISE EXCEPTION 'Cannot revert PK: found % duplicate (sr_id, last_change_date) pairs', v_cnt;
      END IF;
    END $$;
    """)

    # 3) Создаём уникальный индекс CONCURRENTLY на (sr_id, last_change_date),
    #    чтобы привязать его к старому PK
    with op.get_context().autocommit_block():
        op.execute(f"""
            CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {OLD_IDX}
            ON {SCHEMA}.{TABLE} (sr_id, last_change_date)
        """)

    # 4) Переключаем PK назад, используя этот индекс
    op.execute(f'ALTER TABLE {SCHEMA}.{TABLE} DROP CONSTRAINT IF EXISTS wb_order_items_1d_pkey')
    op.execute(f'ALTER TABLE {SCHEMA}.{TABLE} ADD CONSTRAINT wb_order_items_1d_pkey PRIMARY KEY USING INDEX {OLD_IDX}')

