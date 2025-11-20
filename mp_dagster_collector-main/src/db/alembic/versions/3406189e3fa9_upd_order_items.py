"""upd order_items

Revision ID: 3406189e3fa9
Revises: e244133918f7
Create Date: 2025-08-19 09:21:52.846213

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3406189e3fa9'
down_revision: Union[str, None] = 'e244133918f7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 0) safety: запретим миграцию, если есть NULL в last_change_date
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM silver.wb_order_items_1d
            WHERE last_change_date IS NULL
            LIMIT 1
        ) THEN
            RAISE EXCEPTION 'silver.wb_order_items_1d.last_change_date contains NULLs; fix data before applying PK';
        END IF;
    END$$;
    """)

    # 1) убрать лишний UNIQUE(sr_id) — это именно constraint
    op.execute("""
        ALTER TABLE silver.wb_order_items_1d
        DROP CONSTRAINT IF EXISTS wb_order_items_1d_sr_id_key;
    """)

    # 2) снять старый PK по sr_id
    op.execute("""
        ALTER TABLE silver.wb_order_items_1d
        DROP CONSTRAINT IF EXISTS wb_order_items_1d_pkey;
    """)

    # 3) убедиться, что составной уникальный индекс существует
    op.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind='i'
              AND c.relname='uq_wb_order_items_1d_sr_last_change'
              AND n.nspname='silver'
        ) THEN
            CREATE UNIQUE INDEX uq_wb_order_items_1d_sr_last_change
            ON silver.wb_order_items_1d (sr_id, last_change_date);
        END IF;
    END$$;
    """)

    # 4) повесить новый PK, используя существующий составной индекс
    op.execute("""
        ALTER TABLE silver.wb_order_items_1d
        ADD CONSTRAINT wb_order_items_1d_pkey
        PRIMARY KEY USING INDEX uq_wb_order_items_1d_sr_last_change;
    """)


def downgrade():
    # 1) снять композитный PK
    op.execute("""
        ALTER TABLE silver.wb_order_items_1d
        DROP CONSTRAINT IF EXISTS wb_order_items_1d_pkey;
    """)

    # 2) вернуть старый PK по sr_id
    op.execute("""
        ALTER TABLE silver.wb_order_items_1d
        ADD CONSTRAINT wb_order_items_1d_pkey PRIMARY KEY (sr_id);
    """)

    # 3) вернуть UNIQUE(sr_id), как было
    op.execute("""
        ALTER TABLE silver.wb_order_items_1d
        ADD CONSTRAINT wb_order_items_1d_sr_id_key UNIQUE (sr_id);
    """)

    # 4) (опционально) удалить составной индекс,
    # чтобы полностью вернуть прежнее состояние
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind='i'
              AND c.relname='uq_wb_order_items_1d_sr_last_change'
              AND n.nspname='silver'
        ) THEN
            DROP INDEX silver.uq_wb_order_items_1d_sr_last_change;
        END IF;
    END$$;
    """)