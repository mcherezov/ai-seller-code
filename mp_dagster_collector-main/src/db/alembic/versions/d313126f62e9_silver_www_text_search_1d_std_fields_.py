"""silver(www_text_search_1d): std fields, business_dttm backfill, PK=(business_dttm,keyword,nm_id), rename product_id->nm_id, NOT NULLs

Revision ID: d313126f62e9
Revises: 5b0068c20ac3
Create Date: 2025-08-28 16:39:36.153505

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd313126f62e9'
down_revision: Union[str, None] = '5b0068c20ac3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


schema = "silver"
table = "wb_www_text_search_1d"

def upgrade():
    # 1) product_id -> nm_id (если нужно)
    op.execute(f"""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='{schema}' AND table_name='{table}' AND column_name='product_id'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='{schema}' AND table_name='{table}' AND column_name='nm_id'
        ) THEN
            ALTER TABLE {schema}.{table} RENAME COLUMN product_id TO nm_id;
        END IF;
    END $$;
    """)

    # 2) Добавить недостающие колонки
    op.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS company_id   INTEGER;")
    op.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS inserted_at  TIMESTAMPTZ;")
    op.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS business_dttm TIMESTAMPTZ;")

    # inserted_at: дефолт + бэκфилл + NOT NULL
    op.execute(f"ALTER TABLE {schema}.{table} ALTER COLUMN inserted_at SET DEFAULT now();")
    op.execute(f"UPDATE {schema}.{table} SET inserted_at = COALESCE(inserted_at, now()) WHERE inserted_at IS NULL;")
    op.execute(f"ALTER TABLE {schema}.{table} ALTER COLUMN inserted_at SET NOT NULL;")

    # 3) business_dttm = date_trunc('day', response_dttm - 1 day), затем NOT NULL
    op.execute(f"""
    UPDATE {schema}.{table}
       SET business_dttm = date_trunc('day', response_dttm - INTERVAL '1 day')
     WHERE business_dttm IS NULL AND response_dttm IS NOT NULL;
    """)
    op.execute(f"ALTER TABLE {schema}.{table} ALTER COLUMN business_dttm SET NOT NULL;")

    # 4) Смена PK: (business_dttm, keyword, nm_id)
    op.execute(f"""
    DO $$
    BEGIN
        -- Пытаемся снести знакомые имена старого PK
        BEGIN
            ALTER TABLE {schema}.{table} DROP CONSTRAINT wb_www_text_search_1d_pkey;
        EXCEPTION WHEN undefined_object THEN
            BEGIN
                ALTER TABLE {schema}.{table} DROP CONSTRAINT wb_www_text_search_1d_pk;
            EXCEPTION WHEN undefined_object THEN
                -- нет старого PK с этими именами — ок
            END;
        END;
    END $$;
    """)
    op.execute(f"""
    ALTER TABLE {schema}.{table}
      ADD CONSTRAINT wb_www_text_search_1d_pkey
      PRIMARY KEY (business_dttm, keyword, nm_id);
    """)

def downgrade():
    # Откат PK к прежнему виду (request_uuid, keyword, date, product_id/nm_id)
    op.execute(f"ALTER TABLE {schema}.{table} DROP CONSTRAINT IF EXISTS wb_www_text_search_1d_pkey;")

    # Если есть product_id — используем его, иначе nm_id (на случай, если не откатываем rename)
    op.execute(f"""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='{schema}' AND table_name='{table}' AND column_name='product_id'
        ) THEN
            ALTER TABLE {schema}.{table}
              ADD CONSTRAINT wb_www_text_search_1d_pk
              PRIMARY KEY (request_uuid, keyword, date, product_id);
        ELSE
            ALTER TABLE {schema}.{table}
              ADD CONSTRAINT wb_www_text_search_1d_pk
              PRIMARY KEY (request_uuid, keyword, date, nm_id);
        END IF;
    END $$;
    """)

    # По желанию: вернуть nm_id -> product_id (если нужен строгий откат модели)
    op.execute(f"""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='{schema}' AND table_name='{table}' AND column_name='nm_id'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='{schema}' AND table_name='{table}' AND column_name='product_id'
        ) THEN
            ALTER TABLE {schema}.{table} RENAME COLUMN nm_id TO product_id;
        END IF;
    END $$;
    """)

    op.execute(f"ALTER TABLE {schema}.{table} DROP COLUMN IF EXISTS business_dttm;")
    op.execute(f"ALTER TABLE {schema}.{table} DROP COLUMN IF EXISTS inserted_at;")
    op.execute(f"ALTER TABLE {schema}.{table} DROP COLUMN IF EXISTS company_id;")
