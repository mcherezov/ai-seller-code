from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# IDs
revision: str = "5b0068c20ac3"
down_revision: Union[str, None] = "5912fb069494"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

schema = "bronze"
table = "wb_www_text_search_1d"

def upgrade():
    # 1) Новые колонки из AbstractMixin (идемпотентно)
    op.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS run_schedule_dttm TIMESTAMPTZ;")
    op.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS receive_dttm     TIMESTAMPTZ;")
    op.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS business_dttm    TIMESTAMPTZ;")

    # 2) response_code: дефолт 0 (и так NOT NULL по модели)
    op.execute(f"ALTER TABLE {schema}.{table} ALTER COLUMN response_code SET DEFAULT 0;")

    # 3) Бэκфиллы под AbstractMixin
    # 3.1 run_schedule_dttm := COALESCE(run_schedule_dttm, run_dttm, request_dttm)
    op.execute(f"""
    UPDATE {schema}.{table}
       SET run_schedule_dttm = COALESCE(run_schedule_dttm, run_dttm, request_dttm)
     WHERE run_schedule_dttm IS NULL;
    """)

    # 3.2 receive_dttm := COALESCE(receive_dttm, request_dttm)
    op.execute(f"""
    UPDATE {schema}.{table}
       SET receive_dttm = COALESCE(receive_dttm, request_dttm)
     WHERE receive_dttm IS NULL;
    """)

    # 3.3 response_dttm в AbstractMixin NOT NULL → если NULL, ставим request_dttm
    op.execute(f"""
    UPDATE {schema}.{table}
       SET response_dttm = request_dttm
     WHERE response_dttm IS NULL;
    """)

    # 3.4 business_dttm = date_trunc('day', run_dttm - interval '1 day')  -- «за вчера»
    #     если run_dttm отсутствует (на всякий случай) → берем request_dttm - 1 день
    op.execute(f"""
    UPDATE {schema}.{table}
       SET business_dttm = date_trunc('day', COALESCE(run_dttm, request_dttm) - INTERVAL '1 day')
     WHERE business_dttm IS NULL;
    """)

    # 4) Выставляем NOT NULL как в AbstractMixin
    for col in ("run_dttm", "run_schedule_dttm", "response_dttm", "receive_dttm",
                "inserted_at", "request_dttm", "response_code", "business_dttm"):
        op.execute(f"ALTER TABLE {schema}.{table} ALTER COLUMN {col} SET NOT NULL;")

def downgrade():
    # Откат к ExtendedMixin
    # 1) response_dttm снова nullable
    op.execute(f"ALTER TABLE {schema}.{table} ALTER COLUMN response_dttm DROP NOT NULL;")
    # 2) убрать DEFAULT с response_code
    op.execute(f"ALTER TABLE {schema}.{table} ALTER COLUMN response_code DROP DEFAULT;")
    # 3) удалить колонки, которых не было в старом миксине
    op.execute(f"ALTER TABLE {schema}.{table} DROP COLUMN IF EXISTS business_dttm;")
    op.execute(f"ALTER TABLE {schema}.{table} DROP COLUMN IF EXISTS receive_dttm;")
    op.execute(f"ALTER TABLE {schema}.{table} DROP COLUMN IF EXISTS run_schedule_dttm;")