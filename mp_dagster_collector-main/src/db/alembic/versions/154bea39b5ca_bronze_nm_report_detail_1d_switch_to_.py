"""bronze(nm_report_detail_1d): switch to Abstract mixin; silver(sales_funnels_1d,buyouts_percent_1d):
backup + truncate, add business/inserted_at, PK updates, NOT NULL fixes (no backfill in silver)

Revision ID: 154bea39b5ca
Revises: 433a59a574b1
Create Date: 2025-08-27 10:19:44.298914
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "154bea39b5ca"
down_revision: Union[str, None] = "433a59a574b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

BRONZE = "bronze"
SILVER = "silver"

T_BRONZE_NM = "wb_nm_report_detail_1d"
T_FUNNELS   = "wb_sales_funnels_1d"
T_BUYOUTS   = "wb_buyouts_percent_1d"

T_FUNNELS_BKP = f"{T_FUNNELS}_backup"
T_BUYOUTS_BKP = f"{T_BUYOUTS}_backup"


def upgrade():
    # =========================
    # 0) BACKUP + TRUNCATE silver-таблиц
    # =========================
    op.execute(f'CREATE TABLE "{SILVER}"."{T_FUNNELS_BKP}" AS TABLE "{SILVER}"."{T_FUNNELS}" WITH DATA;')
    op.execute(f'CREATE TABLE "{SILVER}"."{T_BUYOUTS_BKP}" AS TABLE "{SILVER}"."{T_BUYOUTS}" WITH DATA;')

    op.execute(f'TRUNCATE TABLE "{SILVER}"."{T_FUNNELS}";')
    op.execute(f'TRUNCATE TABLE "{SILVER}"."{T_BUYOUTS}";')

    # =========================
    # 1) BRONZE: nm_report_detail → AbstractMixin
    # =========================
    op.execute(f'ALTER TABLE "{BRONZE}"."{T_BRONZE_NM}" '
               f'ADD COLUMN IF NOT EXISTS run_schedule_dttm TIMESTAMPTZ')
    op.execute(f'ALTER TABLE "{BRONZE}"."{T_BRONZE_NM}" '
               f'ADD COLUMN IF NOT EXISTS receive_dttm TIMESTAMPTZ')
    op.execute(f'ALTER TABLE "{BRONZE}"."{T_BRONZE_NM}" '
               f'ADD COLUMN IF NOT EXISTS business_dttm TIMESTAMPTZ')

    # backfill техполей
    op.execute(f"""
        UPDATE "{BRONZE}"."{T_BRONZE_NM}"
           SET run_schedule_dttm = COALESCE(run_schedule_dttm, run_dttm)
         WHERE run_schedule_dttm IS NULL
    """)
    op.execute(f"""
        UPDATE "{BRONZE}"."{T_BRONZE_NM}"
           SET receive_dttm = COALESCE(receive_dttm, request_dttm)
         WHERE receive_dttm IS NULL
    """)
    op.execute(f"""
        UPDATE "{BRONZE}"."{T_BRONZE_NM}"
           SET response_dttm = COALESCE(response_dttm, request_dttm)
         WHERE response_dttm IS NULL
    """)
    # business_dttm = (response_dttm - 1 day) @ 00:00 MSK
    op.execute(f"""
        UPDATE "{BRONZE}"."{T_BRONZE_NM}"
           SET business_dttm = COALESCE(
               business_dttm,
               (date_trunc('day', (response_dttm AT TIME ZONE 'Europe/Moscow') - INTERVAL '1 day'))
               AT TIME ZONE 'Europe/Moscow'
           )
         WHERE business_dttm IS NULL
    """)

    # жёсткие NOT NULL/DEFAULT
    op.alter_column(T_BRONZE_NM, "response_dttm",     schema=BRONZE, existing_type=sa.TIMESTAMP(timezone=True), nullable=False)
    op.alter_column(T_BRONZE_NM, "run_schedule_dttm", schema=BRONZE, existing_type=sa.TIMESTAMP(timezone=True), nullable=False)
    op.alter_column(T_BRONZE_NM, "receive_dttm",      schema=BRONZE, existing_type=sa.TIMESTAMP(timezone=True), nullable=False)
    op.alter_column(T_BRONZE_NM, "business_dttm",     schema=BRONZE, existing_type=sa.TIMESTAMP(timezone=True), nullable=False)
    op.alter_column(T_BRONZE_NM, "response_code",     schema=BRONZE, existing_type=sa.Integer(), server_default=sa.text("0"), nullable=False)

    # =========================
    # 2) SILVER: wb_sales_funnels_1d — только DDL (без UPDATE)
    # =========================
    # добавить столбцы (если отсутствуют)
    op.execute(f'''
        ALTER TABLE "{SILVER}"."{T_FUNNELS}"
        ADD COLUMN IF NOT EXISTS inserted_at   TIMESTAMPTZ DEFAULT TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP),
        ADD COLUMN IF NOT EXISTS business_dttm TIMESTAMPTZ
    ''')
    # ужесточить NOT NULL (таблица пуста → безопасно)
    for col, typ in [
        ("inserted_at",   sa.TIMESTAMP(timezone=True)),
        ("business_dttm", sa.TIMESTAMP(timezone=True)),
        ("nm_id",         sa.Integer()),
        ("date",          sa.Date()),
        ("brand",         sa.String()),
        ("subject_id",    sa.Integer()),
        ("subject_name",  sa.String()),
        ("open_card_count",   sa.Integer()),
        ("add_to_cart_count", sa.Integer()),
        ("orders_count",      sa.Integer()),
        ("orders_sum",        sa.REAL()),
        ("buyouts_count",     sa.Integer()),
        ("buyouts_sum",       sa.Integer()),
        ("cancel_count",      sa.Integer()),
        ("cancel_sum",        sa.REAL()),
        ("avg_price",         sa.REAL()),
        ("stocks_mp",         sa.REAL()),
        ("stocks_wb",         sa.REAL()),
    ]:
        op.alter_column(T_FUNNELS, col, schema=SILVER, existing_type=typ, nullable=False)

    # PK → (business_dttm, nm_id)
    op.drop_constraint("pk_wb_sales_funnels_1d", T_FUNNELS, type_="primary", schema=SILVER)
    op.create_primary_key("pk_wb_sales_funnels_1d", T_FUNNELS, ["business_dttm", "nm_id"], schema=SILVER)

    # =========================
    # 3) SILVER: wb_buyouts_percent_1d — только DDL (без UPDATE)
    # =========================
    op.execute(f'''
        ALTER TABLE "{SILVER}"."{T_BUYOUTS}"
        ADD COLUMN IF NOT EXISTS inserted_at   TIMESTAMPTZ DEFAULT TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP),
        ADD COLUMN IF NOT EXISTS business_dttm TIMESTAMPTZ
    ''')

    op.alter_column(T_BUYOUTS, "inserted_at",   schema=SILVER, existing_type=sa.TIMESTAMP(timezone=True), nullable=False)
    op.alter_column(T_BUYOUTS, "business_dttm", schema=SILVER, existing_type=sa.TIMESTAMP(timezone=True), nullable=False)
    op.alter_column(T_BUYOUTS, "nm_id",         schema=SILVER, existing_type=sa.Integer(), nullable=False)

    op.drop_constraint("pk_wb_buyouts_percent_1d", T_BUYOUTS, type_="primary", schema=SILVER)
    op.create_primary_key("pk_wb_buyouts_percent_1d", T_BUYOUTS, ["business_dttm", "nm_id"], schema=SILVER)


def downgrade():
    # вернуть PK
    op.drop_constraint("pk_wb_buyouts_percent_1d", T_BUYOUTS, type_="primary", schema=SILVER)
    op.create_primary_key("pk_wb_buyouts_percent_1d", T_BUYOUTS, ["request_uuid", "date", "nm_id"], schema=SILVER)

    op.drop_constraint("pk_wb_sales_funnels_1d", T_FUNNELS, type_="primary", schema=SILVER)
    op.create_primary_key("pk_wb_sales_funnels_1d", T_FUNNELS, ["request_uuid", "date", "nm_id"], schema=SILVER)

    # ослабление NOT NULL (best effort)
    for col, typ in [
        ("brand", sa.String()),
        ("subject_id", sa.Integer()),
        ("subject_name", sa.String()),
        ("open_card_count", sa.Integer()),
        ("add_to_cart_count", sa.Integer()),
        ("orders_count", sa.Integer()),
        ("orders_sum", sa.REAL()),
        ("buyouts_count", sa.Integer()),
        ("buyouts_sum", sa.Integer()),
        ("cancel_count", sa.Integer()),
        ("cancel_sum", sa.REAL()),
        ("avg_price", sa.REAL()),
        ("stocks_mp", sa.REAL()),
        ("stocks_wb", sa.REAL()),
    ]:
        op.alter_column(T_FUNNELS, col, schema=SILVER, existing_type=typ, nullable=True)

    op.execute(f'ALTER TABLE "{SILVER}"."{T_FUNNELS}" DROP COLUMN IF EXISTS business_dttm')
    op.execute(f'ALTER TABLE "{SILVER}"."{T_FUNNELS}" DROP COLUMN IF EXISTS inserted_at')

    op.execute(f'ALTER TABLE "{SILVER}"."{T_BUYOUTS}" DROP COLUMN IF EXISTS business_dttm')
    op.execute(f'ALTER TABLE "{SILVER}"."{T_BUYOUTS}" DROP COLUMN IF EXISTS inserted_at')
