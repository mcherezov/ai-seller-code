"""silver(sales/logistics_1d): new PKs & NOT NULL adjustments

Revision ID: 8c54e437a7b3
Revises: b777fea1d803
Create Date: 2025-08-25 15:58:23.598362

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8c54e437a7b3'
down_revision: Union[str, None] = 'b777fea1d803'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SILVER = "silver"
T_SALE = "wb_sales_1d"
T_LOG  = "wb_logistics_1d"

def _col_exists(conn, schema: str, table: str, col: str) -> bool:
    q = sa.text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
          AND column_name  = :col
        LIMIT 1
    """)
    return conn.execute(q, {"schema": schema, "table": table, "col": col}).scalar() is not None

def _constraint_exists(conn, schema: str, table: str, cname: str) -> bool:
    q = sa.text("""
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = :schema
          AND t.relname = :table
          AND c.conname = :cname
        LIMIT 1
    """)
    return conn.execute(q, {"schema": schema, "table": table, "cname": cname}).scalar() is not None

def upgrade():
    conn = op.get_bind()

    # ── 1) Добавляем недостающие колонки (если их нет), сразу NOT NULL — таблицы пустые ──
    for tbl in (T_SALE, T_LOG):
        if not _col_exists(conn, SILVER, tbl, "business_dttm"):
            op.execute(f'ALTER TABLE "{SILVER}"."{tbl}" ADD COLUMN "business_dttm" TIMESTAMPTZ NOT NULL;')
        if not _col_exists(conn, SILVER, tbl, "response_dttm"):
            op.execute(f'ALTER TABLE "{SILVER}"."{tbl}" ADD COLUMN "response_dttm" TIMESTAMPTZ NOT NULL;')

    # На всякий случай — если колонка была, но nullable → делаем NOT NULL
    for tbl in (T_SALE, T_LOG):
        op.alter_column(tbl, "business_dttm", schema=SILVER,
                        existing_type=sa.TIMESTAMP(timezone=True), nullable=False)
        op.alter_column(tbl, "response_dttm", schema=SILVER,
                        existing_type=sa.TIMESTAMP(timezone=True), nullable=False)

    # ── 2) Ужесточаем NOT NULL в wb_sales_1d по новой модели ──
    nn_sales = [
        ("sale_date", sa.Date()),
        ("brand", sa.String()),
        ("name", sa.String()),
        ("barcode", sa.String()),
        ("tech_size", sa.String()),
        ("subject", sa.String()),
        ("quantity", sa.Integer()),
        ("price", sa.Float()),
        ("price_with_discount", sa.Float()),
        ("wb_realization_price", sa.Float()),
        ("spp", sa.Float()),
        ("seller_payout", sa.Float()),
        ("commision_percent", sa.Float()),
        ("acquiring_amount", sa.Float()),
        ("acquiring_percent", sa.Float()),
        ("acquiring_bank", sa.String()),
    ]
    for col, ctype in nn_sales:
        op.alter_column(T_SALE, col, schema=SILVER, existing_type=ctype, nullable=False)

    # ── 3) Меняем PK составы ──
    # wb_sales_1d → (business_dttm, company_id, sr_id, payment_reason)
    if _constraint_exists(conn, SILVER, T_SALE, "wb_sales_1d_pkey"):
        op.drop_constraint("wb_sales_1d_pkey", T_SALE, type_="primary", schema=SILVER)
    op.create_primary_key(
        "wb_sales_1d_pkey",
        T_SALE,
        ["business_dttm", "company_id", "sr_id", "payment_reason"],
        schema=SILVER,
    )

    # wb_logistics_1d → (business_dttm, company_id, sr_id, logistic_type, nm_id)
    if _constraint_exists(conn, SILVER, T_LOG, "wb_logistics_1d_pkey"):
        op.drop_constraint("wb_logistics_1d_pkey", T_LOG, type_="primary", schema=SILVER)
    op.create_primary_key(
        "wb_logistics_1d_pkey",
        T_LOG,
        ["business_dttm", "company_id", "sr_id", "logistic_type", "nm_id"],
        schema=SILVER,
    )

def downgrade():
    conn = op.get_bind()

    # Вернуть старые PK (прошлая версия, где логистика включала supplier_article,
    # а sales — расширенный ключ)
    if _constraint_exists(conn, SILVER, T_LOG, "wb_logistics_1d_pkey"):
        op.drop_constraint("wb_logistics_1d_pkey", T_LOG, type_="primary", schema=SILVER)
    op.create_primary_key(
        "wb_logistics_1d_pkey",
        T_LOG,
        ["business_dttm", "company_id", "sr_id", "logistic_type", "nm_id", "supplier_article"],
        schema=SILVER,
    )

    if _constraint_exists(conn, SILVER, T_SALE, "wb_sales_1d_pkey"):
        op.drop_constraint("wb_sales_1d_pkey", T_SALE, type_="primary", schema=SILVER)
    op.create_primary_key(
        "wb_sales_1d_pkey",
        T_SALE,
        [
            "business_dttm",
            "company_id",
            "sr_id",
            "payment_reason",
            "nm_id",
            "supplier_article",
            "order_date",
            "warehouse_name",
            "country",
        ],
        schema=SILVER,
    )

    # Ослабляем NOT NULL обратно (как было до ужесточения) для sales
    to_nullable = [
        ("sale_date", sa.Date()),
        ("brand", sa.String()),
        ("name", sa.String()),
        ("barcode", sa.String()),
        ("tech_size", sa.String()),
        ("subject", sa.String()),
        ("quantity", sa.Integer()),
        ("price", sa.Float()),
        ("price_with_discount", sa.Float()),
        ("wb_realization_price", sa.Float()),
        ("spp", sa.Float()),
        ("seller_payout", sa.Float()),
        ("commision_percent", sa.Float()),
        ("acquiring_amount", sa.Float()),
        ("acquiring_percent", sa.Float()),
        ("acquiring_bank", sa.String()),
    ]
    for col, ctype in to_nullable:
        op.alter_column(T_SALE, col, schema=SILVER, existing_type=ctype, nullable=True)