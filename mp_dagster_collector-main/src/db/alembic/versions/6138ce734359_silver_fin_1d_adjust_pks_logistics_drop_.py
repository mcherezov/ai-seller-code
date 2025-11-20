"""silver(fin_1d): adjust PKs (logistics drop nm_id; sales confirm)

Revision ID: 6138ce734359
Revises: 8c54e437a7b3
Create Date: 2025-08-25 16:07:48.410324

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6138ce734359'
down_revision: Union[str, None] = '8c54e437a7b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SILVER = "silver"
T_LOG  = "wb_logistics_1d"
T_SALE = "wb_sales_1d"

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

    # ── wb_logistics_1d: PK -> (business_dttm, company_id, sr_id, logistic_type)
    if _constraint_exists(conn, SILVER, T_LOG, "wb_logistics_1d_pkey"):
        op.drop_constraint("wb_logistics_1d_pkey", T_LOG, type_="primary", schema=SILVER)
    op.create_primary_key(
        "wb_logistics_1d_pkey",
        T_LOG,
        ["business_dttm", "company_id", "sr_id", "logistic_type"],
        schema=SILVER,
    )

    # ── wb_sales_1d: PK -> (business_dttm, company_id, sr_id, payment_reason)
    if _constraint_exists(conn, SILVER, T_SALE, "wb_sales_1d_pkey"):
        op.drop_constraint("wb_sales_1d_pkey", T_SALE, type_="primary", schema=SILVER)
    op.create_primary_key(
        "wb_sales_1d_pkey",
        T_SALE,
        ["business_dttm", "company_id", "sr_id", "payment_reason"],
        schema=SILVER,
    )


def downgrade():
    conn = op.get_bind()

    # ── wb_sales_1d: вернуть прежний (расширенный) PK, если требуется
    if _constraint_exists(conn, SILVER, T_SALE, "wb_sales_1d_pkey"):
        op.drop_constraint("wb_sales_1d_pkey", T_SALE, type_="primary", schema=SILVER)
    # предыдущая ревизия тоже задавала этот же ключ; для совместимости оставим его же
    op.create_primary_key(
        "wb_sales_1d_pkey",
        T_SALE,
        ["business_dttm", "company_id", "sr_id", "payment_reason"],
        schema=SILVER,
    )

    # ── wb_logistics_1d: вернуть старый PK с nm_id
    if _constraint_exists(conn, SILVER, T_LOG, "wb_logistics_1d_pkey"):
        op.drop_constraint("wb_logistics_1d_pkey", T_LOG, type_="primary", schema=SILVER)
    op.create_primary_key(
        "wb_logistics_1d_pkey",
        T_LOG,
        ["business_dttm", "company_id", "sr_id", "logistic_type", "nm_id"],
        schema=SILVER,
    )
