"""silver(sales/logistics_1d): drop date, remove company_id from PKs

Revision ID: 433a59a574b1
Revises: 6138ce734359
Create Date: 2025-08-27 09:50:36.972378

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '433a59a574b1'
down_revision: Union[str, None] = '6138ce734359'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SCHEMA = "silver"
T_SALES = "wb_sales_1d"
T_LOG   = "wb_logistics_1d"


def upgrade():
    # 1) PK sales: (business_dttm, company_id, sr_id, payment_reason) -> (business_dttm, sr_id, payment_reason)
    op.drop_constraint(f"{T_SALES}_pkey", T_SALES, type_="primary", schema=SCHEMA)
    op.create_primary_key(
        f"{T_SALES}_pkey",
        T_SALES,
        ["business_dttm", "sr_id", "payment_reason"],
        schema=SCHEMA,
    )

    # 2) PK logistics: (business_dttm, company_id, sr_id, logistic_type) -> (business_dttm, sr_id, logistic_type)
    op.drop_constraint(f"{T_LOG}_pkey", T_LOG, type_="primary", schema=SCHEMA)
    op.create_primary_key(
        f"{T_LOG}_pkey",
        T_LOG,
        ["business_dttm", "sr_id", "logistic_type"],
        schema=SCHEMA,
    )

    # 3) Дроп колонки date, если есть (идемпотентно)
    op.execute(f'ALTER TABLE "{SCHEMA}"."{T_SALES}" DROP COLUMN IF EXISTS "date"')
    op.execute(f'ALTER TABLE "{SCHEMA}"."{T_LOG}"   DROP COLUMN IF EXISTS "date"')


def downgrade():
    # 1) Вернуть колонку date (nullable=True для безопасного даунгрейда)
    with op.batch_alter_table(T_LOG, schema=SCHEMA) as batch_op:
        batch_op.add_column(sa.Column("date", sa.Date(), nullable=True))
    with op.batch_alter_table(T_SALES, schema=SCHEMA) as batch_op:
        batch_op.add_column(sa.Column("date", sa.Date(), nullable=True))

    # 2) Вернуть старые PK (с company_id)
    op.drop_constraint(f"{T_LOG}_pkey", T_LOG, type_="primary", schema=SCHEMA)
    op.create_primary_key(
        f"{T_LOG}_pkey",
        T_LOG,
        ["business_dttm", "company_id", "sr_id", "logistic_type"],
        schema=SCHEMA,
    )

    op.drop_constraint(f"{T_SALES}_pkey", T_SALES, type_="primary", schema=SCHEMA)
    op.create_primary_key(
        f"{T_SALES}_pkey",
        T_SALES,
        ["business_dttm", "company_id", "sr_id", "payment_reason"],
        schema=SCHEMA,
    )