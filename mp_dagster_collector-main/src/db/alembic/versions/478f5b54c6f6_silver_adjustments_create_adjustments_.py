"""silver(adjustments): create adjustments_by_product & adjustments_general

Revision ID: 478f5b54c6f6
Revises: d313126f62e9
Create Date: 2025-08-29 08:29:40.296704

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '478f5b54c6f6'
down_revision: Union[str, None] = 'd313126f62e9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # silver.adjustments_by_product
    op.create_table(
        "adjustments_by_product",
        sa.Column("request_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("inserted_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("response_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=False),

        sa.Column("income_id", sa.BigInteger(), nullable=True),
        sa.Column("subject", sa.String(), nullable=True),
        sa.Column("nomenclature_code", sa.BigInteger(), nullable=True),
        sa.Column("brand", sa.String(), nullable=True),
        sa.Column("supplier_article", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("tech_size", sa.String(), nullable=True),
        sa.Column("barcode", sa.String(), nullable=True),
        sa.Column("doc_type_name", sa.String(), nullable=True),
        sa.Column("supplier_oper_name", sa.String(), nullable=False),
        sa.Column("order_date", sa.Date(), nullable=True),
        sa.Column("sale_date", sa.Date(), nullable=True),
        sa.Column("penalty", sa.Numeric(10, 2), nullable=True),
        sa.Column("bonus_type_name", sa.String(), nullable=True),
        sa.Column("office_number", sa.String(), nullable=True),
        sa.Column("warehouse_name", sa.String(), nullable=True),
        sa.Column("country", sa.String(), nullable=True),
        sa.Column("box_type", sa.String(), nullable=True),
        sa.Column("shk_id", sa.String(), nullable=True),
        sa.Column("sr_id", sa.String(), nullable=False),
        sa.Column("seller_payout", sa.Numeric(10, 2), nullable=True),
        sa.Column("additional_payment", sa.Numeric(10, 2), nullable=True),
        sa.Column("cashback_amount", sa.Numeric(10, 2), nullable=True),
        sa.Column("cashback_discount", sa.Numeric(10, 2), nullable=True),

        sa.PrimaryKeyConstraint("business_dttm", "sr_id", "supplier_oper_name", name="pk_adjustments_by_product"),
        schema="silver",
    )

    op.create_index(
        "ix_adjustments_by_product_business_dttm",
        "adjustments_by_product",
        ["business_dttm"],
        unique=False,
        schema="silver",
    )
    op.create_index(
        "ix_adjustments_by_product_company_id",
        "adjustments_by_product",
        ["company_id"],
        unique=False,
        schema="silver",
    )

    # silver.adjustments_general
    op.create_table(
        "adjustments_general",
        sa.Column("request_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("inserted_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("response_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=False),

        sa.Column("income_id", sa.BigInteger(), nullable=True),
        sa.Column("subject", sa.String(), nullable=True),
        sa.Column("nm_id", sa.BigInteger(), nullable=True),
        sa.Column("brand", sa.String(), nullable=True),
        sa.Column("supplier_article", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("tech_size", sa.String(), nullable=True),
        sa.Column("barcode", sa.String(), nullable=True),
        sa.Column("doc_type_name", sa.String(), nullable=True),
        sa.Column("supplier_oper_name", sa.String(), nullable=False),
        sa.Column("order_date", sa.Date(), nullable=True),
        sa.Column("sale_date", sa.Date(), nullable=True),
        sa.Column("penalty", sa.Numeric(10, 2), nullable=True),
        sa.Column("bonus_type_name", sa.String(), nullable=True),
        sa.Column("office_number", sa.String(), nullable=True),
        sa.Column("warehouse_name", sa.String(), nullable=True),
        sa.Column("country", sa.String(), nullable=True),
        sa.Column("box_type", sa.String(), nullable=True),
        sa.Column("shk_id", sa.String(), nullable=True),
        sa.Column("sr_id", sa.String(), nullable=False),
        sa.Column("deduction", sa.Numeric(10, 2), nullable=True),

        sa.PrimaryKeyConstraint("business_dttm", "sr_id", "supplier_oper_name", name="pk_adjustments_general"),
        schema="silver",
    )

    op.create_index(
        "ix_adjustments_general_business_dttm",
        "adjustments_general",
        ["business_dttm"],
        unique=False,
        schema="silver",
    )
    op.create_index(
        "ix_adjustments_general_company_id",
        "adjustments_general",
        ["company_id"],
        unique=False,
        schema="silver",
    )


def downgrade() -> None:
    op.drop_index("ix_adjustments_general_company_id", table_name="adjustments_general", schema="silver")
    op.drop_index("ix_adjustments_general_business_dttm", table_name="adjustments_general", schema="silver")
    op.drop_table("adjustments_general", schema="silver")

    op.drop_index("ix_adjustments_by_product_company_id", table_name="adjustments_by_product", schema="silver")
    op.drop_index("ix_adjustments_by_product_business_dttm", table_name="adjustments_by_product", schema="silver")
    op.drop_table("adjustments_by_product", schema="silver")