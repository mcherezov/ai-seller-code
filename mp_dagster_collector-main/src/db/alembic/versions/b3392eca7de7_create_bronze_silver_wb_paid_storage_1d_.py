"""create bronze/silver wb_paid_storage_1d_new tables

Revision ID: b3392eca7de7
Revises: 7d50377e2c3e
Create Date: 2025-08-29 17:41:40.257282

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql

# revision identifiers, used by Alembic.
revision: str = 'b3392eca7de7'
down_revision: Union[str, None] = '7d50377e2c3e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # -----------------------------
    # BRONZE: bronze.wb_paid_storage_1d_new
    # PK = request_uuid (из миксина)
    # company_id используется НАРЯДУ с api_token_id (оба NOT NULL)
    # Индексы для бэкфилла: (company_id, business_dttm), (api_token_id, business_dttm)
    # -----------------------------
    op.create_table(
        "wb_paid_storage_1d_new",
        sa.Column("request_uuid", psql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("api_token_id", sa.Integer(), sa.ForeignKey("core.tokens.token_id"), nullable=False, index=True),
        sa.Column("company_id", sa.Integer(), sa.ForeignKey("core.companies.company_id"), nullable=False, index=True),

        sa.Column("run_uuid", psql.UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("run_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("run_schedule_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=False),

        sa.Column("request_dttm", sa.TIMESTAMP(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("request_parameters", psql.JSONB, nullable=True),
        sa.Column("request_body", psql.JSONB, nullable=True),

        sa.Column("response_code", sa.Integer(), nullable=False,
                  server_default=sa.text("0")),
        sa.Column("response_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("receive_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("response_body", sa.Text(), nullable=True),

        sa.Column("inserted_at", sa.TIMESTAMP(timezone=True), nullable=False,
                  server_default=sa.text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)")),

        sa.PrimaryKeyConstraint("request_uuid", name="pk_bz_wb_paid_storage_1d_new"),
        sa.Index("ix_bz_wb_paid_storage_1d_new_company_biz", "company_id", "business_dttm"),
        sa.Index("ix_bz_wb_paid_storage_1d_new_token_biz", "api_token_id", "business_dttm"),
        schema="bronze",
    )

    # -----------------------------
    # SILVER: silver.wb_paid_storage_1d_new
    # PK = (business_dttm, warehouse_name, income_id, barcode, calc_type)
    # request_uuid → FK на bronze
    # company_id → FK на core.companies
    # Все бизнес-поля NOT NULL
    # -----------------------------
    op.create_table(
        "wb_paid_storage_1d_new",
        sa.Column("request_uuid", psql.UUID(as_uuid=True),
                  sa.ForeignKey("bronze.wb_paid_storage_1d_new.request_uuid"),
                  nullable=False),

        sa.Column("company_id", sa.Integer(),
                  sa.ForeignKey("core.companies.company_id"),
                  nullable=False),

        sa.Column("response_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("inserted_at", sa.TIMESTAMP(timezone=True), nullable=False,
                  server_default=sa.text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)")),

        sa.Column("date", sa.Date(), nullable=False),

        sa.Column("warehouse_id", sa.Integer(), nullable=False),
        sa.Column("warehouse_name", sa.String(), nullable=False),
        sa.Column("warehouse_coef", sa.Float(), nullable=False),
        sa.Column("income_id", sa.Integer(), nullable=False),
        sa.Column("chrt_id", sa.Integer(), nullable=False),
        sa.Column("tech_size", sa.String(), nullable=False),
        sa.Column("barcode", sa.String(), nullable=False),
        sa.Column("supplier_article", sa.String(), nullable=False),
        sa.Column("nm_id", sa.Integer(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("calc_type", sa.String(), nullable=False),
        sa.Column("warehouse_price", sa.Float(), nullable=False),
        sa.Column("barcodes_count", sa.Integer(), nullable=False),
        sa.Column("pallet_place_code", sa.Integer(), nullable=False),
        sa.Column("pallet_count", sa.Float(), nullable=False),
        sa.Column("loyalty_discount", sa.Float(), nullable=False),

        sa.PrimaryKeyConstraint(
            "business_dttm", "warehouse_name", "income_id", "barcode", "calc_type",
            name="pk_paid_storage_1d_new"
        ),
        sa.Index("ix_swps1d_new_company_biz", "company_id", "business_dttm"),
        sa.Index("ix_swps1d_new_request_uuid", "request_uuid"),
        schema="silver",
    )


def downgrade():
    # Удаляем SILVER (сначала, из-за FK на bronze)
    op.drop_index("ix_swps1d_new_request_uuid", table_name="wb_paid_storage_1d_new", schema="silver")
    op.drop_index("ix_swps1d_new_company_biz", table_name="wb_paid_storage_1d_new", schema="silver")
    op.drop_table("wb_paid_storage_1d_new", schema="silver")

    # Удаляем BRONZE
    op.drop_index("ix_bz_wb_paid_storage_1d_new_token_biz", table_name="wb_paid_storage_1d_new", schema="bronze")
    op.drop_index("ix_bz_wb_paid_storage_1d_new_company_biz", table_name="wb_paid_storage_1d_new", schema="bronze")
    op.drop_table("wb_paid_storage_1d_new", schema="bronze")

