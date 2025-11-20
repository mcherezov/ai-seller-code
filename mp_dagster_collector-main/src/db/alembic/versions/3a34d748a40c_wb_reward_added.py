"""wb_reward added

Revision ID: 3a34d748a40c
Revises: eb679aeb0a74
Create Date: 2025-08-02 02:52:23.945171

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3a34d748a40c'
down_revision: Union[str, None] = 'eb679aeb0a74'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade():
    # === wb_sales_1d ===
    op.add_column(
        "wb_sales_1d",
        sa.Column("wb_reward", sa.Numeric(12, 2), nullable=True),
        schema="silver",
    )
    # TODO: заменить backfill на корректное значение вознаграждения ВВ без НДС из источника
    op.execute("UPDATE silver.wb_sales_1d SET wb_reward = 0 WHERE wb_reward IS NULL")
    op.alter_column("wb_sales_1d", "wb_reward", nullable=False, schema="silver")
    op.drop_constraint("wb_sales_1d_pkey", "wb_sales_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_sales_1d_pkey",
        "wb_sales_1d",
        [
            "request_uuid",
            "sr_id",
            "payment_reason",
            "nm_id",
            "supplier_article",
            "order_date",
            "warehouse_name",
            "country",
            "logistic_type",
            "wb_reward",
        ],
        schema="silver",
    )

    # === wb_fines_1d ===
    op.add_column(
        "wb_fines_1d",
        sa.Column("wb_reward", sa.Numeric(12, 2), nullable=True),
        schema="silver",
    )
    op.execute("UPDATE silver.wb_fines_1d SET wb_reward = 0 WHERE wb_reward IS NULL")
    op.alter_column("wb_fines_1d", "wb_reward", nullable=False, schema="silver")
    op.drop_constraint("wb_fines_1d_pkey", "wb_fines_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_fines_1d_pkey",
        "wb_fines_1d",
        [
            "request_uuid",
            "fine_id",
            "nm_id",
            "supplier_article",
            "wb_reward",
        ],
        schema="silver",
    )

    # === wb_logistics_1d ===
    op.add_column(
        "wb_logistics_1d",
        sa.Column("wb_reward", sa.Numeric(12, 2), nullable=True),
        schema="silver",
    )
    op.execute("UPDATE silver.wb_logistics_1d SET wb_reward = 0 WHERE wb_reward IS NULL")
    op.alter_column("wb_logistics_1d", "wb_reward", nullable=False, schema="silver")
    op.drop_constraint("wb_logistics_1d_pkey", "wb_logistics_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_logistics_1d_pkey",
        "wb_logistics_1d",
        [
            "request_uuid",
            "sr_id",
            "logistic_type",
            "nm_id",
            "supplier_article",
            "wb_reward",
        ],
        schema="silver",
    )

    # === wb_compensations_1d ===
    op.add_column(
        "wb_compensations_1d",
        sa.Column("wb_reward", sa.Numeric(12, 2), nullable=True),
        schema="silver",
    )
    op.execute("UPDATE silver.wb_compensations_1d SET wb_reward = 0 WHERE wb_reward IS NULL")
    op.alter_column("wb_compensations_1d", "wb_reward", nullable=False, schema="silver")
    op.drop_constraint("wb_compensations_1d_pkey", "wb_compensations_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_compensations_1d_pkey",
        "wb_compensations_1d",
        [
            "request_uuid",
            "compensation_id",
            "nm_id",
            "supplier_article",
            "wb_reward",
        ],
        schema="silver",
    )

    # === wb_retentions_1d ===
    op.add_column(
        "wb_retentions_1d",
        sa.Column("wb_reward", sa.Numeric(12, 2), nullable=True),
        schema="silver",
    )
    op.execute("UPDATE silver.wb_retentions_1d SET wb_reward = 0 WHERE wb_reward IS NULL")
    op.alter_column("wb_retentions_1d", "wb_reward", nullable=False, schema="silver")
    op.drop_constraint("wb_retentions_1d_pkey", "wb_retentions_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_retentions_1d_pkey",
        "wb_retentions_1d",
        [
            "request_uuid",
            "retention_id",
            "wb_reward",
        ],
        schema="silver",
    )


def downgrade():
    # === wb_retentions_1d ===
    op.drop_constraint("wb_retentions_1d_pkey", "wb_retentions_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_retentions_1d_pkey",
        "wb_retentions_1d",
        ["request_uuid", "retention_id"],
        schema="silver",
    )
    op.drop_column("wb_retentions_1d", "wb_reward", schema="silver")

    # === wb_compensations_1d ===
    op.drop_constraint("wb_compensations_1d_pkey", "wb_compensations_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_compensations_1d_pkey",
        "wb_compensations_1d",
        ["request_uuid", "compensation_id", "nm_id", "supplier_article"],
        schema="silver",
    )
    op.drop_column("wb_compensations_1d", "wb_reward", schema="silver")

    # === wb_logistics_1d ===
    op.drop_constraint("wb_logistics_1d_pkey", "wb_logistics_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_logistics_1d_pkey",
        "wb_logistics_1d",
        ["request_uuid", "sr_id", "logistic_type", "nm_id", "supplier_article"],
        schema="silver",
    )
    op.drop_column("wb_logistics_1d", "wb_reward", schema="silver")

    # === wb_fines_1d ===
    op.drop_constraint("wb_fines_1d_pkey", "wb_fines_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_fines_1d_pkey",
        "wb_fines_1d",
        ["request_uuid", "fine_id", "nm_id", "supplier_article"],
        schema="silver",
    )
    op.drop_column("wb_fines_1d", "wb_reward", schema="silver")

    # === wb_sales_1d ===
    op.drop_constraint("wb_sales_1d_pkey", "wb_sales_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_sales_1d_pkey",
        "wb_sales_1d",
        [
            "request_uuid",
            "sr_id",
            "payment_reason",
            "nm_id",
            "supplier_article",
            "order_date",
            "warehouse_name",
            "country",
            "logistic_type",
        ],
        schema="silver",
    )
    op.drop_column("wb_sales_1d", "wb_reward", schema="silver")
