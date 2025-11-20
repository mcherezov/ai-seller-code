"""upd cashback tables

Revision ID: 144b2a3dddc6
Revises: 995adc35a36c
Create Date: 2025-08-17 10:53:17.796862

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '144b2a3dddc6'
down_revision: Union[str, None] = '995adc35a36c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ─────────────────────────────────────────────────────────────
    # BRONZE: добавить новые тайминги
    # ─────────────────────────────────────────────────────────────
    op.add_column(
        "wb_www_cashback_reports_1w",
        sa.Column("run_schedule_dttm", sa.TIMESTAMP(timezone=True), nullable=True),
        schema="bronze",
    )
    op.add_column(
        "wb_www_cashback_reports_1w",
        sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=True),
        schema="bronze",
    )
    op.add_column(
        "wb_www_cashback_reports_1w",
        sa.Column("receive_dttm", sa.TIMESTAMP(timezone=True), nullable=True),
        schema="bronze",
    )

    # ─────────────────────────────────────────────────────────────
    # SILVER: добавить business_dttm и заменить PK
    # таблица пустая — можно сразу NOT NULL без default
    # ─────────────────────────────────────────────────────────────
    op.add_column(
        "wb_www_cashback_reports_1w",
        sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
        schema="silver",
    )

    # снять старый PK (скорее всего default-имя: wb_www_cashback_reports_1w_pkey)
    op.drop_constraint(
        "wb_www_cashback_reports_1w_pkey",
        "wb_www_cashback_reports_1w",
        schema="silver",
        type_="primary",
    )

    # создать новый PK с осознанным именем
    op.create_primary_key(
        "pk_silver_wb_www_cashback_reports_1w",
        "wb_www_cashback_reports_1w",
        ["company_id", "business_dttm"],
        schema="silver",
    )


def downgrade() -> None:
    # ─────────────────────────────────────────────────────────────
    # SILVER: вернуть старый PK и убрать business_dttm
    # ─────────────────────────────────────────────────────────────
    op.drop_constraint(
        "pk_silver_wb_www_cashback_reports_1w",
        "wb_www_cashback_reports_1w",
        schema="silver",
        type_="primary",
    )
    op.create_primary_key(
        "wb_www_cashback_reports_1w_pkey",
        "wb_www_cashback_reports_1w",
        ["company_id", "request_uuid"],
        schema="silver",
    )
    op.drop_column("wb_www_cashback_reports_1w", "business_dttm", schema="silver")

    # ─────────────────────────────────────────────────────────────
    # BRONZE: убрать добавленные тайминги
    # ─────────────────────────────────────────────────────────────
    op.drop_column("wb_www_cashback_reports_1w", "receive_dttm", schema="bronze")
    op.drop_column("wb_www_cashback_reports_1w", "business_dttm", schema="bronze")
    op.drop_column("wb_www_cashback_reports_1w", "run_schedule_dttm", schema="bronze")