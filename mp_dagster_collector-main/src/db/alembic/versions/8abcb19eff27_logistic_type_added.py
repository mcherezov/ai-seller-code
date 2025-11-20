"""logistic_type added

Revision ID: 8abcb19eff27
Revises: 3a34d748a40c
Create Date: 2025-08-02 03:12:48.042748

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8abcb19eff27'
down_revision: Union[str, None] = '3a34d748a40c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade():
    # === wb_retentions_1d ===
    # 1. Добавляем колонку logistic_type (если её ещё нет)
    op.add_column(
        "wb_retentions_1d",
        sa.Column("logistic_type", sa.String(), nullable=True),
        schema="silver",
    )
    # 2. Заполняем существующие строки, чтобы не было NULL в будущем PK-ключе
    op.execute(
        "UPDATE silver.wb_retentions_1d SET logistic_type = 'UNDEF' WHERE logistic_type IS NULL"
    )
    # 3. Делаем not null (если хотим, чтобы в PK всегда было значение)
    op.alter_column(
        "wb_retentions_1d",
        "logistic_type",
        nullable=False,
        schema="silver",
    )
    # 4. Пересоздаём PK с логистик тайпом
    op.drop_constraint("wb_retentions_1d_pkey", "wb_retentions_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_retentions_1d_pkey",
        "wb_retentions_1d",
        ["request_uuid", "retention_id", "logistic_type", "wb_reward"],
        schema="silver",
    )

    # === wb_fines_1d ===
    op.add_column(
        "wb_fines_1d",
        sa.Column("logistic_type", sa.String(), nullable=True),
        schema="silver",
    )
    op.execute("UPDATE silver.wb_fines_1d SET logistic_type = 'UNDEF' WHERE logistic_type IS NULL")
    op.alter_column("wb_fines_1d", "logistic_type", nullable=False, schema="silver")
    op.drop_constraint("wb_fines_1d_pkey", "wb_fines_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_fines_1d_pkey",
        "wb_fines_1d",
        [
            "request_uuid",
            "fine_id",
            "nm_id",
            "supplier_article",
            "logistic_type",
            "wb_reward",
        ],
        schema="silver",
    )

    # === wb_compensations_1d ===
    op.add_column(
        "wb_compensations_1d",
        sa.Column("logistic_type", sa.String(), nullable=True),
        schema="silver",
    )
    op.execute(
        "UPDATE silver.wb_compensations_1d SET logistic_type = 'UNDEF' WHERE logistic_type IS NULL"
    )
    op.alter_column("wb_compensations_1d", "logistic_type", nullable=False, schema="silver")
    op.drop_constraint("wb_compensations_1d_pkey", "wb_compensations_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_compensations_1d_pkey",
        "wb_compensations_1d",
        [
            "request_uuid",
            "compensation_id",
            "nm_id",
            "supplier_article",
            "logistic_type",
            "wb_reward",
        ],
        schema="silver",
    )


def downgrade():
    # === wb_compensations_1d ===
    op.drop_constraint("wb_compensations_1d_pkey", "wb_compensations_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_compensations_1d_pkey",
        "wb_compensations_1d",
        ["request_uuid", "compensation_id", "nm_id", "supplier_article", "wb_reward"],
        schema="silver",
    )
    op.drop_column("wb_compensations_1d", "logistic_type", schema="silver")

    # === wb_fines_1d ===
    op.drop_constraint("wb_fines_1d_pkey", "wb_fines_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_fines_1d_pkey",
        "wb_fines_1d",
        ["request_uuid", "fine_id", "nm_id", "supplier_article", "wb_reward"],
        schema="silver",
    )
    op.drop_column("wb_fines_1d", "logistic_type", schema="silver")

    # === wb_retentions_1d ===
    op.drop_constraint("wb_retentions_1d_pkey", "wb_retentions_1d", schema="silver", type_="primary")
    op.create_primary_key(
        "wb_retentions_1d_pkey",
        "wb_retentions_1d",
        ["request_uuid", "retention_id", "wb_reward"],
        schema="silver",
    )
    op.drop_column("wb_retentions_1d", "logistic_type", schema="silver")
