"""upd constraint pk in wb_compensations_1d

Revision ID: 78d1e5de2adc
Revises: ccee9e0f5905
Create Date: 2025-08-01 18:03:10.615392

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '78d1e5de2adc'
down_revision: Union[str, None] = 'ccee9e0f5905'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 1. Удаляем старый PK
    op.drop_constraint(
        constraint_name="wb_compensations_1d_pkey",
        table_name="wb_compensations_1d",
        schema="silver",
        type_="primary"
    )

    # 2. Обновляем nullable=False для ключевых колонок (если требуется)
    op.alter_column("wb_compensations_1d", "nm_id",
                    schema="silver",
                    existing_type=sa.Integer(),
                    nullable=False)

    op.alter_column("wb_compensations_1d", "supplier_article",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=False)

    # 3. Создаем новый составной PK
    op.create_primary_key(
        constraint_name="wb_compensations_1d_pkey",
        table_name="wb_compensations_1d",
        columns=["request_uuid", "compensation_id", "nm_id", "supplier_article"],
        schema="silver"
    )


def downgrade():
    # Откат — возвращаем старый ключ
    op.drop_constraint(
        constraint_name="wb_compensations_1d_pkey",
        table_name="wb_compensations_1d",
        schema="silver",
        type_="primary"
    )

    op.create_primary_key(
        constraint_name="wb_compensations_1d_pkey",
        table_name="wb_compensations_1d",
        columns=["request_uuid", "compensation_id"],
        schema="silver"
    )

    # Возвращаем nullable обратно (если нужно)
    op.alter_column("wb_compensations_1d", "nm_id",
                    schema="silver",
                    existing_type=sa.Integer(),
                    nullable=True)

    op.alter_column("wb_compensations_1d", "supplier_article",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=True)