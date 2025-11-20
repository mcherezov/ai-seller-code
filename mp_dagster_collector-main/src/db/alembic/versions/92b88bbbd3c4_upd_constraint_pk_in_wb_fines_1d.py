"""upd constraint pk in wb_fines_1d

Revision ID: 92b88bbbd3c4
Revises: 78d1e5de2adc
Create Date: 2025-08-01 18:20:00.555400

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '92b88bbbd3c4'
down_revision: Union[str, None] = '78d1e5de2adc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Удаляем старый PK
    op.drop_constraint(
        constraint_name="wb_fines_1d_pkey",
        table_name="wb_fines_1d",
        schema="silver",
        type_="primary"
    )

    # Делаем nm_id и supplier_article NOT NULL
    op.alter_column("wb_fines_1d", "nm_id",
                    schema="silver",
                    existing_type=sa.Integer(),
                    nullable=False)

    op.alter_column("wb_fines_1d", "supplier_article",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=False)

    # Создаем новый составной PK
    op.create_primary_key(
        constraint_name="wb_fines_1d_pkey",
        table_name="wb_fines_1d",
        columns=["request_uuid", "fine_id", "nm_id", "supplier_article"],
        schema="silver"
    )


def downgrade():
    # Удаляем новый составной PK
    op.drop_constraint(
        constraint_name="wb_fines_1d_pkey",
        table_name="wb_fines_1d",
        schema="silver",
        type_="primary"
    )

    # Возвращаем NULLABLE
    op.alter_column("wb_fines_1d", "nm_id",
                    schema="silver",
                    existing_type=sa.Integer(),
                    nullable=True)

    op.alter_column("wb_fines_1d", "supplier_article",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=True)

    # Восстанавливаем старый PK
    op.create_primary_key(
        constraint_name="wb_fines_1d_pkey",
        table_name="wb_fines_1d",
        columns=["request_uuid", "fine_id"],
        schema="silver"
    )