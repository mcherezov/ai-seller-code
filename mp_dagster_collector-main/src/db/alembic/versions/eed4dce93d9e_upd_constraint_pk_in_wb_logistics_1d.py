"""upd constraint pk in wb_logistics_1d

Revision ID: eed4dce93d9e
Revises: 92b88bbbd3c4
Create Date: 2025-08-01 18:27:17.694214

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'eed4dce93d9e'
down_revision: Union[str, None] = '92b88bbbd3c4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Удаляем старый PK
    op.drop_constraint(
        constraint_name="wb_logistics_1d_pkey",
        table_name="wb_logistics_1d",
        schema="silver",
        type_="primary"
    )

    # Делаем nm_id и supplier_article NOT NULL
    op.alter_column("wb_logistics_1d", "nm_id",
                    schema="silver",
                    existing_type=sa.Integer(),
                    nullable=False)

    op.alter_column("wb_logistics_1d", "supplier_article",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=False)

    # Создаём новый составной PK
    op.create_primary_key(
        constraint_name="wb_logistics_1d_pkey",
        table_name="wb_logistics_1d",
        columns=["request_uuid", "sr_id", "logistic_type", "nm_id", "supplier_article"],
        schema="silver"
    )


def downgrade():
    # Удаляем расширенный PK
    op.drop_constraint(
        constraint_name="wb_logistics_1d_pkey",
        table_name="wb_logistics_1d",
        schema="silver",
        type_="primary"
    )

    # Делаем колонки снова nullable
    op.alter_column("wb_logistics_1d", "nm_id",
                    schema="silver",
                    existing_type=sa.Integer(),
                    nullable=True)

    op.alter_column("wb_logistics_1d", "supplier_article",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=True)

    # Восстанавливаем исходный PK
    op.create_primary_key(
        constraint_name="wb_logistics_1d_pkey",
        table_name="wb_logistics_1d",
        columns=["request_uuid", "sr_id", "logistic_type"],
        schema="silver"
    )