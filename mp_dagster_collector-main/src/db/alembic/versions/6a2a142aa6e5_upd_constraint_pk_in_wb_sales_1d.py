"""upd constraint pk in wb_sales_1d

Revision ID: 6a2a142aa6e5
Revises: eed4dce93d9e
Create Date: 2025-08-01 18:52:30.526979

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6a2a142aa6e5'
down_revision: Union[str, None] = 'eed4dce93d9e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Удаляем старый PK
    op.drop_constraint(
        constraint_name="wb_sales_1d_pkey",
        table_name="wb_sales_1d",
        schema="silver",
        type_="primary"
    )

    # Делаем нужные поля NOT NULL
    op.alter_column("wb_sales_1d", "nm_id",
                    schema="silver",
                    existing_type=sa.Integer(),
                    nullable=False)

    op.alter_column("wb_sales_1d", "supplier_article",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=False)

    op.alter_column("wb_sales_1d", "order_date",
                    schema="silver",
                    existing_type=sa.Date(),
                    nullable=False)

    op.alter_column("wb_sales_1d", "warehouse_name",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=False)

    op.alter_column("wb_sales_1d", "country",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=False)

    # Создаём новый составной PK
    op.create_primary_key(
        constraint_name="wb_sales_1d_pkey",
        table_name="wb_sales_1d",
        columns=[
            "request_uuid", "sr_id", "payment_reason",
            "nm_id", "supplier_article", "order_date", "warehouse_name", "country"
        ],
        schema="silver"
    )


def downgrade():
    # Удаляем расширенный PK
    op.drop_constraint(
        constraint_name="wb_sales_1d_pkey",
        table_name="wb_sales_1d",
        schema="silver",
        type_="primary"
    )

    # Делаем колонки снова nullable
    op.alter_column("wb_sales_1d", "nm_id",
                    schema="silver",
                    existing_type=sa.Integer(),
                    nullable=True)

    op.alter_column("wb_sales_1d", "supplier_article",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=True)

    op.alter_column("wb_sales_1d", "order_date",
                    schema="silver",
                    existing_type=sa.Date(),
                    nullable=True)

    op.alter_column("wb_sales_1d", "warehouse_name",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=True)

    op.alter_column("wb_sales_1d", "country",
                    schema="silver",
                    existing_type=sa.String(),
                    nullable=True)

    # Восстанавливаем старый PK
    op.create_primary_key(
        constraint_name="wb_sales_1d_pkey",
        table_name="wb_sales_1d",
        columns=["request_uuid", "sr_id", "payment_reason"],
        schema="silver"
    )