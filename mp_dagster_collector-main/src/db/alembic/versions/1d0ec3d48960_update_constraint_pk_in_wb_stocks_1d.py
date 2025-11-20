"""Update constraint PK in wb_stocks_1d

Revision ID: 1d0ec3d48960
Revises: 749b1c09fabc
Create Date: 2025-07-05 02:31:39.054588

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1d0ec3d48960'
down_revision: Union[str, None] = '749b1c09fabc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Имя таблицы и схемы
TABLE_NAME = "wb_stocks_1d"
SCHEMA = "silver"

# Имена ограничений
OLD_PK_NAME = "pk_wb_stocks_1d"
OLD_UQ_NAME = "uq_wb_stocks_1d_natural"
NEW_PK_NAME = "pk_wb_stocks_1d"


def upgrade():
    # Удаляем старый PK и уникальное ограничение
    op.drop_constraint(OLD_PK_NAME, TABLE_NAME, type_="primary", schema=SCHEMA)
    op.drop_constraint(OLD_UQ_NAME, TABLE_NAME, type_="unique", schema=SCHEMA)

    # Создаём новый составной PK
    op.create_primary_key(
        NEW_PK_NAME,
        TABLE_NAME,
        [
            "request_uuid",
            "response_dttm",
            "company_id",
            "date",
            "barcode",
            "warehouse_name",
        ],
        schema=SCHEMA,
    )


def downgrade():
    # Удаляем новый PK
    op.drop_constraint(NEW_PK_NAME, TABLE_NAME, type_="primary", schema=SCHEMA)

    # Восстанавливаем старые ограничения
    op.create_primary_key(
        OLD_PK_NAME,
        TABLE_NAME,
        ["request_uuid", "response_dttm", "company_id"],
        schema=SCHEMA,
    )

    op.create_unique_constraint(
        OLD_UQ_NAME,
        TABLE_NAME,
        ["date", "barcode", "warehouse_name"],
        schema=SCHEMA,
    )