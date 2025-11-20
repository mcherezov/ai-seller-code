"""silver(wb_sales_funnels_1d): supplier_article NOT NULL

Revision ID: 7d50377e2c3e
Revises: 478f5b54c6f6
Create Date: 2025-08-29 15:07:23.300718

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7d50377e2c3e'
down_revision: Union[str, None] = '478f5b54c6f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



SILVER = "silver"
TBL = "wb_sales_funnels_1d"

def upgrade() -> None:
    op.execute(f'''
        UPDATE "{SILVER}"."{TBL}"
           SET supplier_article = ''
         WHERE supplier_article IS NULL
    ''')

    # сделать колонку NOT NULL
    op.alter_column(
        TBL,
        "supplier_article",
        schema=SILVER,
        existing_type=sa.String(),
        nullable=False,
    )

def downgrade() -> None:
    op.alter_column(
        TBL,
        "supplier_article",
        schema=SILVER,
        existing_type=sa.String(),
        nullable=True,
    )
