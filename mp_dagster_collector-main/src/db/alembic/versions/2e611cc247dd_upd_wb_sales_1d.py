"""upd wb_sales_1d

Revision ID: 2e611cc247dd
Revises: 144b2a3dddc6
Create Date: 2025-08-18 10:34:49.002418

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2e611cc247dd'
down_revision: Union[str, None] = '144b2a3dddc6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "wb_sales_1d",
        sa.Column("loyalty_points_withheld_amount", sa.String(), nullable=True),
        schema="silver",
    )

def downgrade() -> None:
    op.drop_column(
        "wb_sales_1d",
        "loyalty_points_withheld_amount",
        schema="silver",
    )
