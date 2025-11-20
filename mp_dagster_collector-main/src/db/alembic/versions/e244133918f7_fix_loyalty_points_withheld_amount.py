"""fix loyalty_points_withheld_amount

Revision ID: e244133918f7
Revises: 2e611cc247dd
Create Date: 2025-08-18 11:23:54.603461

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e244133918f7'
down_revision: Union[str, None] = '2e611cc247dd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.alter_column(
        "wb_sales_1d",
        "loyalty_points_withheld_amount",
        type_=sa.Numeric(14, 2),
        schema="silver",
        postgresql_using="loyalty_points_withheld_amount::numeric"
    )

def downgrade():
    op.alter_column(
        "wb_sales_1d",
        "loyalty_points_withheld_amount",
        type_=sa.String(),
        schema="silver",
        postgresql_using="loyalty_points_withheld_amount::text"
    )

