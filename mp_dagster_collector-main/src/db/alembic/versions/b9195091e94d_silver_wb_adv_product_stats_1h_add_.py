"""silver.wb_adv_product_stats_1h: add response_dttm, business_dttm

Revision ID: b9195091e94d
Revises: d53d5df417a1
Create Date: 2025-08-14 09:55:40.808511

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP

# revision identifiers, used by Alembic.
revision: str = 'b9195091e94d'
down_revision: Union[str, None] = 'd53d5df417a1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.add_column(
        "wb_adv_product_stats_1h",
        sa.Column("response_dttm", PG_TIMESTAMP(timezone=True), nullable=True),
        schema="silver",
    )
    op.add_column(
        "wb_adv_product_stats_1h",
        sa.Column("business_dttm", PG_TIMESTAMP(timezone=True), nullable=True),
        schema="silver",
    )


def downgrade():
    op.drop_column("wb_adv_product_stats_1h", "business_dttm", schema="silver")
    op.drop_column("wb_adv_product_stats_1h", "response_dttm", schema="silver")