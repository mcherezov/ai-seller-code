"""bronze.wb_adv_fullstats_1h: add run_schedule_dttm, business_dttm, receive_dttm

Revision ID: d53d5df417a1
Revises: 7f3770e5c380
Create Date: 2025-08-14 09:09:50.011865

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP

# revision identifiers, used by Alembic.
revision: str = 'd53d5df417a1'
down_revision: Union[str, None] = '7f3770e5c380'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 1) Добавляем новые колонки (история остаётся NULL)
    op.add_column(
        "wb_adv_fullstats_1h",
        sa.Column("run_schedule_dttm", PG_TIMESTAMP(timezone=True), nullable=True),
        schema="bronze",
    )
    op.add_column(
        "wb_adv_fullstats_1h",
        sa.Column("business_dttm", PG_TIMESTAMP(timezone=True), nullable=True),
        schema="bronze",
    )
    op.add_column(
        "wb_adv_fullstats_1h",
        sa.Column("receive_dttm", PG_TIMESTAMP(timezone=True), nullable=True),
        schema="bronze",
    )



def downgrade():
    # Удаляем добавленные колонки
    op.drop_column("wb_adv_fullstats_1h", "receive_dttm", schema="bronze")
    op.drop_column("wb_adv_fullstats_1h", "business_dttm", schema="bronze")
    op.drop_column("wb_adv_fullstats_1h", "run_schedule_dttm", schema="bronze")
