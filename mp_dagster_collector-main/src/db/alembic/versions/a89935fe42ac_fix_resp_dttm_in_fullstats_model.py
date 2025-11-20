"""fix resp dttm in fullstats model

Revision ID: a89935fe42ac
Revises: 3406189e3fa9
Create Date: 2025-08-19 13:58:46.958629

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a89935fe42ac'
down_revision: Union[str, None] = '3406189e3fa9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        UPDATE bronze.wb_adv_fullstats_1h
        SET response_dttm = COALESCE(response_dttm, receive_dttm, request_dttm, NOW())
        WHERE response_dttm IS NULL
    """)

    op.alter_column(
        "wb_adv_fullstats_1h",
        "response_dttm",
        schema="bronze",
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "wb_adv_fullstats_1h",
        "response_dttm",
        schema="bronze",
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=True,
    )
