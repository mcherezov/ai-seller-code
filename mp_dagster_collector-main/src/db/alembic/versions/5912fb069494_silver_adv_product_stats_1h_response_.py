"""silver(adv_product_stats_1h): response_dttm NOT NULL, backfill = business_dttm + 1 day

Revision ID: 5912fb069494
Revises: 154bea39b5ca
Create Date: 2025-08-28 08:34:12.715031

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5912fb069494'
down_revision: Union[str, None] = '154bea39b5ca'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SCHEMA = "silver"
TABLE  = "wb_adv_product_stats_1h"
COL    = "response_dttm"


def upgrade() -> None:
    # 1) Бэкфилл NULL → business_dttm + 1 day
    op.execute(
        f"""
        UPDATE "{SCHEMA}"."{TABLE}"
           SET "{COL}" = "business_dttm" + INTERVAL '1 day'
         WHERE "{COL}" IS NULL
        """
    )

    # 2) Делаем NOT NULL
    op.alter_column(
        TABLE, COL,
        schema=SCHEMA,
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
    )


def downgrade() -> None:
    # Возвращаем nullable
    op.alter_column(
        TABLE, COL,
        schema=SCHEMA,
        existing_type=sa.DateTime(timezone=True),
        nullable=True,
    )