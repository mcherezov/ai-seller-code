"""upd pk wb_www_text_search_1d

Revision ID: 4e52cf06fabf
Revises: b79ac88a5a12
Create Date: 2025-07-28 10:19:31.913642

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4e52cf06fabf'
down_revision: Union[str, None] = 'b79ac88a5a12'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) Drop the existing composite PK (request_uuid, keyword, date)
    op.drop_constraint(
        constraint_name='wb_www_text_search_1d_pkey',
        table_name='wb_www_text_search_1d',
        schema='silver',
        type_='primary',
    )
    # 2) Create a new PK including product_id
    op.create_primary_key(
        constraint_name='wb_www_text_search_1d_pkey',
        table_name='wb_www_text_search_1d',
        columns=['request_uuid', 'keyword', 'date', 'product_id'],
        schema='silver',
    )


def downgrade() -> None:
    # 1) Drop the augmented PK
    op.drop_constraint(
        constraint_name='wb_www_text_search_1d_pkey',
        table_name='wb_www_text_search_1d',
        schema='silver',
        type_='primary',
    )
    # 2) Restore the previous PK on (request_uuid, keyword, date)
    op.create_primary_key(
        constraint_name='wb_www_text_search_1d_pkey',
        table_name='wb_www_text_search_1d',
        columns=['request_uuid', 'keyword', 'date'],
        schema='silver',
    )