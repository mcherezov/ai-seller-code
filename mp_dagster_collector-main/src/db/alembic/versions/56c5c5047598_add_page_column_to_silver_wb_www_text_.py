"""add page column to silver_wb_www_text_search_1d

Revision ID: 56c5c5047598
Revises: 69a0d4c472b1
Create Date: 2025-08-07 17:12:57.984181

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = '56c5c5047598'
down_revision: Union[str, None] = '69a0d4c472b1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'wb_www_text_search_1d',
        sa.Column(
            'page',
            sa.Integer(),
            nullable=False,
            server_default=text('1')
        ),
        schema='silver',
    )


def downgrade() -> None:
    op.drop_column('wb_www_text_search_1d', 'page', schema='silver')
