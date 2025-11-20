"""Make request_uuid NOT NULL in silver.wb_order_items_1d

Revision ID: b051277ded8a
Revises: ae38d4255c20
Create Date: 2025-08-22 11:11:15.296195

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b051277ded8a'
down_revision: Union[str, None] = 'ae38d4255c20'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.alter_column(
        "wb_order_items_1d",
        "request_uuid",
        schema="silver",
        existing_type=postgresql.UUID(),
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "wb_order_items_1d",
        "request_uuid",
        schema="silver",
        existing_type=postgresql.UUID(),
        nullable=True,
    )