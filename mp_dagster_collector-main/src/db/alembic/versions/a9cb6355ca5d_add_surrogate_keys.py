"""Add surrogate keys

Revision ID: a9cb6355ca5d
Revises: db70043980c1
Create Date: 2025-06-19 03:56:10.352137

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a9cb6355ca5d'
down_revision: Union[str, None] = 'db70043980c1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ─── AUTO ─────────────────────────────────────────────────────
    op.drop_constraint(
        'wb_ad_campaigns_auto_1d_pkey',
        'wb_ad_campaigns_auto_1d',
        schema='silver',
        type_='primary'
    )
    op.add_column(
        'wb_ad_campaigns_auto_1d',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        schema='silver'
    )
    op.create_primary_key(
        'pk_wb_ad_campaigns_auto_1d',
        'wb_ad_campaigns_auto_1d',
        ['id'],
        schema='silver'
    )

    # ─── PRODUCTS ────────────────────────────────────────────────
    op.drop_constraint(
        'wb_ad_campaigns_products_1d_pkey',
        'wb_ad_campaigns_products_1d',
        schema='silver',
        type_='primary'
    )
    op.add_column(
        'wb_ad_campaigns_products_1d',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        schema='silver'
    )
    op.create_primary_key(
        'pk_wb_ad_campaigns_products_1d',
        'wb_ad_campaigns_products_1d',
        ['id'],
        schema='silver'
    )

    # ─── UNITED ─────────────────────────────────────────────────
    op.drop_constraint(
        'wb_ad_campaigns_united_1d_pkey',
        'wb_ad_campaigns_united_1d',
        schema='silver',
        type_='primary'
    )
    op.add_column(
        'wb_ad_campaigns_united_1d',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        schema='silver'
    )
    op.create_primary_key(
        'pk_wb_ad_campaigns_united_1d',
        'wb_ad_campaigns_united_1d',
        ['id'],
        schema='silver'
    )


def downgrade() -> None:
    """Downgrade schema."""
    # ─── AUTO ─────────────────────────────────────────────────────
    op.drop_constraint(
        'pk_wb_ad_campaigns_auto_1d',
        'wb_ad_campaigns_auto_1d',
        schema='silver',
        type_='primary'
    )
    op.drop_column('wb_ad_campaigns_auto_1d', 'id', schema='silver')
    op.create_primary_key(
        'wb_ad_campaigns_auto_1d_pkey',
        'wb_ad_campaigns_auto_1d',
        ['response_uuid', 'campaign_id', 'product_id'],
        schema='silver'
    )

    # ─── PRODUCTS ────────────────────────────────────────────────
    op.drop_constraint(
        'pk_wb_ad_campaigns_products_1d',
        'wb_ad_campaigns_products_1d',
        schema='silver',
        type_='primary'
    )
    op.drop_column('wb_ad_campaigns_products_1d', 'id', schema='silver')
    op.create_primary_key(
        'wb_ad_campaigns_products_1d_pkey',
        'wb_ad_campaigns_products_1d',
        ['response_uuid', 'campaign_id', 'product_id'],
        schema='silver'
    )

    # ─── UNITED ─────────────────────────────────────────────────
    op.drop_constraint(
        'pk_wb_ad_campaigns_united_1d',
        'wb_ad_campaigns_united_1d',
        schema='silver',
        type_='primary'
    )
    op.drop_column('wb_ad_campaigns_united_1d', 'id', schema='silver')
    op.create_primary_key(
        'wb_ad_campaigns_united_1d_pkey',
        'wb_ad_campaigns_united_1d',
        ['response_uuid', 'campaign_id', 'product_id'],
        schema='silver'
    )
