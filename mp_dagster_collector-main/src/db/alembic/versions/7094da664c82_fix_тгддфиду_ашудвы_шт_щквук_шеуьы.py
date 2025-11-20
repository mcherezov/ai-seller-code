"""fix тгддфиду ашудвы шт щквук_шеуьы

Revision ID: 7094da664c82
Revises: a89935fe42ac
Create Date: 2025-08-19 14:13:29.695056

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7094da664c82'
down_revision: Union[str, None] = 'a89935fe42ac'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        UPDATE silver.wb_order_items_1d SET
            warehouse_type       = COALESCE(warehouse_type, ''),
            country_name         = COALESCE(country_name, ''),
            oblast_okrug_name    = COALESCE(oblast_okrug_name, ''),
            region_name          = COALESCE(region_name, ''),
            supplier_article     = COALESCE(supplier_article, ''),
            barcode              = COALESCE(barcode, ''),
            category             = COALESCE(category, ''),
            subject              = COALESCE(subject, ''),
            brand                = COALESCE(brand, ''),
            tech_size            = COALESCE(tech_size, ''),
            income_id            = COALESCE(income_id, 0),
            total_price          = COALESCE(total_price, 0),
            discount_percent     = COALESCE(discount_percent, 0),
            spp                  = COALESCE(spp, 0),
            finished_price       = COALESCE(finished_price, 0),
            price_with_discount  = COALESCE(price_with_discount, 0),
            sticker              = COALESCE(sticker, '')
    """)

    op.alter_column("wb_order_items_1d", "warehouse_type",
                    schema="silver", existing_type=sa.String(), nullable=False)
    op.alter_column("wb_order_items_1d", "country_name",
                    schema="silver", existing_type=sa.String(), nullable=False)
    op.alter_column("wb_order_items_1d", "oblast_okrug_name",
                    schema="silver", existing_type=sa.String(), nullable=False)
    op.alter_column("wb_order_items_1d", "region_name",
                    schema="silver", existing_type=sa.String(), nullable=False)
    op.alter_column("wb_order_items_1d", "supplier_article",
                    schema="silver", existing_type=sa.String(), nullable=False)
    op.alter_column("wb_order_items_1d", "barcode",
                    schema="silver", existing_type=sa.String(), nullable=False)
    op.alter_column("wb_order_items_1d", "category",
                    schema="silver", existing_type=sa.String(), nullable=False)
    op.alter_column("wb_order_items_1d", "subject",
                    schema="silver", existing_type=sa.String(), nullable=False)
    op.alter_column("wb_order_items_1d", "brand",
                    schema="silver", existing_type=sa.String(), nullable=False)
    op.alter_column("wb_order_items_1d", "tech_size",
                    schema="silver", existing_type=sa.String(), nullable=False)
    op.alter_column("wb_order_items_1d", "income_id",
                    schema="silver", existing_type=sa.Integer(), nullable=False)
    op.alter_column("wb_order_items_1d", "total_price",
                    schema="silver", existing_type=sa.Float(), nullable=False)
    op.alter_column("wb_order_items_1d", "discount_percent",
                    schema="silver", existing_type=sa.Float(), nullable=False)
    op.alter_column("wb_order_items_1d", "spp",
                    schema="silver", existing_type=sa.Integer(), nullable=False)
    op.alter_column("wb_order_items_1d", "finished_price",
                    schema="silver", existing_type=sa.Float(), nullable=False)
    op.alter_column("wb_order_items_1d", "price_with_discount",
                    schema="silver", existing_type=sa.Float(), nullable=False)
    op.alter_column("wb_order_items_1d", "sticker",
                    schema="silver", existing_type=sa.String(), nullable=False)


def downgrade() -> None:
    op.alter_column("wb_order_items_1d", "sticker",
                    schema="silver", existing_type=sa.String(), nullable=True)
    op.alter_column("wb_order_items_1d", "price_with_discount",
                    schema="silver", existing_type=sa.Float(), nullable=True)
    op.alter_column("wb_order_items_1d", "finished_price",
                    schema="silver", existing_type=sa.Float(), nullable=True)
    op.alter_column("wb_order_items_1d", "spp",
                    schema="silver", existing_type=sa.Integer(), nullable=True)
    op.alter_column("wb_order_items_1d", "discount_percent",
                    schema="silver", existing_type=sa.Float(), nullable=True)
    op.alter_column("wb_order_items_1d", "total_price",
                    schema="silver", existing_type=sa.Float(), nullable=True)
    op.alter_column("wb_order_items_1d", "income_id",
                    schema="silver", existing_type=sa.Integer(), nullable=True)
    op.alter_column("wb_order_items_1d", "tech_size",
                    schema="silver", existing_type=sa.String(), nullable=True)
    op.alter_column("wb_order_items_1d", "brand",
                    schema="silver", existing_type=sa.String(), nullable=True)
    op.alter_column("wb_order_items_1d", "subject",
                    schema="silver", existing_type=sa.String(), nullable=True)
    op.alter_column("wb_order_items_1d", "category",
                    schema="silver", existing_type=sa.String(), nullable=True)
    op.alter_column("wb_order_items_1d", "barcode",
                    schema="silver", existing_type=sa.String(), nullable=True)
    op.alter_column("wb_order_items_1d", "supplier_article",
                    schema="silver", existing_type=sa.String(), nullable=True)
    op.alter_column("wb_order_items_1d", "region_name",
                    schema="silver", existing_type=sa.String(), nullable=True)
    op.alter_column("wb_order_items_1d", "oblast_okrug_name",
                    schema="silver", existing_type=sa.String(), nullable=True)
    op.alter_column("wb_order_items_1d", "country_name",
                    schema="silver", existing_type=sa.String(), nullable=True)
    op.alter_column("wb_order_items_1d", "warehouse_type",
                    schema="silver", existing_type=sa.String(), nullable=True)