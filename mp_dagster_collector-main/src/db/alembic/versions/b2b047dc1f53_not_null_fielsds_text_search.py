"""not null fielsds text_search

Revision ID: b2b047dc1f53
Revises: b3392eca7de7
Create Date: 2025-09-01 13:56:42.676799

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b2b047dc1f53'
down_revision: Union[str, None] = 'b3392eca7de7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Важно: миграция упадет, если в колонках есть NULL-ы.
    with op.batch_alter_table("wb_www_text_search_1d", schema="silver") as batch:
        batch.alter_column("nm_position",               existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("app_type",                  existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("lang",                      existing_type=sa.String(),       nullable=False)
        batch.alter_column("cur",                       existing_type=sa.String(),       nullable=False)
        batch.alter_column("resultset",                 existing_type=sa.String(),       nullable=False)

        batch.alter_column("catalog_type",              existing_type=sa.String(),       nullable=False)
        batch.alter_column("catalog_value",             existing_type=sa.String(),       nullable=False)
        batch.alter_column("search_result_name",        existing_type=sa.String(),       nullable=False)
        batch.alter_column("search_result_rmi",         existing_type=sa.String(),       nullable=False)
        batch.alter_column("search_result_title",       existing_type=sa.String(),       nullable=False)
        batch.alter_column("search_result_rs",          existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("search_result_qv",          existing_type=sa.String(),       nullable=False)

        batch.alter_column("nm_id",                     existing_type=sa.BigInteger(),   nullable=False)  # (product_id)

        batch.alter_column("product_time_1",            existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_time_2",            existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_wh",                existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_d_type",            existing_type=sa.BigInteger(),   nullable=False)
        batch.alter_column("product_dist",              existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_root",              existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_kind_id",           existing_type=sa.Integer(),      nullable=False)

        batch.alter_column("product_brand",             existing_type=sa.String(),       nullable=False)
        batch.alter_column("product_brand_id",          existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_site_brand_id",     existing_type=sa.Integer(),      nullable=False)

        batch.alter_column("product_subject_id",        existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_subject_parent_id", existing_type=sa.Integer(),      nullable=False)

        batch.alter_column("product_name",              existing_type=sa.String(),       nullable=False)
        batch.alter_column("product_entity",            existing_type=sa.String(),       nullable=False)
        batch.alter_column("product_match_id",          existing_type=sa.Integer(),      nullable=False)

        batch.alter_column("product_supplier",          existing_type=sa.String(),       nullable=False)
        batch.alter_column("product_supplier_id",       existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_supplier_rating",   existing_type=sa.Float(),        nullable=False)
        batch.alter_column("product_supplier_flags",    existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_pics",              existing_type=sa.Integer(),      nullable=False)

        batch.alter_column("product_rating",            existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_review_rating",     existing_type=sa.Float(),        nullable=False)
        batch.alter_column("product_nm_review_rating",  existing_type=sa.Float(),        nullable=False)
        batch.alter_column("product_feedbacks",         existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_nm_feedbacks",      existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_volume",            existing_type=sa.Integer(),      nullable=False)
        batch.alter_column("product_view_flags",        existing_type=sa.Integer(),      nullable=False)

        batch.alter_column("product_sizes_name",            existing_type=sa.String(),     nullable=False)
        batch.alter_column("product_sizes_orig_name",       existing_type=sa.String(),     nullable=False)
        batch.alter_column("product_sizes_rank",            existing_type=sa.Integer(),    nullable=False)
        batch.alter_column("product_sizes_option_id",       existing_type=sa.Integer(),    nullable=False)
        batch.alter_column("product_sizes_wh",              existing_type=sa.Integer(),    nullable=False)
        batch.alter_column("product_sizes_time_1",          existing_type=sa.Integer(),    nullable=False)
        batch.alter_column("product_sizes_time_2",          existing_type=sa.Integer(),    nullable=False)
        batch.alter_column("product_sizes_d_type",          existing_type=sa.BigInteger(), nullable=False)
        batch.alter_column("product_sizes_price_basic",     existing_type=sa.Integer(),    nullable=False)
        batch.alter_column("product_sizes_price_product",   existing_type=sa.Integer(),    nullable=False)
        batch.alter_column("product_sizes_price_logistics", existing_type=sa.Integer(),    nullable=False)
        batch.alter_column("product_sizes_price_return",    existing_type=sa.Integer(),    nullable=False)
        batch.alter_column("product_sizes_sale_conditions", existing_type=sa.Integer(),    nullable=False)
        batch.alter_column("product_sizes_payload",         existing_type=sa.Text(),       nullable=False)

        batch.alter_column("product_meta_tokens",       existing_type=sa.String(),       nullable=False)
        batch.alter_column("product_meta_preset_id",    existing_type=sa.Integer(),      nullable=False)


def downgrade() -> None:
    with op.batch_alter_table("wb_www_text_search_1d", schema="silver") as batch:
        batch.alter_column("nm_position",               existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("app_type",                  existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("lang",                      existing_type=sa.String(),       nullable=True)
        batch.alter_column("cur",                       existing_type=sa.String(),       nullable=True)
        batch.alter_column("resultset",                 existing_type=sa.String(),       nullable=True)

        batch.alter_column("catalog_type",              existing_type=sa.String(),       nullable=True)
        batch.alter_column("catalog_value",             existing_type=sa.String(),       nullable=True)
        batch.alter_column("search_result_name",        existing_type=sa.String(),       nullable=True)
        batch.alter_column("search_result_rmi",         existing_type=sa.String(),       nullable=True)
        batch.alter_column("search_result_title",       existing_type=sa.String(),       nullable=True)
        batch.alter_column("search_result_rs",          existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("search_result_qv",          existing_type=sa.String(),       nullable=True)

        batch.alter_column("nm_id",                     existing_type=sa.BigInteger(),   nullable=True)

        batch.alter_column("product_time_1",            existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_time_2",            existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_wh",                existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_d_type",            existing_type=sa.BigInteger(),   nullable=True)
        batch.alter_column("product_dist",              existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_root",              existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_kind_id",           existing_type=sa.Integer(),      nullable=True)

        batch.alter_column("product_brand",             existing_type=sa.String(),       nullable=True)
        batch.alter_column("product_brand_id",          existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_site_brand_id",     existing_type=sa.Integer(),      nullable=True)

        batch.alter_column("product_subject_id",        existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_subject_parent_id", existing_type=sa.Integer(),      nullable=True)

        batch.alter_column("product_name",              existing_type=sa.String(),       nullable=True)
        batch.alter_column("product_entity",            existing_type=sa.String(),       nullable=True)
        batch.alter_column("product_match_id",          existing_type=sa.Integer(),      nullable=True)

        batch.alter_column("product_supplier",          existing_type=sa.String(),       nullable=True)
        batch.alter_column("product_supplier_id",       existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_supplier_rating",   existing_type=sa.Float(),        nullable=True)
        batch.alter_column("product_supplier_flags",    existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_pics",              existing_type=sa.Integer(),      nullable=True)

        batch.alter_column("product_rating",            existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_review_rating",     existing_type=sa.Float(),        nullable=True)
        batch.alter_column("product_nm_review_rating",  existing_type=sa.Float(),        nullable=True)
        batch.alter_column("product_feedbacks",         existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_nm_feedbacks",      existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_volume",            existing_type=sa.Integer(),      nullable=True)
        batch.alter_column("product_view_flags",        existing_type=sa.Integer(),      nullable=True)

        batch.alter_column("product_sizes_name",            existing_type=sa.String(),     nullable=True)
        batch.alter_column("product_sizes_orig_name",       existing_type=sa.String(),     nullable=True)
        batch.alter_column("product_sizes_rank",            existing_type=sa.Integer(),    nullable=True)
        batch.alter_column("product_sizes_option_id",       existing_type=sa.Integer(),    nullable=True)
        batch.alter_column("product_sizes_wh",              existing_type=sa.Integer(),    nullable=True)
        batch.alter_column("product_sizes_time_1",          existing_type=sa.Integer(),    nullable=True)
        batch.alter_column("product_sizes_time_2",          existing_type=sa.Integer(),    nullable=True)
        batch.alter_column("product_sizes_d_type",          existing_type=sa.BigInteger(), nullable=True)
        batch.alter_column("product_sizes_price_basic",     existing_type=sa.Integer(),    nullable=True)
        batch.alter_column("product_sizes_price_product",   existing_type=sa.Integer(),    nullable=True)
        batch.alter_column("product_sizes_price_logistics", existing_type=sa.Integer(),    nullable=True)
        batch.alter_column("product_sizes_price_return",    existing_type=sa.Integer(),    nullable=True)
        batch.alter_column("product_sizes_sale_conditions", existing_type=sa.Integer(),    nullable=True)
        batch.alter_column("product_sizes_payload",         existing_type=sa.Text(),       nullable=True)

        batch.alter_column("product_meta_tokens",       existing_type=sa.String(),       nullable=True)
        batch.alter_column("product_meta_preset_id",    existing_type=sa.Integer(),      nullable=True)