"""update silver.* tables: add company_id & inserted_at, update PK, drop wb_reward

Revision ID: 21fb6feb8486
Revises: 56c5c5047598
Create Date: 2025-08-08 20:18:18.055711

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '21fb6feb8486'
down_revision: Union[str, None] = '56c5c5047598'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade():
    # --- WbRetentions1d ---
    op.add_column('wb_retentions_1d', sa.Column('company_id', sa.Integer(), server_default=sa.text("99"), nullable=False), schema='silver')
    op.add_column('wb_retentions_1d', sa.Column('inserted_at', sa.DateTime(timezone=True), server_default=sa.text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"), nullable=False), schema='silver')
    op.drop_constraint('wb_retentions_1d_pkey', 'wb_retentions_1d', schema='silver', type_='primary')
    op.create_primary_key('wb_retentions_1d_pkey', 'wb_retentions_1d', ['company_id', 'request_uuid', 'retention_id', 'retentions_description'], schema='silver')
    op.drop_column('wb_retentions_1d', 'wb_reward', schema='silver')

    # --- WbCompensations1d ---
    op.add_column('wb_compensations_1d', sa.Column('company_id', sa.Integer(), server_default=sa.text("99"), nullable=False), schema='silver')
    op.add_column('wb_compensations_1d', sa.Column('inserted_at', sa.DateTime(timezone=True), server_default=sa.text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"), nullable=False), schema='silver')
    op.drop_constraint('wb_compensations_1d_pkey', 'wb_compensations_1d', schema='silver', type_='primary')
    op.create_primary_key('wb_compensations_1d_pkey', 'wb_compensations_1d', ['company_id', 'request_uuid', 'compensation_id', 'nm_id', 'supplier_article'], schema='silver')
    op.drop_column('wb_compensations_1d', 'wb_reward', schema='silver')

    # --- WbLogistics1d ---
    op.add_column('wb_logistics_1d', sa.Column('company_id', sa.Integer(), server_default=sa.text("99"), nullable=False), schema='silver')
    op.add_column('wb_logistics_1d', sa.Column('inserted_at', sa.DateTime(timezone=True), server_default=sa.text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"), nullable=False), schema='silver')
    op.drop_constraint('wb_logistics_1d_pkey', 'wb_logistics_1d', schema='silver', type_='primary')
    op.create_primary_key('wb_logistics_1d_pkey', 'wb_logistics_1d', ['company_id', 'request_uuid', 'sr_id', 'logistic_type', 'nm_id', 'supplier_article'], schema='silver')
    op.drop_column('wb_logistics_1d', 'wb_reward', schema='silver')

    # --- WbFines1d ---
    op.add_column('wb_fines_1d', sa.Column('company_id', sa.Integer(), server_default=sa.text("99"), nullable=False), schema='silver')
    op.add_column('wb_fines_1d', sa.Column('inserted_at', sa.DateTime(timezone=True), server_default=sa.text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"), nullable=False), schema='silver')
    op.drop_constraint('wb_fines_1d_pkey', 'wb_fines_1d', schema='silver', type_='primary')
    op.create_primary_key('wb_fines_1d_pkey', 'wb_fines_1d', ['company_id', 'request_uuid', 'fine_id', 'nm_id', 'supplier_article', 'fine_description'], schema='silver')
    op.drop_column('wb_fines_1d', 'wb_reward', schema='silver')

    # --- WbSales1d ---
    op.add_column('wb_sales_1d', sa.Column('company_id', sa.Integer(), server_default=sa.text("99"), nullable=False), schema='silver')
    op.add_column('wb_sales_1d', sa.Column('inserted_at', sa.DateTime(timezone=True), server_default=sa.text("TIMEZONE('Europe/Moscow', CURRENT_TIMESTAMP)"), nullable=False), schema='silver')
    op.drop_constraint('wb_sales_1d_pkey', 'wb_sales_1d', schema='silver', type_='primary')
    op.create_primary_key('wb_sales_1d_pkey', 'wb_sales_1d', ['company_id', 'request_uuid', 'sr_id', 'payment_reason', 'nm_id', 'supplier_article', 'order_date', 'warehouse_name', 'country'], schema='silver')
    op.drop_column('wb_sales_1d', 'wb_reward', schema='silver')
    op.drop_column('wb_sales_1d', 'logistic_type', schema='silver')


def downgrade():
    # --- WbRetentions1d ---
    op.add_column('wb_retentions_1d', sa.Column('wb_reward', sa.Numeric(precision=12, scale=2), nullable=False), schema='silver')
    op.drop_constraint('wb_retentions_1d_pkey', 'wb_retentions_1d', schema='silver', type_='primary')
    op.create_primary_key('wb_retentions_1d_pkey', 'wb_retentions_1d', ['request_uuid', 'retention_id', 'retentions_description', 'wb_reward'], schema='silver')
    op.drop_column('wb_retentions_1d', 'inserted_at', schema='silver')
    op.drop_column('wb_retentions_1d', 'company_id', schema='silver')

    # --- WbCompensations1d ---
    op.add_column('wb_compensations_1d', sa.Column('wb_reward', sa.Numeric(precision=12, scale=2), nullable=False), schema='silver')
    op.drop_constraint('wb_compensations_1d_pkey', 'wb_compensations_1d', schema='silver', type_='primary')
    op.create_primary_key('wb_compensations_1d_pkey', 'wb_compensations_1d', ['request_uuid', 'compensation_id', 'nm_id', 'supplier_article', 'logistic_type', 'wb_reward'], schema='silver')
    op.drop_column('wb_compensations_1d', 'inserted_at', schema='silver')
    op.drop_column('wb_compensations_1d', 'company_id', schema='silver')

    # --- WbLogistics1d ---
    op.add_column('wb_logistics_1d', sa.Column('wb_reward', sa.Numeric(precision=12, scale=2), nullable=False), schema='silver')
    op.drop_constraint('wb_logistics_1d_pkey', 'wb_logistics_1d', schema='silver', type_='primary')
    op.create_primary_key('wb_logistics_1d_pkey', 'wb_logistics_1d', ['request_uuid', 'sr_id', 'logistic_type', 'nm_id', 'supplier_article', 'wb_reward'], schema='silver')
    op.drop_column('wb_logistics_1d', 'inserted_at', schema='silver')
    op.drop_column('wb_logistics_1d', 'company_id', schema='silver')

    # --- WbFines1d ---
    op.add_column('wb_fines_1d', sa.Column('wb_reward', sa.Numeric(precision=12, scale=2), nullable=False), schema='silver')
    op.drop_constraint('wb_fines_1d_pkey', 'wb_fines_1d', schema='silver', type_='primary')
    op.create_primary_key('wb_fines_1d_pkey', 'wb_fines_1d', ['request_uuid', 'fine_id', 'nm_id', 'supplier_article', 'fine_description', 'wb_reward'], schema='silver')
    op.drop_column('wb_fines_1d', 'inserted_at', schema='silver')
    op.drop_column('wb_fines_1d', 'company_id', schema='silver')

    # --- WbSales1d ---
    op.add_column('wb_sales_1d', sa.Column('wb_reward', sa.Numeric(precision=12, scale=2), nullable=False), schema='silver')
    op.add_column('wb_sales_1d', sa.Column('logistic_type', sa.String(), nullable=True), schema='silver')
    op.drop_constraint('wb_sales_1d_pkey', 'wb_sales_1d', schema='silver', type_='primary')
    op.create_primary_key('wb_sales_1d_pkey', 'wb_sales_1d', ['request_uuid', 'sr_id', 'payment_reason', 'nm_id', 'supplier_article', 'order_date', 'warehouse_name', 'country', 'logistic_type', 'wb_reward'], schema='silver')
    op.drop_column('wb_sales_1d', 'inserted_at', schema='silver')
    op.drop_column('wb_sales_1d', 'company_id', schema='silver')
