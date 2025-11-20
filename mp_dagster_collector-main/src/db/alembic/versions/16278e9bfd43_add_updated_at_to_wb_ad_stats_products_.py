"""add updated_at to wb_ad_stats_products_1d, add bronze.wb_products_search_texts

Revision ID: 16278e9bfd43
Revises: ee41fa451114
Create Date: 2025-06-20 15:52:39.040178

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '16278e9bfd43'
down_revision: Union[str, None] = 'ee41fa451114'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # создаём новую таблицу в схеме bronze
    op.create_table(
        'wb_products_search_texts',
        sa.Column('id', sa.BigInteger(), sa.Sequence('wb_products_search_texts_id_seq', schema='bronze'),
                  primary_key=True, nullable=False),
        sa.Column('content', sa.Text(), nullable=False),
        sa.Column('batch_id', sa.UUID(), nullable=False),
        sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('token_id', sa.Integer(), nullable=True),
        sa.Column('response_uuid', sa.Text(), nullable=True),
        sa.Column('response_dttm', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['token_id'], ['core.tokens.token_id']),
        schema='bronze'
    )
    op.create_index(
        op.f('ix_bronze_wb_products_search_texts_id'),
        'wb_products_search_texts', ['id'],
        unique=False, schema='bronze'
    )
    op.create_index(
        op.f('ix_bronze_wb_products_search_texts_batch_id'),
        'wb_products_search_texts', ['batch_id'],
        unique=False, schema='bronze'
    )

    # добавляем поле updated_at в silver-таблицу
    op.add_column(
        'wb_ad_stats_products_1d',
        sa.Column(
            'updated_at',
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text('now()')
        ),
        schema='silver'
    )


def downgrade() -> None:
    """Downgrade schema."""
    # откатываем добавление поля
    op.drop_column('wb_ad_stats_products_1d', 'updated_at', schema='silver')

    # удаляем индексы и таблицу в bronze
    op.drop_index(op.f('ix_bronze_wb_products_search_texts_batch_id'),
                  table_name='wb_products_search_texts', schema='bronze')
    op.drop_index(op.f('ix_bronze_wb_products_search_texts_id'),
                  table_name='wb_products_search_texts', schema='bronze')
    op.drop_table('wb_products_search_texts', schema='bronze')
