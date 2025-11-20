"""add FK wb_ad_stats_keywords_1d → ad_keywords

Revision ID: 3d25c154f547
Revises: 5a7854cc849f
Create Date: 2025-06-24 16:32:35.296028

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '3d25c154f547'
down_revision: Union[str, None] = '5a7854cc849f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) Справочник кластеров
    op.create_table(
        'ad_keyword_clusters',
        sa.Column('keyword_cluster', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('keyword_cluster'),
        schema='silver',
    )

    # 2) Справочник ключевых слов
    op.create_table(
        'ad_keywords',
        sa.Column('keyword', sa.Text(), nullable=False),
        sa.Column('keyword_cluster', sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ['keyword_cluster'],
            ['silver.ad_keyword_clusters.keyword_cluster'],
        ),
        sa.PrimaryKeyConstraint('keyword'),
        schema='silver',
    )

    # 3) Суточные кластеры
    op.create_table(
        'wb_ad_keyword_clusters_1d',
        sa.Column('last_response_uuid', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('last_response_dttm', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('campaign_id', sa.Integer(), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('keyword_cluster', sa.Text(), nullable=False),
        sa.Column('cluster_views', sa.Integer(), nullable=True),
        sa.Column('is_excluded', sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(
            ['keyword_cluster'],
            ['silver.ad_keyword_clusters.keyword_cluster'],
        ),
        sa.PrimaryKeyConstraint('last_response_uuid', 'campaign_id', 'date', 'keyword_cluster'),
        schema='silver',
    )

    # 4) Добавляем новый FK на существующую таблицу wb_ad_stats_keywords_1d
    op.create_foreign_key(
        'fk_wb_ad_stats_keywords_1d_keyword_ad_keywords',
        'wb_ad_stats_keywords_1d', 'ad_keywords',
        ['keyword'], ['keyword'],
        source_schema='silver',
        referent_schema='silver',
    )

    # 5) Удаляем старую 5-минутную таблицу кластеров
    op.drop_table('wb_ad_keyword_clusters_5m', schema='silver')


def downgrade() -> None:
    # 1) Восстанавливаем старую 5-минутную таблицу кластеров
    op.create_table(
        'wb_ad_keyword_clusters_5m',
        sa.Column('response_uuid', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('response_dttm', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('campaign_id', sa.INTEGER(), nullable=False),
        sa.Column('keyword_cluster', sa.TEXT(), nullable=False),
        sa.Column('keyword_cluster_views', sa.INTEGER(), nullable=True),
        sa.Column('keyword', sa.TEXT(), nullable=False),
        sa.Column('is_excluded', sa.BOOLEAN(), nullable=True),
        sa.PrimaryKeyConstraint(
            'response_uuid', 'campaign_id', 'keyword_cluster', 'keyword',
            name=op.f('wb_ad_keyword_clusters_5m_pkey')
        ),
        schema='silver',
    )

    # 2) Удаляем FK из wb_ad_stats_keywords_1d
    op.drop_constraint(
        'fk_wb_ad_stats_keywords_1d_keyword_ad_keywords',
        'wb_ad_stats_keywords_1d',
        schema='silver',
        type_='foreignkey',
    )

    # 3) Удаляем суточные кластеры и справочники
    op.drop_table('wb_ad_keyword_clusters_1d', schema='silver')
    op.drop_table('ad_keywords',             schema='silver')
    op.drop_table('ad_keyword_clusters',     schema='silver')
