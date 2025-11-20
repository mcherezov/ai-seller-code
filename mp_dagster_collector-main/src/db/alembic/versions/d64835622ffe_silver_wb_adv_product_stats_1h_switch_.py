"""silver.wb_adv_product_stats_1h: switch PK to (business_dttm, advert_id, app_type, nm_id)

Revision ID: d64835622ffe
Revises: b9195091e94d
Create Date: 2025-08-14 15:49:32.736717

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd64835622ffe'
down_revision: Union[str, None] = 'b9195091e94d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 1) гарантируем NOT NULL на business_dttm (бэкофилл уже сделан)
    op.alter_column(
        "wb_adv_product_stats_1h",
        "business_dttm",
        schema="silver",
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=False,
    )

    # 2) создаём уникальный индекс на новый ключ (CONCURRENTLY → нужен autocommit)
    with op.get_context().autocommit_block():
        op.execute("""
            CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS
                ux_wb_adv_product_stats_1h_biz_ad_app_nm
            ON silver.wb_adv_product_stats_1h (business_dttm, advert_id, app_type, nm_id)
        """)

    # 3) снимаем старый PK и вешаем новый, используя готовый индекс
    op.drop_constraint(
        "wb_adv_product_stats_1h_pkey",
        "wb_adv_product_stats_1h",
        schema="silver",
        type_="primary",
    )
    op.execute("""
        ALTER TABLE silver.wb_adv_product_stats_1h
        ADD CONSTRAINT wb_adv_product_stats_1h_pkey
        PRIMARY KEY USING INDEX ux_wb_adv_product_stats_1h_biz_ad_app_nm
    """)

    # 4) при необходимости — индексы под запросы по request_uuid/request_dttm
    with op.get_context().autocommit_block():
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
              ix_wb_adv_product_stats_1h_request_uuid
            ON silver.wb_adv_product_stats_1h (request_uuid)
        """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
              ix_wb_adv_product_stats_1h_request_dttm
            ON silver.wb_adv_product_stats_1h (request_dttm)
        """)


def downgrade():
    # снимаем новый PK
    op.drop_constraint(
        "wb_adv_product_stats_1h_pkey",
        "wb_adv_product_stats_1h",
        schema="silver",
        type_="primary",
    )

    # возвращаем старый PK
    op.execute("""
        ALTER TABLE silver.wb_adv_product_stats_1h
        ADD CONSTRAINT wb_adv_product_stats_1h_pkey
        PRIMARY KEY (request_uuid, advert_id, request_dttm, app_type, nm_id)
    """)

    # убираем созданные индексы
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS silver.ux_wb_adv_product_stats_1h_biz_ad_app_nm")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS silver.ix_wb_adv_product_stats_1h_request_uuid")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS silver.ix_wb_adv_product_stats_1h_request_dttm")

    # (опционально) вернуть business_dttm в nullable=True
    op.alter_column(
        "wb_adv_product_stats_1h",
        "business_dttm",
        schema="silver",
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=True,
    )