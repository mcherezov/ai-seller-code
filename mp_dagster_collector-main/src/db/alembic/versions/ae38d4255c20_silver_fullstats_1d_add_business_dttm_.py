"""silver(fullstats_1d): add business_dttm/response_dttm, backfill and change PK

Revision ID: ae38d4255c20
Revises: e551502ee06f
Create Date: 2025-08-22 10:00:51.622332

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ae38d4255c20'
down_revision: Union[str, None] = 'e551502ee06f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SILVER = "silver"
BRONZE = "bronze"

T_STATS = "wb_adv_product_stats_1d"
T_POS   = "wb_adv_product_positions_1d"

def upgrade():
    # 1) Добавляем новые поля (пока NULLABLE)
    op.add_column(T_STATS, sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=True), schema=SILVER)
    op.add_column(T_STATS, sa.Column("response_dttm", sa.TIMESTAMP(timezone=True), nullable=True), schema=SILVER)

    op.add_column(T_POS,   sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=True), schema=SILVER)
    op.add_column(T_POS,   sa.Column("response_dttm", sa.TIMESTAMP(timezone=True), nullable=True), schema=SILVER)

    # 2) Бэкфилл business_dttm
    op.execute(f"""
        UPDATE {SILVER}.{T_STATS}
           SET business_dttm = request_dttm
         WHERE business_dttm IS NULL;
    """)
    op.execute(f"""
        UPDATE {SILVER}.{T_POS}
           SET business_dttm = "date"
         WHERE business_dttm IS NULL;
    """)

    # 3) Бэкфилл response_dttm из bronze по request_uuid (+ фолбэк)
    op.execute(f"""
        UPDATE {SILVER}.{T_STATS} s
           SET response_dttm = b.response_dttm
          FROM {BRONZE}.wb_adv_fullstats_1d b
         WHERE s.request_uuid = b.request_uuid
           AND s.response_dttm IS NULL;
    """)
    op.execute(f"""
        UPDATE {SILVER}.{T_POS} s
           SET response_dttm = b.response_dttm
          FROM {BRONZE}.wb_adv_fullstats_1d b
         WHERE s.request_uuid = b.request_uuid
           AND s.response_dttm IS NULL;
    """)
    op.execute(f"""
        UPDATE {SILVER}.{T_STATS}
           SET response_dttm = COALESCE(response_dttm, business_dttm);
    """)
    op.execute(f"""
        UPDATE {SILVER}.{T_POS}
           SET response_dttm = COALESCE(response_dttm, business_dttm);
    """)

    # 4) NOT NULL
    op.alter_column(T_STATS, "business_dttm", schema=SILVER, nullable=False,
                    existing_type=sa.TIMESTAMP(timezone=True))
    op.alter_column(T_STATS, "response_dttm", schema=SILVER, nullable=False,
                    existing_type=sa.TIMESTAMP(timezone=True))
    op.alter_column(T_POS, "business_dttm", schema=SILVER, nullable=False,
                    existing_type=sa.TIMESTAMP(timezone=True))
    op.alter_column(T_POS, "response_dttm", schema=SILVER, nullable=False,
                    existing_type=sa.TIMESTAMP(timezone=True))

    # 5) ДЕДУПЛИКАЦИЯ перед сменой PK
    #    Оставляем запись с max(response_dttm), затем max(inserted_at), затем max(request_dttm), затем max(request_uuid)
    op.execute(f"""
        WITH ranked AS (
            SELECT ctid,
                   ROW_NUMBER() OVER (
                       PARTITION BY business_dttm, advert_id, app_type, nm_id
                       ORDER BY response_dttm DESC, inserted_at DESC, request_dttm DESC, request_uuid DESC
                   ) AS rn
            FROM {SILVER}.{T_STATS}
        )
        DELETE FROM {SILVER}.{T_STATS} t
        USING ranked r
        WHERE t.ctid = r.ctid AND r.rn > 1;
    """)
    op.execute(f"""
        WITH ranked AS (
            SELECT ctid,
                   ROW_NUMBER() OVER (
                       PARTITION BY business_dttm, advert_id, nm_id
                       ORDER BY response_dttm DESC, inserted_at DESC, "date" DESC, request_uuid DESC
                   ) AS rn
            FROM {SILVER}.{T_POS}
        )
        DELETE FROM {SILVER}.{T_POS} t
        USING ranked r
        WHERE t.ctid = r.ctid AND r.rn > 1;
    """)

    # 6) Меняем PK
    op.drop_constraint("wb_adv_product_stats_1d_pkey", T_STATS, type_="primary", schema=SILVER)
    op.create_primary_key(
        "wb_adv_product_stats_1d_pkey",
        T_STATS,
        ["business_dttm", "advert_id", "app_type", "nm_id"],
        schema=SILVER,
    )

    op.drop_constraint("wb_adv_product_positions_1d_pkey", T_POS, type_="primary", schema=SILVER)
    op.create_primary_key(
        "wb_adv_product_positions_1d_pkey",
        T_POS,
        ["business_dttm", "advert_id", "nm_id"],
        schema=SILVER,
    )



def downgrade():
    # откат PK к старым
    op.drop_constraint("wb_adv_product_positions_1d_pkey", T_POS, type_="primary", schema=SILVER)
    op.create_primary_key(
        "wb_adv_product_positions_1d_pkey",
        T_POS,
        ["request_uuid", "advert_id", "nm_id", "date"],
        schema=SILVER,
    )

    op.drop_constraint("wb_adv_product_stats_1d_pkey", T_STATS, type_="primary", schema=SILVER)
    op.create_primary_key(
        "wb_adv_product_stats_1d_pkey",
        T_STATS,
        ["request_uuid", "advert_id", "request_dttm", "app_type", "nm_id"],
        schema=SILVER,
    )

    # Снимаем NOT NULL
    op.alter_column(T_POS, "response_dttm", schema=SILVER, nullable=True,
                    existing_type=sa.TIMESTAMP(timezone=True))
    op.alter_column(T_POS, "business_dttm", schema=SILVER, nullable=True,
                    existing_type=sa.TIMESTAMP(timezone=True))
    op.alter_column(T_STATS, "response_dttm", schema=SILVER, nullable=True,
                    existing_type=sa.TIMESTAMP(timezone=True))
    op.alter_column(T_STATS, "business_dttm", schema=SILVER, nullable=True,
                    existing_type=sa.TIMESTAMP(timezone=True))

    # Удаляем добавленные колонки
    op.drop_column(T_POS, "response_dttm", schema=SILVER)
    op.drop_column(T_POS, "business_dttm", schema=SILVER)
    op.drop_column(T_STATS, "response_dttm", schema=SILVER)
    op.drop_column(T_STATS, "business_dttm", schema=SILVER)
