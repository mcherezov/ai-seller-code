"""bronze(www_fin_reports_1d): drop company_id, add api_token_id (FK tokens), truncate table

Revision ID: b777fea1d803
Revises: b051277ded8a
Create Date: 2025-08-25 15:19:20.278667

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql


# revision identifiers, used by Alembic.
revision: str = 'b777fea1d803'
down_revision: Union[str, None] = 'b051277ded8a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SCHEMA = "bronze"
TABLE = "wb_www_fin_reports_1d"


def _table_exists(conn) -> bool:
    return bool(conn.execute(sa.text("SELECT to_regclass(:qname)"), {"qname": f"{SCHEMA}.{TABLE}"}).scalar())


def upgrade():
    conn = op.get_bind()
    if not _table_exists(conn):
        # ── Таблицы нет → создаём сразу по новой схеме (миксин + оверрайды типов) ─────────
        op.create_table(
            TABLE,
            sa.Column("api_token_id", sa.Integer(), nullable=False),
            sa.Column("run_uuid", psql.UUID(as_uuid=True), nullable=False),
            sa.Column("run_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
            sa.Column("run_schedule_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
            sa.Column("request_uuid", psql.UUID(as_uuid=True), primary_key=True, nullable=False),
            sa.Column("request_dttm", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.Column("request_parameters", psql.JSONB(astext_type=sa.Text()), nullable=True),
            # override: request_body = TEXT (в миксине JSONB)
            sa.Column("request_body", sa.Text(), nullable=True),
            sa.Column("response_code", sa.Integer(), server_default=sa.text("0"), nullable=False),
            sa.Column("response_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
            # override: response_body = BYTEA (в миксине TEXT)
            sa.Column("response_body", psql.BYTEA(), nullable=True),
            sa.Column("receive_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
            sa.Column("inserted_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=False),
            schema=SCHEMA,
        )
        # индексы/внешние ключи не создаём (по твоему требованию)
        return

    # ── Таблица есть → переводим схему под миксин ────────────────────────────────────────
    # Обнуляем данные, чтобы без боли проставить NOT NULL без дефолтов/бэкфилла
    op.execute(f'TRUNCATE TABLE "{SCHEMA}"."{TABLE}";')

    # 1) Удаляем старую колонку company_id (если была)
    op.execute(f'ALTER TABLE "{SCHEMA}"."{TABLE}" DROP COLUMN IF EXISTS "company_id";')

    # 2) Добавляем api_token_id (без FK, как просил), NOT NULL
    op.add_column(
        TABLE,
        sa.Column("api_token_id", sa.Integer(), nullable=False),
        schema=SCHEMA,
    )

    # 3) Добавляем недостающие поля из миксина (NOT NULL, без дефолтов)
    op.add_column(TABLE, sa.Column("run_schedule_dttm", sa.TIMESTAMP(timezone=True), nullable=False), schema=SCHEMA)
    op.add_column(TABLE, sa.Column("receive_dttm", sa.TIMESTAMP(timezone=True), nullable=False), schema=SCHEMA)
    op.add_column(TABLE, sa.Column("business_dttm", sa.TIMESTAMP(timezone=True), nullable=False), schema=SCHEMA)

    # 4) Приводим response_dttm к NOT NULL (по миксину)
    op.alter_column(
        TABLE,
        "response_dttm",
        schema=SCHEMA,
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=False,
    )

    # 5) Ставит DEFAULT 0 на response_code (и убеждаемся, что он NOT NULL)
    op.execute(f'ALTER TABLE "{SCHEMA}"."{TABLE}" ALTER COLUMN "response_code" SET DEFAULT 0;')
    op.alter_column(
        TABLE,
        "response_code",
        schema=SCHEMA,
        existing_type=sa.Integer(),
        nullable=False,
    )

    # 6) Типы request_body/response_body уже соответствуют нужным оверрайдам:
    #    request_body: TEXT, response_body: BYTEA — оставляем как есть.
    #    (Если в каком-то окружении они отличались — можно добавить ALTER TYPE при необходимости.)


def downgrade():
    conn = op.get_bind()
    if not _table_exists(conn):
        return

    # Откат к «старой» схеме
    op.execute(f'TRUNCATE TABLE "{SCHEMA}"."{TABLE}";')

    # 1) Удаляем новые поля миксина
    op.drop_column(TABLE, "business_dttm", schema=SCHEMA)
    op.drop_column(TABLE, "receive_dttm", schema=SCHEMA)
    op.drop_column(TABLE, "run_schedule_dttm", schema=SCHEMA)

    # 2) Возвращаем company_id (как было в старой модели), nullable
    op.add_column(TABLE, sa.Column("company_id", sa.Integer(), nullable=True), schema=SCHEMA)

    # 3) Убираем api_token_id
    op.drop_column(TABLE, "api_token_id", schema=SCHEMA)

    # 4) Делаем response_dttm nullable и снимаем DEFAULT с response_code
    op.alter_column(
        TABLE,
        "response_dttm",
        schema=SCHEMA,
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=True,
    )
    op.execute(f'ALTER TABLE "{SCHEMA}"."{TABLE}" ALTER COLUMN "response_code" DROP DEFAULT;')
