"""add sequence keys

Revision ID: 2d2f429149c7
Revises: a9cb6355ca5d
Create Date: 2025-06-19 04:03:18.408680

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.schema import CreateSequence, DropSequence

# revision identifiers, used by Alembic.
revision: str = '2d2f429149c7'
down_revision: Union[str, None] = 'a9cb6355ca5d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLES = [
    "wb_ad_campaigns_1d",
    "wb_ad_campaigns_products_1d",
    "wb_ad_campaigns_auto_1d",
    "wb_ad_campaigns_united_1d",
]


def upgrade() -> None:
    for tbl in TABLES:
        seq = sa.Sequence(f"{tbl}_id_seq", schema="silver")
        # 1) создаём саму последовательность
        op.execute(CreateSequence(seq))
        # 2) ставим её как DEFAULT для колонки id
        op.alter_column(
            tbl,
            "id",
            schema="silver",
            existing_type=sa.BigInteger(),
            server_default=sa.text(f"nextval('silver.{tbl}_id_seq'::regclass)"),
            existing_nullable=False,
        )


def downgrade() -> None:
    for tbl in TABLES:
        # 1) убираем default
        op.alter_column(
            tbl,
            "id",
            schema="silver",
            existing_type=sa.BigInteger(),
            server_default=None,
            existing_nullable=False,
        )
        # 2) удаляем последовательность
        seq = sa.Sequence(f"{tbl}_id_seq", schema="silver")
        op.execute(DropSequence(seq))
