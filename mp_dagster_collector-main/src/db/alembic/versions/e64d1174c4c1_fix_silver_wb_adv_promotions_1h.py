"""fix silver.wb_adv_promotions_1h

Revision ID: e64d1174c4c1
Revises: 30e4b54fe1e4
Create Date: 2025-08-20 12:45:21.995108

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e64d1174c4c1'
down_revision: Union[str, None] = '30e4b54fe1e4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) снять текущий PK (product_id, advert_id, business_dttm)
    op.drop_constraint(
        "pk_adv_promotions_1h",
        "wb_adv_promotions_1h",
        type_="primary",
        schema="silver",
    )

    # 2) удалить колонку product_id
    op.drop_column("wb_adv_promotions_1h", "product_id", schema="silver")

    # 3) создать новый PK (advert_id, business_dttm)
    op.create_primary_key(
        "pk_adv_promotions_1h",
        "wb_adv_promotions_1h",
        ["advert_id", "business_dttm"],
        schema="silver",
    )


def downgrade() -> None:
    # Откат: вернуть колонку и старый PK (product_id, advert_id, business_dttm)
    op.drop_constraint(
        "pk_adv_promotions_1h",
        "wb_adv_promotions_1h",
        type_="primary",
        schema="silver",
    )

    op.add_column(
        "wb_adv_promotions_1h",
        sa.Column("product_id", sa.BigInteger(), nullable=False, server_default="0"),
        schema="silver",
    )
    # (по желанию можно убрать дефолт)
    op.alter_column(
        "wb_adv_promotions_1h",
        "product_id",
        server_default=None,
        schema="silver",
    )

    op.create_primary_key(
        "pk_adv_promotions_1h",
        "wb_adv_promotions_1h",
        ["product_id", "advert_id", "business_dttm"],
        schema="silver",
    )
