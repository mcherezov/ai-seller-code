"""Upd wb_mp_skus_1d

Revision ID: 5f86db1b4532
Revises: d9e3f4e68c04
Create Date: 2025-07-04 14:39:50.533492

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5f86db1b4532'
down_revision: Union[str, None] = 'd9e3f4e68c04'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) Drop the old PK. Replace 'pk_wb_mp_skus_1d' with the *actual* name
    op.drop_constraint(
        constraint_name='pk_wb_mp_skus_1d',
        table_name='wb_mp_skus_1d',
        schema='silver',
        type_='primary',
    )

    # 2) Create the new composite PK including barcode
    op.create_primary_key(
        constraint_name='pk_wb_mp_skus_1d',
        table_name='wb_mp_skus_1d',
        columns=['request_uuid', 'response_dttm', 'barcode'],
        schema='silver',
    )


def downgrade() -> None:
    # 1) Drop the three-column PK
    op.drop_constraint(
        constraint_name='pk_wb_mp_skus_1d',
        table_name='wb_mp_skus_1d',
        schema='silver',
        type_='primary',
    )

    # 2) Recreate the old PK on just (request_uuid, response_dttm)
    op.create_primary_key(
        constraint_name='pk_wb_mp_skus_1d',
        table_name='wb_mp_skus_1d',
        columns=['request_uuid', 'response_dttm'],
        schema='silver',
    )