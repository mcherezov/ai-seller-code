"""Upd constraint PK wb_mp_skus_1d

Revision ID: 2c747086304c
Revises: 5f86db1b4532
Create Date: 2025-07-04 17:47:02.142324

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2c747086304c'
down_revision: Union[str, None] = '5f86db1b4532'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 1) Drop the old primary key
    op.drop_constraint(
        'pk_wb_mp_skus_1d',                # previously created PK name
        'wb_mp_skus_1d',
        schema='silver',
        type_='primary'
    )

    # 2) Drop the unique constraint on barcode (automatically named by PG)
    op.drop_constraint(
        'wb_mp_skus_1d_barcode_key',       # Postgres default for UNIQUE(barcode)
        'wb_mp_skus_1d',
        schema='silver',
        type_='unique'
    )

    # 3) Create the new composite primary key
    op.create_primary_key(
        'pk_wb_mp_skus_1d',
        'wb_mp_skus_1d',
        ['request_uuid', 'response_dttm', 'barcode', 'company_id'],
        schema='silver'
    )

    # 4) (Optional) recreate a non-unique index on barcode for lookups
    op.create_index(
        'ix_wb_mp_skus_1d_barcode',
        'wb_mp_skus_1d',
        ['barcode'],
        unique=False,
        schema='silver'
    )


def downgrade():
    # 1) Drop the non-unique barcode index
    op.drop_index(
        'ix_wb_mp_skus_1d_barcode',
        table_name='wb_mp_skus_1d',
        schema='silver'
    )

    # 2) Drop the new composite PK
    op.drop_constraint(
        'pk_wb_mp_skus_1d',
        'wb_mp_skus_1d',
        schema='silver',
        type_='primary'
    )

    # 3) Recreate the old unique constraint on barcode
    op.create_unique_constraint(
        'wb_mp_skus_1d_barcode_key',
        'wb_mp_skus_1d',
        ['barcode'],
        schema='silver'
    )

    # 4) Restore the old primary key on (request_uuid, response_dttm)
    op.create_primary_key(
        'pk_wb_mp_skus_1d',
        'wb_mp_skus_1d',
        ['request_uuid', 'response_dttm'],
        schema='silver'
    )
