"""Add api_token_id to bronze.wb_adv_fullstats_1d

Revision ID: 14cb392663e2
Revises: a3a1303ce143
Create Date: 2025-06-26 20:14:03.251876

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '14cb392663e2'
down_revision: Union[str, None] = 'a3a1303ce143'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade() -> None:
    # 1) Добавляем колонку api_token_id
    op.add_column(
        'wb_adv_fullstats_1d',
        sa.Column('api_token_id', sa.Integer(), nullable=False),
        schema='bronze',
    )
    # 2) Создаем foreign key на core.tokens(token_id)
    op.create_foreign_key(
        'fk_wb_adv_fullstats_1d_api_token_id_core_tokens',
        'wb_adv_fullstats_1d',               # source table
        'tokens',                            # target table name
        ['api_token_id'],                    # source columns
        ['token_id'],                        # target columns
        source_schema='bronze',
        referent_schema='core',
    )


def downgrade() -> None:
    # 1) Удаляем foreign key
    op.drop_constraint(
        'fk_wb_adv_fullstats_1d_api_token_id_core_tokens',
        'wb_adv_fullstats_1d',
        schema='bronze',
        type_='foreignkey',
    )
    # 2) Удаляем колонку api_token_id
    op.drop_column(
        'wb_adv_fullstats_1d',
        'api_token_id',
        schema='bronze',
    )