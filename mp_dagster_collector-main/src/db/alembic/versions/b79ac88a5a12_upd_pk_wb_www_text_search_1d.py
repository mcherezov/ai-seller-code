"""upd pk wb_www_text_search_1d

Revision ID: b79ac88a5a12
Revises: 0baf8ec2a65d
Create Date: 2025-07-28 09:58:44.412077

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b79ac88a5a12'
down_revision: Union[str, None] = '0baf8ec2a65d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade() -> None:
    # 1) Удаляем старый PK (на request_uuid)
    op.drop_constraint(
        constraint_name='wb_www_text_search_1d_pkey',
        table_name='wb_www_text_search_1d',
        schema='silver',
        type_='primary',
    )
    # 2) Создаем составной PK из трёх колонок
    op.create_primary_key(
        constraint_name='wb_www_text_search_1d_pkey',
        table_name='wb_www_text_search_1d',
        columns=['request_uuid', 'keyword', 'date'],
        schema='silver',
    )


def downgrade() -> None:
    # Откат: убираем составной PK…
    op.drop_constraint(
        constraint_name='wb_www_text_search_1d_pkey',
        table_name='wb_www_text_search_1d',
        schema='silver',
        type_='primary',
    )
    # …и возвращаем прежний PK только на request_uuid
    op.create_primary_key(
        constraint_name='wb_www_text_search_1d_pkey',
        table_name='wb_www_text_search_1d',
        columns=['request_uuid'],
        schema='silver',
    )