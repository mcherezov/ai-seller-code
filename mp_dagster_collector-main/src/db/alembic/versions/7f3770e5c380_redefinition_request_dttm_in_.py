"""redefinition request_dttm in WbAdvFullstats1h

Revision ID: 7f3770e5c380
Revises: 21fb6feb8486
Create Date: 2025-08-13 18:25:50.053870

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7f3770e5c380'
down_revision: Union[str, None] = '21fb6feb8486'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Важно: просто снимаем DEFAULT; тип и NOT NULL сохраняем.
    op.alter_column(
        "wb_adv_fullstats_1h",
        "request_dttm",
        schema="bronze",
        existing_type=sa.TIMESTAMP(timezone=True),
        server_default=None,        # ← DROP DEFAULT
        existing_nullable=False,
    )



def downgrade():
    # Возвращаем DEFAULT на стороне БД (как было в миксине) — now() т.е. текущий момент транзакции (UTC).
    op.alter_column(
        "wb_adv_fullstats_1h",
        "request_dttm",
        schema="bronze",
        existing_type=sa.TIMESTAMP(timezone=True),
        server_default=sa.text("now()"),
        existing_nullable=False,
    )