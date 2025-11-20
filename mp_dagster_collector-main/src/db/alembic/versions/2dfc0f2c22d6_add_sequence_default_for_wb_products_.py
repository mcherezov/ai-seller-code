"""add sequence default for wb_products_search_texts

Revision ID: 2dfc0f2c22d6
Revises: 16278e9bfd43
Create Date: 2025-06-20 18:07:39.954561

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2dfc0f2c22d6'
down_revision: Union[str, None] = '16278e9bfd43'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade() -> None:
    # 1) создаём sequence
    op.execute(
        """
        CREATE SEQUENCE bronze.wb_products_search_texts_id_seq;
        """
    )

    # 2) назначаем её дефолтом для id
    op.execute(
        """
        ALTER TABLE bronze.wb_products_search_texts
        ALTER COLUMN id
        SET DEFAULT nextval('bronze.wb_products_search_texts_id_seq');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE bronze.wb_products_search_texts
        ALTER COLUMN id DROP DEFAULT;
        """
    )
    op.execute(
        """
        DROP SEQUENCE bronze.wb_products_search_texts_id_seq;
        """
    )