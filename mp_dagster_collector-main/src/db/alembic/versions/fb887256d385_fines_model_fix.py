"""fines model fix

Revision ID: fb887256d385
Revises: 8abcb19eff27
Create Date: 2025-08-02 03:20:57.396602

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'fb887256d385'
down_revision: Union[str, None] = '8abcb19eff27'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 1. Переименование fine_discription → fine_description, если есть
    op.execute("""
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='silver' AND table_name='wb_fines_1d' AND column_name='fine_discription'
  ) THEN
    EXECUTE format(
      'ALTER TABLE silver.wb_fines_1d RENAME COLUMN %I TO %I',
      'fine_discription', 'fine_description'
    );
  END IF;
END
$$;
""")

    # 2. Удалить logistic_type, если он существует
    op.execute("ALTER TABLE silver.wb_fines_1d DROP COLUMN IF EXISTS logistic_type;")

    # 3. Заменить NULL в fine_description на пустую строку, чтобы PK мог включать это поле
    op.execute("""
UPDATE silver.wb_fines_1d
SET fine_description = ''
WHERE fine_description IS NULL;
""")

    # 4. Пересоздать PK: убрать старый (если есть) и создать новый с fine_description и wb_reward
    op.execute("ALTER TABLE silver.wb_fines_1d DROP CONSTRAINT IF EXISTS wb_fines_1d_pkey;")
    op.execute("""
ALTER TABLE silver.wb_fines_1d
  ADD CONSTRAINT wb_fines_1d_pkey PRIMARY KEY (
    request_uuid,
    fine_id,
    nm_id,
    supplier_article,
    fine_description,
    wb_reward
  );
""")


def downgrade():
    # 1. Восстановить logistic_type (если нет) и сделать NOT NULL с заполнением
    op.execute("ALTER TABLE silver.wb_fines_1d ADD COLUMN IF NOT EXISTS logistic_type text;")
    op.execute("""
UPDATE silver.wb_fines_1d
SET logistic_type = 'UNDEF'
WHERE logistic_type IS NULL;
""")
    op.execute("ALTER TABLE silver.wb_fines_1d ALTER COLUMN logistic_type SET NOT NULL;")

    # 2. Вернуть старый PK: удалить текущий и создать прежний (с logistic_type вместо fine_description)
    op.execute("ALTER TABLE silver.wb_fines_1d DROP CONSTRAINT IF EXISTS wb_fines_1d_pkey;")
    op.execute("""
ALTER TABLE silver.wb_fines_1d
  ADD CONSTRAINT wb_fines_1d_pkey PRIMARY KEY (
    request_uuid,
    fine_id,
    nm_id,
    supplier_article,
    logistic_type,
    wb_reward
  );
""")

    # 3. Переименование fine_description → fine_discription обратно, если есть
    op.execute("""
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='silver' AND table_name='wb_fines_1d' AND column_name='fine_description'
  ) THEN
    EXECUTE format(
      'ALTER TABLE silver.wb_fines_1d RENAME COLUMN %I TO %I',
      'fine_description', 'fine_discription'
    );
  END IF;
END
$$;
""")