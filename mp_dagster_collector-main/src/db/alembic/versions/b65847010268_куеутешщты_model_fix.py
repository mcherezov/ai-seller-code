"""куеутешщты model fix

Revision ID: b65847010268
Revises: fb887256d385
Create Date: 2025-08-02 03:33:55.866925

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b65847010268'
down_revision: Union[str, None] = 'fb887256d385'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 1. Переименование retentions_discription → retentions_description, если есть
    op.execute("""
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='silver' AND table_name='wb_retentions_1d' AND column_name='retentions_discription'
  ) THEN
    EXECUTE format(
      'ALTER TABLE silver.wb_retentions_1d RENAME COLUMN %I TO %I',
      'retentions_discription', 'retentions_description'
    );
  END IF;
END
$$;
""")

    # 2. Удаление logistic_type, если существует
    op.execute("ALTER TABLE silver.wb_retentions_1d DROP COLUMN IF EXISTS logistic_type;")

    # 3. Заменяем NULL в retentions_description на пустую строку, чтобы можно было включить в PK
    op.execute("""
UPDATE silver.wb_retentions_1d
SET retentions_description = ''
WHERE retentions_description IS NULL;
""")

    # 4. Пересоздание PK: удалить старый и создать новый
    op.execute("ALTER TABLE silver.wb_retentions_1d DROP CONSTRAINT IF EXISTS wb_retentions_1d_pkey;")
    op.execute("""
ALTER TABLE silver.wb_retentions_1d
  ADD CONSTRAINT wb_retentions_1d_pkey PRIMARY KEY (
    request_uuid,
    retention_id,
    retentions_description,
    wb_reward
  );
""")


def downgrade():
    # 1. Восстановление logistic_type (если нет): добавить, заполнить и сделать NOT NULL
    op.execute("ALTER TABLE silver.wb_retentions_1d ADD COLUMN IF NOT EXISTS logistic_type text;")
    op.execute("""
UPDATE silver.wb_retentions_1d
SET logistic_type = 'UNDEF'
WHERE logistic_type IS NULL;
""")
    op.execute("ALTER TABLE silver.wb_retentions_1d ALTER COLUMN logistic_type SET NOT NULL;")

    # 2. Откат PK: убрать текущий и вернуть прежний с logistic_type вместо retentions_description
    op.execute("ALTER TABLE silver.wb_retentions_1d DROP CONSTRAINT IF EXISTS wb_retentions_1d_pkey;")
    op.execute("""
ALTER TABLE silver.wb_retentions_1d
  ADD CONSTRAINT wb_retentions_1d_pkey PRIMARY KEY (
    request_uuid,
    retention_id,
    logistic_type,
    wb_reward
  );
""")

    # 3. Переименование retentions_description обратно в retentions_discription, если есть
    op.execute("""
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='silver' AND table_name='wb_retentions_1d' AND column_name='retentions_description'
  ) THEN
    EXECUTE format(
      'ALTER TABLE silver.wb_retentions_1d RENAME COLUMN %I TO %I',
      'retentions_description', 'retentions_discription'
    );
  END IF;
END
$$;
""")