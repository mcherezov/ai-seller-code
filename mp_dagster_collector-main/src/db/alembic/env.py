import os
import sys
import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.engine import Connection
from dotenv import load_dotenv

# -----------------------------------------------------------------------
# 1) Путь к корню проекта и загрузка .env
# -----------------------------------------------------------------------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
dotenv_path = os.path.join(project_root, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
sys.path.insert(0, project_root)

# -----------------------------------------------------------------------
# 2) Импорт метаданных
# -----------------------------------------------------------------------
from src.db.base import Base  # DeclarativeBase
import src.db.bronze.models
import src.db.silver.models
import src.db.core.models
import src.db.bronze.ozon_models

# -----------------------------------------------------------------------
# 3) Logging configuration
# -----------------------------------------------------------------------
config = context.config
fileConfig(config.config_file_name)

# -----------------------------------------------------------------------
# 4) Target metadata – SQLAlchemy-метаданные
# -----------------------------------------------------------------------
target_metadata = Base.metadata

# -----------------------------------------------------------------------
# 5) Фильтрация схем 'ads' и 'public'
# -----------------------------------------------------------------------
def include_object(object, name, type_, reflected, compare_to):
    schema = getattr(object, "schema", None)
    return schema in ("silver", "bronze")

# -----------------------------------------------------------------------
# 6) «Синхронная» часть миграций, которую будем вызывать внутри async
# -----------------------------------------------------------------------
def _do_run_migrations(connection: Connection):
    # создаём схему bronze, если её нет
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS core"))
    connection.commit()

    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
        include_schemas=True,
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()

# -----------------------------------------------------------------------
# 7) Асинхронный запуск миграций (online mode)
# -----------------------------------------------------------------------
async def run_migrations_online():
    # 7.1) Берём sync-URL и конвертируем под asyncpg
    sync_url = os.getenv("DATABASE_URL")
    if not sync_url:
        raise RuntimeError("Переменная окружения DATABASE_URL не задана. Проверьте .env.")
    async_url = sync_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    # 7.2) Обновляем конфиг Alembic
    config.set_main_option("sqlalchemy.url", async_url)

    # 7.3) Создаём асинхронный движок
    connectable = create_async_engine(
        async_url,
        poolclass=pool.NullPool,
    )

    # 7.4) Подключаемся и вызываем синхронную функцию миграций в контексте run_sync
    async with connectable.connect() as connection:
        await connection.run_sync(_do_run_migrations)

    # 7.5) Закрываем движок
    await connectable.dispose()

# -----------------------------------------------------------------------
# 8) Offline миграции остаются без изменений
# -----------------------------------------------------------------------
def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url") or os.getenv("DATABASE_URL")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()

# -----------------------------------------------------------------------
# 9) Выбор режима
# -----------------------------------------------------------------------
if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
