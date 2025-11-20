from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from dagster import resource
import os


@resource(config_schema={})
def postgres_resource(_):
    import ssl

    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL не задана")

    async_url = url.replace("postgresql://", "postgresql+asyncpg://", 1)

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE  # при необходимости

    engine = create_async_engine(
        async_url,
        pool_pre_ping=True,
        pool_size=20,  # пул на уровне asyncpg
        max_overflow=10,
        pool_recycle=3600,
        connect_args={
            "ssl": ssl_context,
            "statement_cache_size": 0
        },
    )

    return async_sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)

