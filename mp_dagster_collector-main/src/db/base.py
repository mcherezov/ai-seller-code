import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
load_dotenv(dotenv_path="config/.env")

SYNC_DATABASE_URL = os.getenv("DATABASE_URL")
if not SYNC_DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задана")

ASYNC_DATABASE_URL = SYNC_DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# создаём асинхронный двигатель
engine = create_async_engine(ASYNC_DATABASE_URL, pool_pre_ping=True, echo=False)

# асинхронная сессия
AsyncSessionLocal = sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,
)

Base = declarative_base()
