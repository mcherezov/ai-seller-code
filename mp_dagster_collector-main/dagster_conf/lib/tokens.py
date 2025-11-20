from __future__ import annotations

import os
from functools import lru_cache
from typing import Dict

from sqlalchemy import create_engine, text


def _build_sync_dsn() -> str:
    dsn = os.getenv("POSTGRES_SYNC_DSN")
    if dsn:
        return dsn

    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if "+psycopg2" not in db_url and "+psycopg" not in db_url:
            return db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
        return db_url

    user = os.getenv("DB_USER", "")
    pwd = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "postgres")
    sslm = os.getenv("DB_SSLMODE", "require")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}?sslmode={sslm}"


_ENGINE = create_engine(_build_sync_dsn(), future=True, pool_pre_ping=True)


@lru_cache(maxsize=128)
def token_for_company(company_id: int) -> Dict[str, object]:
    """
    Возвращает {token_id, token} для заданного company_id из core.tokens.
    Правило: берём активный токен (is_active=TRUE) по возрастанию token_id.
    """
    with _ENGINE.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT token_id, token
                  FROM core.tokens
                 WHERE company_id = :cid
                   AND is_active = TRUE
              ORDER BY token_id ASC
                 LIMIT 1
                """
            ),
            {"cid": company_id},
        ).first()
        if not row:
            raise RuntimeError(f"Активный token не найден для company_id={company_id}")
        return {"token_id": int(row[0]), "token": row[1]}
