import os
import psycopg2


def get_active_tokens() -> list[tuple[int, str]]:
    """
    Возвращает список (token_id, token) из core.tokens,
    где is_active = true и (is_readonly = false OR is_readonly IS NULL).
    """
    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode="require")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT token_id, token
                  FROM core.tokens
                 WHERE is_active = true
                   AND (is_readonly IS DISTINCT FROM true)
            """)
            return cur.fetchall()
    finally:
        conn.close()
