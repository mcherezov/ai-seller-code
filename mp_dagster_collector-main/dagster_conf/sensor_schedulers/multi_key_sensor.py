import os
import psycopg2
from datetime import datetime
from pytz import timezone
from dagster import sensor, RunRequest

from dagster_conf.pipelines.wb_bronze_ppl import wb_full_job

@sensor(
    job=wb_full_job,
    minimum_interval_seconds=60,
)
def wb_multi_key_sensor(context):
    """
    Каждую минуту проверяем, не наступила ли 30-я минута часа
    по московскому времени. Если да – запускаем wb_full_job
    для каждого токена из core.tokens.
    """
    now = datetime.now(timezone("Europe/Moscow"))
    if now.minute != 30:
        return

    # Подключение к БД через psycopg2
    conn = psycopg2.connect(
        os.getenv("DATABASE_URL"),
        sslmode="require"
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT token_id, token FROM core.tokens WHERE token_id IN (2, 3, 4, 5, 45)")
            rows = cur.fetchall()

            if not rows:
                context.log.warning("Нет активных токенов в core.tokens")
                return

            for token_id, token in rows:
                run_key = f"{now.strftime('%Y%m%d%H')}-{token_id}"
                run_config = {
                    "resources": {
                        "wildberries_client": {
                            "config": {
                                "token": token,
                                "token_id": token_id
                            }
                        },
                        "postgres": {"config": {}},
                    }
                }
                context.log.info(f"Scheduling wb_full_job for token_id={token_id}")
                yield RunRequest(run_key=run_key, run_config=run_config)
    finally:
        conn.close()
