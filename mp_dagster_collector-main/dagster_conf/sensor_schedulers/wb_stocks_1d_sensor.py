import os
import psycopg2
from datetime import datetime
from pytz import timezone, UTC
from dagster import sensor, RunRequest

from dagster_conf.pipelines.wb_stocks_1d_job import wb_stocks_1d_job

# окно в минутах после 23:55 (в пределах 4 минут)
WINDOW_MINUTES = 4

@sensor(
    job=wb_stocks_1d_job,
    minimum_interval_seconds=60,
)
def bronze_wb_stocks_report_daily_sensor(context):
    """
    Каждую минуту проверяем, попадает ли текущее время в окно 23:55–23:59 МСК.
    Если да — планируем запуск bronze_wb_stocks_report_1d → silver_wb_stocks_1d
    для каждого активного токена, проставляя в тегах метку начала окна (23:55) в UTC.
    """
    now_msk = datetime.now(timezone("Europe/Moscow"))

    if not (now_msk.hour == 23 and 55 <= now_msk.minute < 55 + WINDOW_MINUTES):
        return

    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode="require")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT token_id, token FROM core.tokens
                WHERE token_id IN (1, 2, 3, 4, 5, 45)
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        context.log.warning("Нет активных токенов в core.tokens")
        return

    scheduled_msk = now_msk.replace(hour=23, minute=55, second=0, microsecond=0)
    scheduled_utc = scheduled_msk.astimezone(UTC).isoformat()

    for token_id, token in rows:
        run_key = f"{scheduled_msk.strftime('%Y%m%d')}-{token_id}"
        run_config = {
            "resources": {
                "wildberries_client": {
                    "config": {"token": token, "token_id": token_id}
                },
                "postgres": {"config": {}},
            }
        }
        context.log.info(f"Scheduling bronze_wb_stocks_report_1d for token_id={token_id}")
        yield RunRequest(
            run_key=run_key,
            run_config=run_config,
            tags={"dagster/scheduled_execution_time": scheduled_utc},
        )
