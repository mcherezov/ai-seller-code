import os
import psycopg2
from datetime import datetime
from pytz import timezone, UTC
from dagster import sensor, RunRequest

from dagster_conf.pipelines.supplier_orders_job import wb_supplier_orders_1d_job

# окно в минутах после 06:00, в котором принимаем тик
WINDOW_MINUTES = 5

@sensor(
    job=wb_supplier_orders_1d_job,
    minimum_interval_seconds=60,
)
def bronze_supplier_orders_daily_sensor(context):
    """
    Каждую минуту проверяем в окне 06:00–06:05 МСК.
    Если текущее время попадает в это окно — планируем запуск bronze_wb_supplier_orders_1d
    для каждого активного токена, передавая в тегах плановое время (06:00 МСК).
    """
    now_msk = datetime.now(timezone("Europe/Moscow"))

    # триггерим в окне 06:00–06:05
    if not (now_msk.hour == 4 and now_msk.minute < WINDOW_MINUTES):
        return

    # Берём всех активные токены из core.tokens
    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode="require")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT token_id, token FROM core.tokens
                WHERE token_id IN (1, 2, 3, 4, 5, 45)
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        context.log.warning("Нет активных токенов в core.tokens")
        return

    # В качестве тега используем начало окна — 06:00 МСК
    scheduled_msk = now_msk.replace(minute=0, second=0, microsecond=0)
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
        context.log.info(f"Scheduling bronze_wb_supplier_orders_1d for token_id={token_id}")
        yield RunRequest(
            run_key=run_key,
            run_config=run_config,
            tags={"dagster/scheduled_execution_time": scheduled_utc},
        )
