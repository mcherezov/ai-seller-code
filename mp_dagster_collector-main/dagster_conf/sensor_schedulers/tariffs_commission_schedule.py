import os
import psycopg2
from datetime import datetime
from dagster import schedule, RunRequest
from dagster_conf.pipelines.tariffs_commission_job import wb_tariffs_commission_1d_job

@schedule(
    job=wb_tariffs_commission_1d_job,
    cron_schedule="0 6 * * *",               # Каждый день в 06:00
    execution_timezone="Europe/Moscow",      # По московскому времени
)
def daily_tariffs_commission_schedule(context):
    """
    По расписанию в 06:00 МСК запускаем wb_tariffs_commission_1d_job один раз,
    беря первый активный токен из core.tokens.
    """
    # 1. Подключаемся и берём первый активный токен
    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode="require")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT token_id, token
                  FROM core.tokens
                 WHERE is_active = true
                 ORDER BY token_id
                 LIMIT 1
                """
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        context.log.warning("Нет активных токенов в core.tokens — пропускаем запуск wb_tariffs_commission_1d_job")
        return

    token_id, token = row

    # 2. Формируем run_key и run_config
    now = datetime.now()
    run_key = f"{now.strftime('%Y%m%d')}-commission"
    run_config = {
        "resources": {
            "wildberries_client": {
                "config": {"token": token, "token_id": token_id}
            },
            "postgres": {"config": {}},
        }
    }

    context.log.info(f"Scheduling wb_tariffs_commission_1d_job for token_id={token_id}")
    return RunRequest(
        run_key=run_key,
        run_config=run_config,
        # тэг, которым в ассете мы проставляем run_dttm:
        tags={"dagster/scheduled_execution_time": now.isoformat()},
    )
