import os
import psycopg2
from datetime import datetime
from pytz import timezone, UTC

from dagster import sensor, RunRequest

from dagster_conf.pipelines.promotions_job import wb_adv_promotions_1h_job

WINDOW_START_MINUTE = 5
WINDOW_DURATION_MINUTES = 5

@sensor(
    job=wb_adv_promotions_1h_job,
    minimum_interval_seconds=60,
)
def wb_adv_promotions_hourly_sensor(context):
    """
    Каждую минуту проверяем, наступило ли окно 05–10 минут нового часа (МСК).
    Если да — запускаем wb_adv_promotions_1h_job для каждого токена.
    """
    now_msk = datetime.now(timezone("Europe/Moscow"))
    minute = now_msk.minute

    if not (WINDOW_START_MINUTE <= minute < WINDOW_START_MINUTE + WINDOW_DURATION_MINUTES):
        return

    conn = psycopg2.connect(os.getenv("DATABASE_URL"), sslmode="require")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT token_id, token FROM core.tokens WHERE token_id IN (2, 3, 4, 5, 45)")
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        context.log.warning("Нет активных токенов в core.tokens")
        return

    scheduled_msk = now_msk.replace(minute=0, second=0, microsecond=0)
    scheduled_utc = scheduled_msk.astimezone(UTC).isoformat()

    for token_id, token in rows:
        run_key = f"{scheduled_msk.strftime('%Y%m%d%H')}-{token_id}"

        run_config = {
            "resources": {
                "wildberries_client": {
                    "config": {"token": token, "token_id": token_id}
                },
                "postgres": {"config": {}},
            },
            "ops": {
            },
        }

        context.log.info(
            f"Scheduling wb_adv_promotions_1h_job for token_id={token_id}"
        )
        yield RunRequest(
            run_key=run_key,
            run_config=run_config,
            tags={"dagster/scheduled_execution_time": scheduled_utc},
        )
