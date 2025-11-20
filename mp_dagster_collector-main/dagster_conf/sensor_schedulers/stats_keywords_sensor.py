import os
import psycopg2
from datetime import datetime
from pytz import timezone, UTC
from dagster import sensor, RunRequest

from dagster_conf.pipelines.stats_keywords_jobs import wb_adv_stats_keywords_1d_job, wb_adv_stats_keywords_1h_job

# настройки окна триггера в МСК
WINDOW_HOUR = 4                   # час
WINDOW_START_MINUTE = 5           # начало окна (минута)
WINDOW_DURATION_MINUTES = 5       # длина окна


@sensor(
    job=wb_adv_stats_keywords_1d_job,
    minimum_interval_seconds=60,
)
def bronze_stats_keywords_daily_sensor(context):
    now_msk = datetime.now(timezone("Europe/Moscow"))
    minute = now_msk.minute

    if not (
        now_msk.hour == WINDOW_HOUR
        and WINDOW_START_MINUTE <= minute < WINDOW_START_MINUTE + WINDOW_DURATION_MINUTES
    ):
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

    scheduled_msk = now_msk.replace(
        hour=WINDOW_HOUR,
        minute=WINDOW_START_MINUTE,
        second=0,
        microsecond=0,
    )
    scheduled_utc = scheduled_msk.astimezone(UTC).isoformat()

    for token_id, token in rows:
        run_key = f"{scheduled_msk.strftime('%Y%m%d')}-{token_id}"
        run_config = {
            "resources": {
                "wildberries_client": {
                    "config": {"token": token, "token_id": token_id}
                },
                "postgres": {"config": {}},
            },
            "ops": {
                "bronze__wb_adv_stats_keywords": {"config": {"time_grain": "1d"}},
                "silver__wb_adv_keyword_stats":  {"config": {"time_grain": "1d"}},
            },
        }
        context.log.info(f"Scheduling wb_adv_stats_keywords_1d_job for token_id={token_id}")
        yield RunRequest(
            run_key=run_key,
            run_config=run_config,
            tags={"dagster/scheduled_execution_time": scheduled_utc},
        )


@sensor(
    job=wb_adv_stats_keywords_1h_job,
    minimum_interval_seconds=60,
)
def bronze_stats_keywords_hourly_sensor(context):
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
                "bronze__wb_adv_stats_keywords": {"config": {"time_grain": "1h"}},
                "silver__wb_adv_keyword_stats":  {"config": {"time_grain": "1h"}},
            },
        }
        context.log.info(f"Scheduling wb_adv_stats_keywords_1h_job for token_id={token_id}")
        yield RunRequest(
            run_key=run_key,
            run_config=run_config,
            tags={"dagster/scheduled_execution_time": scheduled_utc},
        )
