from dagster import schedule
from dagster_conf.pipelines.wb_bronze_ppl import wb_full_job

@schedule(
    cron_schedule="30 * * * *",         # 30-я минута каждого часа
    job=wb_full_job,
    execution_timezone="Europe/Moscow", # по Москве
)
def wb_half_hourly_schedule(context):
    """
    Запускает wb_full_job каждый час в 30 минут.
    """
    return {}
