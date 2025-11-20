from dagster import schedule
from dagster_conf.pipelines.llm_wrapper_job import llm_wrapper_job


@schedule(
    cron_schedule="0 10 * * *",  # каждый день в 10:00 по МСК
    job=llm_wrapper_job,
    execution_timezone="Europe/Moscow"
)
def llm_wrapper_schedule():
    return {}
