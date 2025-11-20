from dagster import schedule
from dagster_conf.pipelines.ako_V2_job import ako_V2_wrapper_job


@schedule(
    cron_schedule="0 10 * * *",
    job=ako_V2_wrapper_job,
    execution_timezone="Europe/Moscow"
)
def ako_V2_wrapper_schedule():
    return {}
