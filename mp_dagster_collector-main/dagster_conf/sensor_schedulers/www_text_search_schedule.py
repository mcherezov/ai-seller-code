from dagster import schedule, RunRequest, DefaultScheduleStatus
from dagster_conf.pipelines.www_text_search_job import wb_text_search_1d_job


@schedule(
    cron_schedule="0 9 * * *",
    job=wb_text_search_1d_job,
    execution_timezone="Europe/Moscow",
    default_status=DefaultScheduleStatus.RUNNING,
)
def wb_text_search_1d_schedule(context):
    return RunRequest(
        run_config={},
        tags={"dagster/scheduled_execution_time": context.scheduled_execution_time.isoformat()},
    )
