from dagster import schedule, RunRequest, DefaultScheduleStatus
from dagster_conf.pipelines.wb_search_results_6h_job import wb_search_results_6h_job

@schedule(
    cron_schedule="30 */6 * * *",
    job=wb_search_results_6h_job,
    execution_timezone="Europe/Moscow",
    default_status=DefaultScheduleStatus.RUNNING,
)
def wb_search_results_6h_schedule(context):
    return RunRequest(
        run_config={},
        tags={"dagster/scheduled_execution_time": context.scheduled_execution_time.isoformat()},
    )
