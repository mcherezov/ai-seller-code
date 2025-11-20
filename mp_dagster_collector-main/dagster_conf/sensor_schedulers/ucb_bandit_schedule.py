from dagster import schedule
from dagster_conf.pipelines.ucb_bandit_job import ucb_bandit_wrapper_job


@schedule(
    cron_schedule="0 07 * * *",
    job=ucb_bandit_wrapper_job,
    execution_timezone="Europe/Moscow"
)
def ucb_bandit_daily_schedule():
    return {}
