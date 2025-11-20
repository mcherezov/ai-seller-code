from dagster import sensor, RunRequest, SkipReason, RunsFilter, DagsterRunStatus
from dagster_conf.pipelines.ako_V2_job import ako_V2_wrapper_job
from dagster_conf.pipelines.ucb_bandit_job import ucb_bandit_wrapper_job

# Имена джоб, за которыми следим
SRC_JOBS = [
    "wb_adv_fullstats_1d_job",
    "wb_adv_promotion_adverts_1d_job",
    "wb_adv_stats_keywords_1d_job",
]

# Wrapper-джобы, которые нужно запустить после них
TARGET_JOBS = [
    "ako_V2_wrapper_job",
    "ucb_bandit_wrapper_job",
]

@sensor(
    name="trigger_wrapper_jobs_sensor",
    minimum_interval_seconds=60,
    jobs=[ako_V2_wrapper_job, ucb_bandit_wrapper_job],
)
def trigger_wrapper_jobs_sensor(context):
    instance = context.instance

    # 1. Берём последние успешные запуски
    latest_runs = {}
    for job_name in SRC_JOBS:
        recs = instance.get_runs(
            filters=RunsFilter(job_name=job_name, statuses=[DagsterRunStatus.SUCCESS]),
            limit=1,
        )
        if not recs:
            return SkipReason(f"Нет успешных запусков для {job_name} ещё")
        latest_runs[job_name] = recs[0]

    # 2. Проверяем единый scheduled_execution_time
    sched_times = {
        run.tags.get("dagster/scheduled_execution_time")
        for run in latest_runs.values()
    }
    if len(sched_times) != 1 or None in sched_times:
        return SkipReason("Ожидаем одинаковый scheduled_execution_time у всех трёх джоб")
    schedule_time = sched_times.pop()

    # 3. Генерируем RunRequest для каждого wrapper-job
    run_key_base = f"wrapper_for_{schedule_time}"
    for job_name in TARGET_JOBS:
        yield RunRequest(
            run_key=f"{run_key_base}_{job_name}",
            job_name=job_name,
            run_config={},
        )
