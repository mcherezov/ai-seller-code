from dagster import op, job, ScheduleDefinition
from dagster_conf.lib.partitions import refresh_company_partitions
from dagster_conf.resources.pg_resource import postgres_resource
from zoneinfo import ZoneInfo
from datetime import datetime

MSK = ZoneInfo("Europe/Moscow")

@op(required_resource_keys={"postgres"})
def sync_companies_op(context, cfg: dict):
    """
    Ежедневно подтягивает динамические company_id из БД и публикует их
    в DynamicPartitionsDefinition('company_id').
    """
    # dagster instance доступен через context.instance
    instance = context.instance

    # refresh_company_partitions у нас async → обернём простым раннером
    import asyncio
    values = asyncio.run(
        refresh_company_partitions(instance, context.resources.postgres, cfg)
    )
    context.log.info(f"Dynamic partitions 'company_id' updated: {len(values)} values")

@job(resource_defs={"postgres": postgres_resource})
def sync_companies_job():
    sync_companies_op()

def make_sync_companies_schedule(cfg: dict) -> ScheduleDefinition:
    """
    Создаёт daily-расписание обновления динамических партиций.
    Время — до основного пайплайна.
    """
    cron = cfg.get("partitions", {}).get("company_id", {}).get("refresh", {}).get("cron", "0 0 * * *")
    tz = cfg.get("timezone", "Europe/Moscow")
    return ScheduleDefinition(
        name="sync_company_partitions_daily",
        job=sync_companies_job,
        cron_schedule=cron,
        execution_timezone=tz,
        run_config={"ops": {"sync_companies_op": {"config": cfg}}},  # если решите читать cfg из config
    )
