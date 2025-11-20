import dagster as dg
from dagster_conf.resources.pg_resource import postgres_resource
from sqlalchemy import text

@dg.op(
    required_resource_keys={"postgres"},
    description="Обновление витрины gold.vm_product_orders_1d",
)
async def refresh_vm_product_orders_1d(context):
    session_maker = context.resources.postgres

    context.log.info(f"Start refreshing materialized view gold.vm_product_orders_1d")
    async with session_maker() as session:
        await session.execute(text("ANALYZE silver_v2.wb_sales_funnels_1d"))
        await session.execute(text("ANALYZE silver_v2.wb_fin_reports_1w"))
        await session.execute(text("ANALYZE silver_v2.wb_adv_product_stats_1d"))
        await session.execute(text("ANALYZE silver_v2.wb_paid_storage_1d"))
        await session.execute(text("ANALYZE silver_v2.wb_commission_1d"))
        await session.execute(text("ANALYZE core.individual_commissions"))
        await session.execute(text("ANALYZE core.companies"))
        await session.execute(text("ANALYZE core.sellers"))
        await session.execute(text("ANALYZE core.product_costs"))

        await session.execute(text("REFRESH MATERIALIZED VIEW gold.vm_product_orders_1d"))
        res = await session.execute(text("SELECT max(date) FROM gold.vm_product_orders_1d"))
        max_date = res.first()
        await session.commit()

    context.log.info(f"Finish refreshing gold.vm_product_orders_1d. Max date: {max_date}")

    return {"inserted": 0}

@dg.job(
    name="gold__vm_product_orders_1d",
    description="Процесс обновления витрины gold.vm_product_orders_1d",
    tags={"layer": "gold"},
    resource_defs={"postgres": postgres_resource},
    executor_def=dg.in_process_executor,
)
def refresh_vm_product_orders_1d_job():
    refresh_vm_product_orders_1d()

@dg.schedule(
    cron_schedule="0 6 * * *",
    job=refresh_vm_product_orders_1d_job,
    execution_timezone="Europe/Moscow",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def refresh_vm_product_orders_1d_schedule(context):
    return dg.RunRequest(
        run_config={},
        tags={"dagster/scheduled_execution_time": context.scheduled_execution_time.isoformat()},
    )
