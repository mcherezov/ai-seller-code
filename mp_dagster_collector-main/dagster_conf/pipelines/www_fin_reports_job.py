from dagster import job, in_process_executor, Out, op
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dagster_conf.pipelines.www_fin_reports_ops import (
    get_report_ids,
    write_fin_report_zip,
    unpack_and_load_excel,
    load_wb_sales,
    load_wb_logistics,
    load_wb_adjustments_general,
    load_wb_adjustments_by_product,
)
from dagster_conf.pipelines.wb_bronze_ops import generate_batch_id as _gen_batch
from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.resources.secret_manager import secret_manager
from dagster_conf.resources.selenium_remote import selenium_remote

MSK = ZoneInfo("Europe/Moscow")

@op(out=Out(str), description="Генерирует уникальный batch_id")
def get_batch_id(context) -> str:
    return _gen_batch()


@op(out=Out(str), description="Определяет дату отчёта по тегу dagster/scheduled_execution_time; если тега нет — вчера (MSK)")
def get_report_date(context) -> str:
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    if scheduled_iso:
        s = scheduled_iso.strip()
        # поддержка 'Z' в конце
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        run_schedule_dttm = dt.astimezone(MSK)
        report_date = (run_schedule_dttm - timedelta(days=1)).date()
    else:
        report_date = (datetime.now(MSK) - timedelta(days=1)).date()
    return report_date.isoformat()

@job(
    name="wb_www_fin_report_job",
    resource_defs={
        "postgres": postgres_resource,
        "secret_manager": secret_manager,
        "selenium_remote": selenium_remote,
    },
    executor_def=in_process_executor,
)
def wb_www_fin_report_job():
    # 1) готовим batch и дату
    batch_id    = get_batch_id()
    report_date = get_report_date()

    # 2) собираем метаданные по всем профилям — динамический вывод
    report_metas = get_report_ids(report_date)

    # 3) для каждого report_meta сначала сохраняем ZIP в бронзу
    written = report_metas.map(write_fin_report_zip)

    # 4) а потом — читаем его из бронзы и распаковываем в DataFrame
    dfs = written.map(unpack_and_load_excel)

    # 5) и нарезаем этот DataFrame на пять «силвер»-таблиц
    dfs.map(load_wb_sales)
    dfs.map(load_wb_logistics)
    dfs.map(load_wb_adjustments_by_product)
    dfs.map(load_wb_adjustments_general)

