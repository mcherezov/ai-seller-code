from dagster import job, Nothing, in_process_executor

from dagster_conf.pipelines.www_cashback_reports_ops import (
    get_cashback_download_meta,
    download_and_write_cashback_xlsx,
    upsert_cashback_silver_from_bronze
)

from dagster_conf.resources.pg_resource import postgres_resource
from dagster_conf.resources.selenium_remote import selenium_remote


@job(
    name="wb_www_cashback_job",
    resource_defs={
        "postgres": postgres_resource,
        "selenium_remote": selenium_remote,
    },
    executor_def=in_process_executor,
)
def wb_www_cashback_job():
    metas = get_cashback_download_meta()
    bronze_rows = metas.map(download_and_write_cashback_xlsx)
    barrier: Nothing = bronze_rows.collect()
    upsert_cashback_silver_from_bronze(barrier)
