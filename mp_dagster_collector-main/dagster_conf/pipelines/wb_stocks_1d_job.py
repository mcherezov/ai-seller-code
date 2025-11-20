from dagster import define_asset_job
from dagster_conf.pipelines.warehouse_remains_assets import bronze_wb_stocks_report_1d, silver_wb_stocks_1d


wb_stocks_1d_job = define_asset_job(
    name="wb_stocks_1d_job",
    selection=[
        bronze_wb_stocks_report_1d,
        silver_wb_stocks_1d,
    ],
)
