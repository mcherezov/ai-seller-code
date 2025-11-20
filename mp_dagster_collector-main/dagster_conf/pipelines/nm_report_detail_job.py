from dagster import define_asset_job
from dagster_conf.pipelines.nm_report_detail_assets import bronze_wb_nm_report_detail_1d, silver_wb_buyouts_percent_1d, silver_wb_sales_funnels_1d


wb_nm_report_detail_1d_job = define_asset_job(
    name="wb_nm_report_detail_1d_job",
    selection=[
        bronze_wb_nm_report_detail_1d,
        silver_wb_buyouts_percent_1d,
        silver_wb_sales_funnels_1d,
    ],
)