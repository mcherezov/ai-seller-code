from dagster import define_asset_job
from dagster_conf.pipelines.wb_search_results_6h_assets import bronze_wb_search_results_6h, silver_wb_search_results_6h


wb_search_results_6h_job = define_asset_job(
    name="wb_search_results_6h_job",
    selection=[
        bronze_wb_search_results_6h,
        silver_wb_search_results_6h,
    ],
)