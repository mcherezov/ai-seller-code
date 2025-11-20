from dagster import define_asset_job
from dagster_conf.pipelines.www_text_search_assets import bronze_wb_www_text_search_1d, silver_wb_www_text_search_1d


wb_text_search_1d_job = define_asset_job(
    name="wb_text_search_1d_job",
    selection=[
        bronze_wb_www_text_search_1d,
        silver_wb_www_text_search_1d,
    ],
)