from dagster import define_asset_job
from dagster_conf.pipelines.stats_keywords_assets import bronze_wb_adv_stats_keywords, silver_wb_adv_keyword_stats


wb_adv_stats_keywords_1d_job = define_asset_job(
    name="wb_adv_stats_keywords_1d_job",
    selection=[
        bronze_wb_adv_stats_keywords,
        silver_wb_adv_keyword_stats,
    ],
)


wb_adv_stats_keywords_1h_job = define_asset_job(
    name="wb_adv_stats_keywords_1h_job",
    selection=[
        bronze_wb_adv_stats_keywords,
        silver_wb_adv_keyword_stats,
    ],
)
