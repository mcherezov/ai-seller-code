from dagster import define_asset_job
from dagster_conf.pipelines.auto_stat_words_assets import bronze_wb_adv_auto_stat_words_1d, silver_wb_adv_keyword_clusters_1d


wb_adv_auto_stat_words_1d_job = define_asset_job(
    name="wb_adv_auto_stat_words_1d_job",
    selection=[
        bronze_wb_adv_auto_stat_words_1d,
        silver_wb_adv_keyword_clusters_1d,
    ],
)