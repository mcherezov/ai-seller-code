from dagster import define_asset_job
from dagster_conf.pipelines.cards_list_assets import bronze_wb_cards_list_1d, silver_wb_mp_skus_1d


wb_cards_list_1d_job = define_asset_job(
    name="wb_cards_list_1d_job",
    selection=[
        bronze_wb_cards_list_1d,
        silver_wb_mp_skus_1d,
    ],
)