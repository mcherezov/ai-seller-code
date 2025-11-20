from dagster import define_asset_job

from dagster_conf.pipelines.fullstats_assets import (
    bronze_wb_adv_fullstats,
    silver_wb_adv_fullstats,
)

wb_adv_fullstats_1d_job = define_asset_job(
    name="wb_adv_fullstats_1d_job",
    selection=[
        bronze_wb_adv_fullstats,
        silver_wb_adv_fullstats,
    ],
)

wb_adv_fullstats_1h_job = define_asset_job(
    name="wb_adv_fullstats_1h_job",
    selection=[
        bronze_wb_adv_fullstats,
        silver_wb_adv_fullstats,
    ],
)
