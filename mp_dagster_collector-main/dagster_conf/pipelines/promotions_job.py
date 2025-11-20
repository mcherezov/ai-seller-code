from dagster import define_asset_job

from dagster_conf.pipelines.promotions_assets import (
    bronze_wb_adv_promotions_1h,
    silver_adv_promotions_1h,
)

wb_adv_promotions_1h_job = define_asset_job(
    name="wb_adv_promotions_1h_job",
    selection=[
        bronze_wb_adv_promotions_1h,
        silver_adv_promotions_1h,
    ],
)