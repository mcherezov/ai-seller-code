from dagster import define_asset_job

from dagster_conf.pipelines.promotion_adverts_assets import (
    bronze_wb_adv_promotion_adverts,
    silver_wb_adv_campaigns,
    silver_wb_adv_product_rates,
)


wb_adv_promotion_adverts_1d_job = define_asset_job(
    name="wb_adv_promotion_adverts_1d_job",
    selection=[
        bronze_wb_adv_promotion_adverts,
        silver_wb_adv_campaigns,
        silver_wb_adv_product_rates,
    ],
)


wb_adv_promotion_adverts_1h_job = define_asset_job(
    name="wb_adv_promotion_adverts_1h_job",
    selection=[
        bronze_wb_adv_promotion_adverts,
        silver_wb_adv_campaigns,
        silver_wb_adv_product_rates,
    ],
)
