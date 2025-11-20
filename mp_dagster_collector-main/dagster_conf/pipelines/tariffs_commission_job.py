from dagster import define_asset_job
from dagster_conf.pipelines.tariffs_commission_assets import bronze_wb_commission_1d, silver_wb_commission_1d


wb_tariffs_commission_1d_job = define_asset_job(
    name="wb_tariffs_commission_1d_job",
    selection=[
        bronze_wb_commission_1d,
        silver_wb_commission_1d,
    ],
)