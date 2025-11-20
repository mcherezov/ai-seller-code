from dagster import define_asset_job
from dagster_conf.pipelines.paid_storage_assets import bronze_wb_paid_storage_1d, silver_paid_storage_1d


wb_paid_storage_1d_job = define_asset_job(
    name="wb_paid_storage_1d_job",
    selection=[
        bronze_wb_paid_storage_1d,
        silver_paid_storage_1d,
    ],
)
