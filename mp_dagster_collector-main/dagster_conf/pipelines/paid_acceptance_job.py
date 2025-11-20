from dagster import define_asset_job
from dagster_conf.pipelines.paid_acceptance_assets import bronze_wb_paid_acceptance_1d, silver_wb_paid_acceptances_1d


wb_acceptance_report_1d_job = define_asset_job(
    name="wb_acceptance_report_1d_job",
    selection=[
        bronze_wb_paid_acceptance_1d,
        silver_wb_paid_acceptances_1d,
    ],
)
