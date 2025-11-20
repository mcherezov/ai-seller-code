from dagster import define_asset_job
from dagster_conf.pipelines.supplier_incomes_assets import bronze_wb_supplier_incomes_1d, silver_wb_supplies_1d


wb_supplier_incomes_1d_job = define_asset_job(
    name="wb_supplier_incomes_1d_job",
    selection=[
        bronze_wb_supplier_incomes_1d,
        silver_wb_supplies_1d,
    ],
)