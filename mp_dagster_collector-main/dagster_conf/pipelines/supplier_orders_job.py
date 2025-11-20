from dagster import define_asset_job
from dagster_conf.pipelines.supplier_orders_assets import bronze_wb_supplier_orders_1d, silver_order_items_1d


wb_supplier_orders_1d_job = define_asset_job(
    name="wb_supplier_orders_1d_job",
    selection=[
        bronze_wb_supplier_orders_1d,
        silver_order_items_1d,
    ],
)