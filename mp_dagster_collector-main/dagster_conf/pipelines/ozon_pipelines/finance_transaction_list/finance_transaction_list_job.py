from dagster import define_asset_job, AssetSelection, AssetKey

BRONZE_OZON_FINANCE_TRANSACTION_LIST_1D = AssetKey(["bronze", "ozon_finance_transaction_list_1d"])

ozon_finance_transaction_list_1d_job = define_asset_job(
    name="ozon_finance_transaction_list_1d_job",
    selection=AssetSelection.keys(BRONZE_OZON_FINANCE_TRANSACTION_LIST_1D),
    description="Материализует bronze/ozon_finance_transaction_list_1d (POST /v3/finance/transaction/list).",
)
