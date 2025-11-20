from dagster import define_asset_job, AssetSelection, AssetKey

BRONZE_OZON_ADV_EXPENSE_1D = AssetKey(["bronze", "ozon_adv_expense_1d"])

ozon_adv_expense_1d_job = define_asset_job(
    name="ozon_adv_expense_1d_job",
    selection=AssetSelection.keys(BRONZE_OZON_ADV_EXPENSE_1D),
    description="Материализует bronze/ozon_adv_expense_1d (CSV расходов Ozon Performance в BYTEA).",
)
