from dagster import define_asset_job, AssetSelection, AssetKey

BRONZE_OZON_PRODUCT_INFO_PRICES_1D = AssetKey(["bronze", "ozon_product_info_prices_1d"])

ozon_product_info_prices_1d_job = define_asset_job(
    name="ozon_product_info_prices_1d_job",
    selection=AssetSelection.keys(BRONZE_OZON_PRODUCT_INFO_PRICES_1D),
    description="Материализует bronze/ozon_product_info_prices_1d (POST /v5/product/info/prices, 1 страница = 1 строка).",
)
