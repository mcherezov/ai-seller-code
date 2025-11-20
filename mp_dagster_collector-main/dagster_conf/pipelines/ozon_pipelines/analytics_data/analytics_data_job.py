from dagster import define_asset_job, AssetSelection, AssetKey


BRONZE_OZON_ANALYTICS_DATA_1D = AssetKey(["bronze", "ozon_analytics_data_1d"])

ozon_analytics_data_1d_job = define_asset_job(
    name="ozon_analytics_data_1d_job",
    selection=AssetSelection.keys(BRONZE_OZON_ANALYTICS_DATA_1D),
    description=(
        "Материализует bronze/ozon_analytics_data_1d: сохраняет сырые ответы "
        "Ozon /v1/analytics/data (воронка продаж) по принципу 1 запрос = 1 строка."
    ),
)
