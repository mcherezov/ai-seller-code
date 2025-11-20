from dagster import define_asset_job, AssetSelection, AssetKey

BRONZE_OZON_REPORT_POSTINGS_FILE_1D = AssetKey(["bronze", "ozon_report_postings_file_1d"])

ozon_report_postings_file_1d_job = define_asset_job(
    name="ozon_report_postings_file_1d_job",
    selection=AssetSelection.keys(BRONZE_OZON_REPORT_POSTINGS_FILE_1D),
    description=(
        "Материализует bronze/ozon_report_postings_file_1d: создаёт отчёт заказов, "
        "ждёт готовности и скачивает XLSX; в бронзу пишется финальный запрос скачивания."
    ),
)
