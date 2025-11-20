import time
import logging
import pandas as pd
from datetime import date
from api_clients.wb import WildberriesAPI


def fetch_paid_storage_data(config: dict, entities_meta, date_from: date, date_to: date) -> pd.DataFrame:
    """
    Получить отчет по платному хранению для всех юрлиц за указанный период
    """
    sellers = list(config["wb_marketplace_keys"].keys())
    dfs = []

    date_from_str = date_from.strftime("%Y-%m-%d")
    date_to_str = date_to.strftime("%Y-%m-%d")

    for seller_legal in sellers:
        wb = WildberriesAPI(seller_legal)

        # 1. Запуск задачи
        data = wb.get_paid_storage_report(date_from=date_from_str, date_to=date_to_str)
        task_id = data.get("data", {}).get("taskId")

        if not task_id:
            logging.debug(f"⚠️ Нет данных для {seller_legal}: WB не сформировал задачу (скорее всего, не было платного хранения в указанный период).")
            continue

        # 2. Ожидание
        for attempt in range(60):
            status = wb.get_paid_storage_status(task_id)
            if status.get("data", {}).get("status") == "done":
                break
            elif status.get("data", {}).get("status") == "error":
                logging.debug(f"❌ Ошибка генерации отчета для {seller_legal}, пропускаем.")
                task_id = None
                break
            time.sleep(5)

        if not task_id:
            continue

        # 3. Скачивание
        report = wb.get_paid_storage_data(task_id)
        if not isinstance(report, list):
            raise Exception(f"Некорректный формат данных от API для {seller_legal}: {report}")

        if not report:
            logging.info(f"⚠️ Нет данных для {seller_legal}: WB вернул пустой список, платного хранения не было.")
            continue

        # 4. Финальный датафрейм
        df = pd.json_normalize(report)

        df.rename(columns={
            "logWarehouseCoef": "log_warehouse_coef",
            "officeId": "office_id",
            "warehouseCoef": "warehouse_coef",
            "giId": "gi_id",
            "chrtId": "chrt_id",
            "vendorCode": "vendor_code",
            "nmId": "nm_id",
            "calcType": "calc_type",
            "warehousePrice": "warehouse_price",
            "barcodesCount": "barcodes_count",
            "palletPlaceCode": "pallet_place_code",
            "palletCount": "pallet_count",
            "originalDate": "original_date",
            "loyaltyDiscount": "loyalty_discount",
            "tariffFixDate": "tariff_fix_date",
            "tariffLowerDate": "tariff_lower_date",
            "date": "report_date"
        }, inplace=True)

        df["date"] = pd.to_datetime(date.today())
        meta = entities_meta.get(seller_legal)
        if meta:
            df["legal_entity"] = meta["display_name"]
            # df["entity_id"]    = meta["id"]
        else:
            df["legal_entity"] = seller_legal
            # df["entity_id"]    = None

        int_cols = [
            "log_warehouse_coef", "office_id", "gi_id", "chrt_id", "nm_id",
            "barcodes_count", "pallet_place_code", "pallet_count"
        ]

        float_cols = [
            "warehouse_coef", "volume", "warehouse_price", "loyalty_discount"
        ]

        text_cols = [
            "warehouse", "size", "barcode", "subject", "brand",
            "vendor_code", "calc_type"
        ]

        date_cols = [
            "report_date", "original_date", "tariff_fix_date", "tariff_lower_date", "date"
        ]

        df[int_cols] = df[int_cols].fillna(0).astype(int)
        df[float_cols] = df[float_cols].fillna(0.0)
        df[text_cols] = df[text_cols].fillna("")

        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce")  # → NaT, если невалидная дата
            df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
            df[col] = df[col].astype("object")

        df = df.where(pd.notnull(df), None)
        dfs.append(df)

    if not dfs:
        raise Exception("Нет данных ни для одного продавца за указанный период")

    return pd.concat(dfs, ignore_index=True)
