#!/usr/bin/env python3
"""
Скрипт для ручной выгрузки отчета платного хранения Wildberries за переданную дату и загрузки в базу данных.
Если дата не указана, используется текущая дата.
Примеры вызова:
    python wb_history_manual.py 18.05.2025
    python wb_history_manual.py
"""
import sys
import os
# Поднимаемся на один уровень до директории python_service/app
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, root_dir)

import argparse
from datetime import datetime, date as _date
import time
import logging
import pandas as pd

# Импорты конфигурации и работы с БД
from config_loader import load_config
from db import records_presented_at, delete_previous_and_insert_new_postgres_table
# Импорт API-клиента WB
from api_clients.wb import WildberriesAPI


def fetch_paid_storage_data_inline(config: dict, entities_meta, date_from: _date, date_to: _date) -> pd.DataFrame:
    print("[DEBUG] Начало fetch_paid_storage_data")
    sellers = list(config.get("wb_marketplace_keys", {}).keys())
    print(f"[DEBUG] sellers: {sellers}")
    dfs = []

    date_from_str = date_from.strftime("%Y-%m-%d")
    date_to_str = date_to.strftime("%Y-%m-%d")
    print(f"[DEBUG] Даты: {date_from_str} - {date_to_str}")

    for seller_legal in sellers:
        print(f"[DEBUG] Обрабатываем seller: {seller_legal}")
        wb = WildberriesAPI(seller_legal)

        # 1. Запуск задачи
        print("[DEBUG] Запрос get_paid_storage_report")
        data = wb.get_paid_storage_report(date_from=date_from_str, date_to=date_to_str)
        task_id = data.get("data", {}).get("taskId")
        print(f"[DEBUG] task_id: {task_id}")

        if not task_id:
            print(f"⚠️ Нет данных для {seller_legal}")
            continue

        # 2. Ожидание
        for attempt in range(60):
            status = wb.get_paid_storage_status(task_id)
            state = status.get("data", {}).get("status")
            print(f"[DEBUG] Попытка {attempt}, статус: {state}")
            if state == "done":
                break
            elif state == "error":
                print(f"❌ Ошибка для {seller_legal}")
                task_id = None
                break
            time.sleep(5)

        if not task_id:
            continue

        # 3. Скачивание
        print("[DEBUG] Запрос get_paid_storage_data")
        report = wb.get_paid_storage_data(task_id)
        print(f"[DEBUG] Тип report: {type(report)}, len: {len(report) if isinstance(report, list) else 'N/A'}")

        if not isinstance(report, list):
            raise Exception(f"Некорректный формат данных от API для {seller_legal}: {report}")
        if not report:
            print(f"⚠️ Пустой список для {seller_legal}")
            continue

        # 4. Финальный датафрейм
        print("[DEBUG] Преобразуем в DataFrame")
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

        # добавляем текущую дату загрузки
        df['date'] = pd.to_datetime(_date.today())

        # добавляем имя юрлица
        meta = entities_meta.get(seller_legal)
        if meta:
            df['legal_entity'] = meta.get('display_name')
        else:
            df['legal_entity'] = seller_legal

        # Приведение типов
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
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
            df[col] = df[col].astype(object)

        # Заменяем NaN на None
        df = df.where(pd.notnull(df), None)

        dfs.append(df)

    if not dfs:
        raise Exception("Нет данных ни для одного продавца за указанный период")

    print("[DEBUG] fetch_paid_storage_data завершён")
    return pd.concat(dfs, ignore_index=True)


def main():
    parser = argparse.ArgumentParser(
        description="Выгрузка и загрузка в БД отчета платного хранения WB за указанный день"
    )
    parser.add_argument(
        "date",
        nargs="?",
        help="Дата в формате DD.MM.YYYY",
        default=None
    )
    args = parser.parse_args()

    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%d.%m.%Y").date()
        except ValueError as e:
            print(f"Неверный формат даты: {e}")
            return
    else:
        target_date = datetime.now().date()

    print(f"Выгрузка за {target_date}")

    config = load_config()
    entities_meta = config.get("entities_meta", {})

    try:
        df = fetch_paid_storage_data_inline(config, entities_meta, target_date, target_date)
    except Exception as e:
        print(f"Ошибка при получении данных: {e}")
        return

    table = 'wb_paid_storage'
    print("[DEBUG] Проверяем наличие записей в БД")
    if records_presented_at(table, target_date):
        print(f"{table} за {target_date} уже есть")
        return

    print("[DEBUG] Проверяем, что DataFrame не пустой")
    if df.empty:
        print(f"{table}: нет данных за {target_date}")
        return

    print("[DEBUG] Загружаем в БД")
    try:
        delete_previous_and_insert_new_postgres_table(df, table)
        print(f"{table} за {target_date} загружен")
    except Exception as e:
        print(f"Ошибка при загрузке в БД: {e}")


if __name__ == "__main__":
    main()
