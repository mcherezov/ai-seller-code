import requests
import uuid
from datetime import date
import pandas as pd
import numpy as np
from typing import Dict, List
import logging


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def fetch_ozon_data(sku_dict: Dict[str, List[str]], creds: dict):
    today = date.today().isoformat()
    all_data = []

    # соответствие "ИНТЕР" -> "inter" и т.п.
    reverse_map = {
        "ИНТЕР": "inter",
        "АТ": "ut"
    }

    for legal_entity, sku_list in sku_dict.items():
        if not sku_list:
            logging.warning(f"⚠️ Пропущено {legal_entity}: список SKU пуст")
            continue

        api_key = reverse_map.get(legal_entity)
        if not api_key or api_key not in creds:
            logging.warning(f"⚠️ Нет доступа к API для юрлица {legal_entity}")
            continue

        headers = {
            "Client-Id": creds[api_key]["client_id"],
            "Api-Key": creds[api_key]["api_key"]
        }

        # Приведение всех SKU к строкам
        sku_list = [str(sku) for sku in sku_list]

        for chunk in chunk_list(sku_list, 1000):
            payload = {
                "skus": chunk,
                "warehouse_ids": []
            }

            response = requests.post(
                "https://api-seller.ozon.ru/v1/analytics/stocks",
                headers=headers,
                json=payload
            )

            if response.status_code != 200:
                logging.error(f"❌ Ошибка ответа {legal_entity}: {response.status_code}")
                logging.error(response.text)
                continue

            items = response.json().get("items", [])
            for item in items:
                item["legal_entity"] = legal_entity
                item["date"] = today
                item["uuid"] = str(uuid.uuid4())
                all_data.append(item)

    if not all_data:
        raise Exception("Нет данных для загрузки")

    df = pd.DataFrame(all_data)

    if "valid_stock_count" in df.columns and "available_stock_count" in df.columns:
        df = df.drop(columns=["valid_stock_count"])

    df = df.rename(columns={
        "sku": "sku",
        "product_name": "name",
        "offer_id": "offer_id",
        "warehouse_name": "warehouse_name",
        "available_stock_count": "valid_stock_count",
        "waiting_docs_stock_count": "waitingdocs_stock_count",
        "expiring_stock_count": "expiring_stock_count",
        "stock_defect_stock_count": "defect_stock_count"
    })

    expected_columns = [
        "sku", "name", "offer_id", "date", "warehouse_name",
        "valid_stock_count", "waitingdocs_stock_count",
        "expiring_stock_count", "defect_stock_count",
        "legal_entity", "uuid"
    ]

    for col in expected_columns:
        if col not in df.columns:
            df[col] = "" if col in ["name", "offer_id", "warehouse_name", "legal_entity"] else 0

    df = df[expected_columns]
    df = df.replace([np.inf, -np.inf], np.nan).fillna("")
    df = df.drop_duplicates()

    return df
