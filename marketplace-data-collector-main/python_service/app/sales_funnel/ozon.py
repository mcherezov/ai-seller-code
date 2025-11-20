from datetime import datetime, date as date_type
import logging
import time
from typing import Dict, Union

import numpy as np
import pandas as pd
import requests


def fetch_ozon_sales_funnel(config: Dict[str, Dict[str, str]], date: Union[datetime, date_type], hour: int):
    """
    Получает данные по воронке продаж через API Ozon Analytics.

    :param config: Словарь для каждого seller_legal, внутри которого пары ключ-значение с api_key и client_id.
    :param date: Дата в формате datetime.date.
    :param hour: Час, для которого нужно получить данные.
    """
    formatted_date_from = date.strftime(f"%Y-%m-%dT{hour}:00:00+03:00")
    formatted_date_to = date.strftime(f"%Y-%m-%dT{hour}:59:59+03:00")

    dfs = []
    for seller_legal, data in config.items():
        log_prefix = f"Воронка ozon. {seller_legal}. "

        # Получаем API-ключ и client_id
        api_key = data['api_key']
        client_id = data['client_id']

        # URL для получения данных
        analytics_url = "https://api-seller.ozon.ru/v1/analytics/data"
        analytics_headers = {
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json"
        }

        # Начальные параметры запроса
        limit = 1000
        offset = 0


        combined_rows = []

        while True:
            analytics_payload = {
                "date_from": formatted_date_from,
                "date_to": formatted_date_to,
                "metrics": ["session_view_pdp", "hits_tocart", "position_category", "delivered_units", "returns"],
                "dimension": ["sku"],
                "filters": [],
                "sort": [],
                "limit": limit,
                "offset": offset
            }

            retry_attempts = 5
            for attempt in range(retry_attempts):
                analytics_response = requests.post(analytics_url, json=analytics_payload, headers=analytics_headers)

                if analytics_response.status_code == 200:
                    break
                elif analytics_response.status_code == 429:
                    logging.error(f"{log_prefix}Превышен лимит запросов. Попытка {attempt + 1} из {retry_attempts}. Ждём 60 секунд...")
                    time.sleep(60)  # Ожидание 60 секунд
                else:
                    raise Exception(f"{log_prefix}Ошибка получения данных по воронке продаж: {analytics_response.status_code}, {analytics_response.text}")
            else:
                raise Exception(f"{log_prefix}Не удалось получить данные после нескольких попыток.")

            response_json = analytics_response.json()
            result_data = response_json.get("result", {}).get("data", [])

            # Если данных нет, прекращаем сбор
            if not result_data:
                break

            # Преобразуем полученные данные в строки для DataFrame
            rows = [
                {
                    "sku": item["dimensions"][0]["id"],
                    "name": item["dimensions"][0]["name"],
                    "session_view_pdp": item["metrics"][0],
                    "hits_tocart": item["metrics"][1],
                    "position_category": item["metrics"][2],
                    "delivered_units": item["metrics"][3],
                    "returns": item["metrics"][4],
                }
                for item in result_data
            ]
            combined_rows.extend(rows)

            # Обновляем offset для следующего запроса
            offset += 1000
        df = pd.DataFrame(combined_rows)
        df['legal_entity'] = seller_legal
        dfs.append(df)

    if not dfs:
        raise Exception("Не удалось получить данные об остатках. Проверьте запросы и входные данные.")

    # Преобразование данных в DataFrame
    final_data = pd.concat(dfs)

    if final_data.empty:
        raise Exception("Полученный DataFrame пуст. Проверьте входные данные и API-ответ.")

    # Преобразование NaN и недопустимых значений для JSON совместимости
    final_data = final_data.replace([np.inf, -np.inf], np.nan)

    # Разделяем столбцы по типу данных
    numeric_cols = final_data.select_dtypes(include=[np.number]).columns
    non_numeric_cols = final_data.select_dtypes(exclude=[np.number]).columns

    # Для числовых столбцов заменяем NaN на 0
    final_data[numeric_cols] = final_data[numeric_cols].fillna(0)

    # Для нечисловых столбцов заменяем NaN на ""
    final_data[non_numeric_cols] = final_data[non_numeric_cols].fillna("")

    # Дополнительно убеждаемся, что больше нет NaN значений
    assert not final_data.isnull().values.any(), "DataFrame содержит NaN значения после обработки."

    return final_data
