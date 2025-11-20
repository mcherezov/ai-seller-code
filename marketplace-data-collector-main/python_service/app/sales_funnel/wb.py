from datetime import datetime, date as date_type
import logging
import time
import json
from typing import Dict, Union

import numpy as np
import pandas as pd
import requests


def _flatten_data(data):
    """
    Преобразует вложенные структуры JSON в плоскую структуру.
    :param data: Входной JSON-объект (словарь или список).
    :return: Плоский DataFrame.
    """
    return pd.json_normalize(data, sep="_")


def fetch_wb_sales_funnel(config: Dict[str, Dict[str, str]], entities_meta, date: Union[datetime, date_type], hour: int):
    """
    Получает данные по воронке продаж через API Wildberries.

    :param config: Словарь для каждого seller_legal, внутри которого пары ключ-значение с api_key и client_id.
    :param date: Дата в формате datetime.date.
    :param hour: Час, для которого нужно получить данные.
    """
    formatted_date_from = date.strftime(f"%Y-%m-%d {hour}:00:00")
    formatted_date_to = date.strftime(f"%Y-%m-%d {hour}:59:59")

    dfs = []
    for seller_legal, api_key in config.items():
        log_prefix = f"Воронка wb. {seller_legal}. "

        base_url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"
        payload = {
            "timezone": "Europe/Moscow",
            "period": {
                "begin": formatted_date_from,
                "end": formatted_date_to
            },
            "page": 1
        }

        headers = {
            "Authorization": api_key,
            "Content-Type": "application/json"
        }

        all_data = []

        while True:
            retry_attempts = 5
            for attempt in range(retry_attempts):
                try:
                    logging.debug(f"{log_prefix}Отправка запроса: {base_url}")
                    logging.debug(f"{log_prefix}Payload: {json.dumps(payload, ensure_ascii=False)}")
                    logging.debug(f"{log_prefix}Headers: {headers}")

                    response = requests.post(base_url, json=payload, headers=headers)
                    logging.debug(f"{log_prefix}HTTP {response.status_code}. Ответ: {response.text}")

                    if response.status_code == 200:
                        break
                    elif response.status_code in (429, 500, 503):
                        wait = 60 * (attempt + 1)
                        logging.warning(
                            f"{log_prefix}Код {response.status_code}. Попытка {attempt + 1}/{retry_attempts}. Ждём {wait} сек...")
                        time.sleep(wait)

                    else:
                        raise Exception(f"{log_prefix}Нестандартный ответ: {response.status_code}, {response.text}")
                except requests.exceptions.RequestException as e:
                    logging.error(f"{log_prefix}Ошибка сети: {e}. Попытка {attempt + 1}/{retry_attempts}")
                    time.sleep(2 ** attempt)
            else:
                raise Exception(
                    f"{log_prefix}Не удалось получить данные после {retry_attempts} попыток. "
                    f"Последний статус: {response.status_code}, ответ: {response.text}"
                )
            data = response.json()
            if data.get("error", False):
                raise Exception(f"Ошибка API: {data.get('errorText', 'Неизвестная ошибка')}")

            page_data = data.get("data", {}).get("cards", [])
            if not page_data:
                break

            for data in page_data:
                if 'previousPeriod' in data['statistics']:
                    del data['statistics']['previousPeriod']

                if 'periodComparison' in data['statistics']:
                    del data['statistics']['periodComparison']

            all_data.extend(page_data)
            if not data.get("data", {}).get("isNextPage", False):
                break

            payload["page"] += 1

        if not all_data:
            logging.info("Нет данных для выбранного периода.")
            continue

        # Разворачиваем вложенные структуры
        df = _flatten_data(all_data)

        if df.empty:
            logging.info("Нет данных для выбранного периода.")
            continue

        meta = entities_meta.get(seller_legal)
        if meta:
            df["legal_entity"] = meta["display_name"]
            # df["entity_id"]    = meta["id"]
        else:
            df["legal_entity"] = seller_legal
            # df["entity_id"]    = None

        dfs.append(df)

    if not dfs:
        raise Exception("Не удалось получить данные об остатках. Проверьте запросы и входные данные.")

    # Преобразование данных в DataFrame
    final_data = pd.concat(dfs)

    final_data = final_data.drop(['statistics_selectedPeriod_begin', 'statistics_selectedPeriod_end'], axis=1)

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
