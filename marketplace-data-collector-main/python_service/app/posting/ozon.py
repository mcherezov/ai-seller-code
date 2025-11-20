from datetime import datetime, date as date_type
from typing import Dict, Union
import uuid

import numpy as np
import pandas as pd
import requests

from network import request_with_retries


def _prepare_df(data):
    df = pd.DataFrame(data)
    df['sku'] = df.products.apply(lambda products: [product['sku'] for product in products])
    df['name'] = df.products.apply(lambda products: [product['name'] for product in products])
    df['offer_id'] = df.products.apply(lambda products: [product['offer_id'] for product in products])
    df['warehouse_name'] = df.analytics_data.apply(lambda x: x['warehouse_name'])
    df['quantity'] = df.products.apply(lambda products: [product['quantity'] for product in products])
    df['price'] = df.products.apply(lambda products: [product['price'] for product in products])
    df['cluster_from'] = df.financial_data.apply(lambda x: x['cluster_from'])
    columns = ['sku', 'name', 'offer_id', 'warehouse_name', 'created_at', 'status', 'quantity', 'price', 'cluster_from']
    df = df[columns]
    return df


def fetch_ozon_posting_data(config: Dict[str, Dict[str, str]], date: Union[datetime, date_type]):
    """
    Получает cписок отправлений API Ozon.

    :param config: Словарь для каждого seller_legal, внутри которого пары ключ-значение с api_key и client_id.
    """
    formatted_date_from = date.strftime("%Y-%m-%dT00:00:00+03:00")
    formatted_date_to = date.strftime("%Y-%m-%dT23:59:59+03:00")

    dfs = []
    for seller_legal, data in config.items():
        log_prefix = f"Остатки ozon. {seller_legal}. "
        err_prefix = f"\033[91mWARNING: {log_prefix}\033[0m"

        # Получаем API-ключ и client_id
        api_key = data['api_key']
        client_id = data['client_id']

        headers = {
            "Client-Id": client_id,
            "Api-Key": api_key
        }

        # Выгружаем данные об остатках с использованием API
        offset = 0

        while True:
            payload = {
                "dir": "ASC",
                "filter": {
                    "since": formatted_date_from,
                    "status": "",
                    "to": formatted_date_to
                },
                "limit": 1000,
                "offset": offset,
                "translit": True,
                "with": {
                    "analytics_data": True,
                    "financial_data": True
                }
            }

            stocks_response = request_with_retries(
                requests.post, url="https://api-seller.ozon.ru/v2/posting/fbo/list", json=payload, headers=headers, err_prefix=err_prefix
            )
            if stocks_response.status_code != 200:
                raise Exception(f"{err_prefix}Ошибка API: {stocks_response.status_code}, {stocks_response.text}")

            # Парсим данные из ответа
            data = stocks_response.json().get("result")

            # Если данных нет, или `next` равен None, выходим из цикла
            if not data:
                #print("Данные закончились или отсутствуют.")
                break

            df = _prepare_df(data)

            if seller_legal == "inter":
                df["legal_entity"] = "ИНТЕР"
            elif seller_legal == "ut":
                df["legal_entity"] = "АТ"
            else:
                raise Exception(f"Необработанный seller_legal={seller_legal}")
    
            dfs.append(df)

            # Обновляем offset для следующего запроса
            offset += 1000

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

    final_data['uuid'] = [str(uuid.uuid4()) for _ in range(len(final_data))]

    # Дополнительно убеждаемся, что больше нет NaN значений
    assert not final_data.isnull().values.any(), "DataFrame содержит NaN значения после обработки."

    return final_data
