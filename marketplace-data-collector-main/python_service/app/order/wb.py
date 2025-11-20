from time import sleep
from typing import Dict, Union
import uuid
import requests
import pandas as pd

from network import request_with_retries
from datetime import datetime, timedelta, date as date_type


def fetch_wb_orders_data(marketplace_keys: Dict[str, str], entities_meta, date: Union[datetime, date_type]) -> pd.DataFrame:
    """
    Получает данные по остаткам через API Wildberries.

    :param marketplace_keys: Пары ключ-значение с seller_legal и api_key.
    """
    try:
        formatted_date = date.strftime("%Y-%m-%d")
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    # Настройка URL и параметров API
    base_url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    params = {
        "dateFrom": formatted_date,
        "dateTo": formatted_date,
        "flag": 0
    }

    df_list = []
    for seller_legal, api_key in marketplace_keys.items():
        log_prefix = f"Заказы wb. {seller_legal}. "
        err_prefix = f"\033[91mWARNING: {log_prefix}\033[0m"
        headers = {
            "Authorization": api_key
        }
        response = request_with_retries(
            requests.get, url=base_url, headers=headers, params=params, err_prefix=err_prefix
        )
        if response.status_code != 200:
            raise Exception(f"{err_prefix}Ошибка при создании отчета: {response.status_code}, {response.text}")

        data = response.json()
        df = pd.DataFrame(data)
        if df.empty:
            continue
        if isinstance(date, datetime):
            date_from_dt = datetime.combine(date.date(), datetime.min.time())
        elif isinstance(date, date_type):
            date_from_dt = datetime.combine(date, datetime.min.time())
        date_to_dt = date_from_dt + timedelta(days=1) - timedelta(seconds=1)

        # Преобразуем столбец "date" в формат datetime для фильтрации
        df['date_datetime'] = pd.to_datetime(df['date'], format="%Y-%m-%dT%H:%M:%S")

        # Фильтруем строки, оставляя только те, которые попадают в диапазон
        df = df[(df['date_datetime'] >= date_from_dt) & (df['date_datetime'] <= date_to_dt)]

        # Удаляем временный столбец "date_datetime", чтобы вернуть данные в исходном формате
        df.drop(columns=['date_datetime'], inplace=True)

        meta = entities_meta.get(seller_legal)
        if meta:
            df['legal_entity'] = meta['display_name']
            # df['entity_id']    = meta['id']
        else:
            df['legal_entity'] = seller_legal
            # df['entity_id']    = None

        if not df.empty:
            if 'orderType' not in df.columns:
                df['orderType'] = 'unknown'
            df_list.append(df)

    if not df_list:
        return None
    final_df = pd.concat(df_list, ignore_index=True)
    final_df = final_df.drop_duplicates()

    final_df['uuid'] = [str(uuid.uuid4()) for _ in range(len(final_df))]

    return final_df
