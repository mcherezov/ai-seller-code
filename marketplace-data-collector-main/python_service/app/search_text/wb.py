import logging
import time
import uuid
import requests
import pandas as pd
from datetime import datetime, date as date_type
from typing import List, Union

logger = logging.getLogger(__name__)


def _fetch_data(api_key, date, nm_ids):
    url = "https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts"
    formatted_date = date.strftime("%Y-%m-%d")
    
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    batch_size = 50
    index = 0

    all_data = []
    while index < len(nm_ids):
        batch = nm_ids[index:index + batch_size]
        params = {
            "currentPeriod": {"start": formatted_date, "end": formatted_date},
            "nmIds": batch,
            "topOrderBy": "openToCart",
            "orderBy": {"field": "avgPosition", "mode": "desc"},
            "limit": 30
        }
        
        response = requests.post(url, headers=headers, json=params)
        
        if response.status_code == 200:
            data = response.json().get("data", {}).get("items", [])
            if data:
                all_data.extend(data)
            index += batch_size  # Переходим к следующей группе
        
        elif response.status_code == 429:
            logger.debug(f"Получен 429 Too Many Requests для группы {batch}, ждем 60 секунд...")
            time.sleep(60)
            continue  # Повторяем попытку с тем же batch
        
        elif response.status_code == 500:
            logger.error(f"Получен 500 Internal Server Error для группы {batch}, переходим к одиночным запросам...")
            _process_individually(batch, url, headers, formatted_date, all_data)
            index += batch_size  # Переходим к следующей группе
        
        else:
            logger.error(f"Ошибка запроса {response.status_code}: {response.text}")
            index += batch_size  # Пропускаем эту группу
        
        time.sleep(20)  # Пауза между запросами
    return all_data


def _process_individually(nm_ids, url, headers, formatted_date, all_data):
    for nm_id in nm_ids:
        time.sleep(20)  # Пауза между запросами
        while True:
            params = {
                "currentPeriod": {"start": formatted_date, "end": formatted_date},
                "nmIds": [nm_id],
                "topOrderBy": "openToCart",
                "orderBy": {"field": "avgPosition", "mode": "asc"},
                "limit": 30
            }
            response = requests.post(url, headers=headers, json=params)
            
            if response.status_code == 200:
                data = response.json().get("data", {}).get("items", [])
                if data:
                    all_data.extend(data)
                break  # Успешный запрос
            
            elif response.status_code == 429:
                logger.debug(f"Получен 429 Too Many Requests для nm_id {nm_id}, ждем 60 секунд...")
                time.sleep(60)
            
            elif response.status_code == 500:
                logger.error(f"Ошибка 500 для nm_id {nm_id}, прекращаем попытки.")
                break  # Прекращаем попытки для этого nm_id
            
            else:
                logger.error(f"Ошибка запроса {response.status_code}: {response.text}")
                break  # Прерываем попытки для этого nm_id


def fetch_wb_search_stats(api_key: str, date: Union[datetime, date_type], nm_ids: List[int]) -> pd.DataFrame:
    """
    Получает статистику поисковых запросов за указанный день для списка артикулов,
    запрашивая данные по каждому nm_id отдельно с паузами и повторными попытками при ошибке 429.
    
    :param api_key: API ключ Wildberries INTER
    :param date: Дата для запроса статистики
    :param nm_ids: Список артикулов (числовые значения)
    :return: DataFrame с объединенными результатами
    """
    all_data = _fetch_data(api_key, date, nm_ids)
    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data, columns=[
        "nmId", "text", "frequency", "avgPosition", "openCard", "addToCart", "orders"
    ])
    columns_to_extract = ['frequency', 'avgPosition', 'openCard', 'addToCart', 'orders']
    df[columns_to_extract] = df[columns_to_extract].applymap(lambda x: x.get('current', None))

    df["ctrToCard"] = df.addToCart / df.frequency
    df["ctrToOrder"] = df.orders / df.frequency
    df["date"] = date
    df['uuid'] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df
