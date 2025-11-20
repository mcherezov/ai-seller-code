from datetime import datetime, date as date_type
import logging
import time
from typing import Dict, Union

import numpy as np
import pandas as pd
import requests


def _flatten_and_filter_data(data):
    """
    Преобразует данные fullstats в плоский формат и оставляет только нужные столбцы.

    :param data: Входные данные fullstats.
    :return: Список словарей с отфильтрованными полями.
    """
    result = []
    for item in data:
        advert_id = item.get("advertId")
        for day in item.get("days", []):
            day_date = day.get("date")
            for app in day.get("apps", []):
                app_type = app.get("appType")
                avg_sum = app.get("sum", 0) / max(len(app.get("nm", [])), 1)  # Среднее значение суммы
                for nm in app.get("nm", []):
                    result.append({
                        "nm_id": nm.get("nmId"),
                        "advert_id": advert_id,
                        "sum": avg_sum if app_type == 0 else nm.get("sum"),
                        "app_type": app_type,
                        "date": day_date,
                        "views": nm.get("views", 0),
                        "clicks": nm.get("clicks", 0),
                        "atbs": nm.get("atbs", 0),
                        "orders": nm.get("orders", 0),
                        "ctr": nm.get("ctr", 0),
                    })
    return result


def _chunk_list(lst, chunk_size):
    """
    Разделяет список на подсписки заданного размера.

    :param lst: Исходный список.
    :param chunk_size: Размер подсписка.
    :return: Генератор подсписков.
    """
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def fetch_wb_ad_campaign(config: Dict[str, Dict[str, str]],
                         entities_meta: Dict[str, Dict[str, object]],
                         date: Union[datetime, date_type]):
    """
    Получает данные по рекламным кампаниям через API Wildberries.

    :param config: Словарь для каждого seller_legal, внутри которого пары ключ-значение с api_key и client_id.
    :param date: Дата в формате datetime.date.
    """
    formatted_date_from = date.strftime(f"%Y-%m-%d")
    formatted_date_to = date.strftime(f"%Y-%m-%d")

    # Настройка API
    url_promotion_count = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
    url_fullstats = "https://advert-api.wildberries.ru/adv/v2/fullstats"

    dfs = []
    for seller_legal, api_key in config.items():
        log_prefix = f"Рекламные кампании wb. {seller_legal}. {formatted_date_from} "

        headers = {
            "Authorization": api_key
        }

        retry_attempts = 5
        for attempt in range(retry_attempts):
            response_promotion_count = requests.get(
                url_promotion_count,
                params={"dateFrom": formatted_date_from, "dateTo": formatted_date_to},
                headers=headers
            )
            if response_promotion_count.status_code == 200:
                break
            elif response_promotion_count.status_code == 429:
                logging.error(f"{log_prefix}Превышен лимит запросов. Попытка {attempt + 1} из {retry_attempts}. Ждём 60 секунд...")
                time.sleep(60)  # Ожидание 60 секунд
            else:
                raise Exception(f"{log_prefix}Ошибка получения данных по воронке продаж: {response_promotion_count.status_code}, {response_promotion_count.text}")
        else:
            raise Exception(f"{log_prefix}Не удалось получить данные после нескольких попыток.")

        try:
            # Получаем JSON-ответ
            campaigns_data = response_promotion_count.json()

            # Проверяем, что это словарь
            if not isinstance(campaigns_data, dict):
                raise Exception(
                    f"Некорректный формат ответа от API (promotion/count): ожидается словарь, получено {type(campaigns_data)}")

            # Извлекаем список кампаний из adverts
            adverts = campaigns_data.get("adverts", [])
            if not isinstance(adverts, list):
                raise Exception("Некорректный формат данных в поле 'adverts': ожидается список.")

            # Собираем ID кампаний из каждого элемента adverts
            advert_ids = [
                advert["advertId"]
                for advert_group in adverts
                for advert in advert_group.get("advert_list", [])
                if "advertId" in advert
            ]

            if not advert_ids:
                logging.info(f"{log_prefix}Нет доступных кампаний за указанный период.")
                continue
        except Exception as e:
            raise Exception(f"Ошибка обработки данных из API (promotion/count): {e}")

        # Подготовка к обработке ID кампаний по частям
        all_filtered_data = []
        chunks = list(_chunk_list(advert_ids, 100))

        for index, chunk in enumerate(chunks):
            logging.debug(f"{log_prefix}Обработка группы {index + 1} из {len(chunks)} (ID кампаний: {len(chunk)}).")
            fullstats_payload = [
                {"id": advert_id, "interval": {"begin": formatted_date_from, "end": formatted_date_to}}
                for advert_id in chunk
            ]

            # Выполняем запрос
            retry_attempts = 5
            for attempt in range(retry_attempts):
                response_fullstats = requests.post(url_fullstats, json=fullstats_payload, headers=headers)
                if response_fullstats.status_code == 200:
                    break
                elif response_fullstats.status_code == 429:
                    logging.error(f"{log_prefix}Превышен лимит запросов. Попытка {attempt + 1} из {retry_attempts}. Ждём 60 секунд...")
                    time.sleep(60)  # Ожидание 60 секунд
                else:
                    logging.error(f"{log_prefix}Ошибка получения данных по воронке продаж: {response_fullstats.status_code}, {response_fullstats.text}, ждем 10 секунд...")
                    time.sleep(10)
                    break
            else:
                logging.error(f"{log_prefix}Не удалось получить данные после нескольких попыток.")
                continue

            if response_fullstats.status_code != 200:
                continue

            try:
                fullstats = response_fullstats.json()
                if not isinstance(fullstats, list):
                    raise Exception("Ответ API (fullstats) не является списком.")

                # Распаковываем данные и фильтруем
                filtered_data = _flatten_and_filter_data(fullstats)
                all_filtered_data.extend(filtered_data)
            except Exception as e:
                logging.error(f"{log_prefix}Ошибка обработки данных из API (fullstats): {e}")
                continue

            # Ожидание между запросами
            if index < len(chunks) - 1:
                logging.debug(f"{log_prefix}Ждем 10 секунд перед следующим запросом...")
                time.sleep(10)

        # Преобразуем данные в DataFrame
        if not all_filtered_data:
            logging.info(f"{log_prefix}Нет данных для загрузки в таблицу.")
            continue

        df = pd.DataFrame(all_filtered_data)

        if df.empty:
            logging.info(f"{log_prefix}Нет данных для выбранного периода.")
            continue

        meta = entities_meta.get(seller_legal)
        if not meta:
            logging.warning(f"{log_prefix}{seller_legal} — нет meta, legal_entity не заполняются")
        else:
            df['legal_entity'] = meta['display_name']
            # df['entity_id']    = meta['id']           # числовой id из таблицы legal_entities
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
