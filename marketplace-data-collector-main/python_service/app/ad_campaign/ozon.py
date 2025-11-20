import logging
from typing import Dict, Optional
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO, StringIO
from datetime import datetime
import time
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

from network import request_with_retries

logger = logging.getLogger(__name__)


def _process_csv_data(csv_data: str) -> Optional[pd.DataFrame]:
    required_columns = [
        "SKU", "Название товара", "Цена товара, ₽", "Показы", "Клики", "CTR, %",
        "Корзины", "Стоимость клика, ₽", "Расход, ₽, с НДС",
        "Расход за минусом бонусов, ₽ с НДС", "Заказы, шт", "Выручка, ₽",
        "Заказы модели", "Выручка с заказов модели, ₽", "Дата"
    ]
    try:
        df = pd.read_csv(StringIO(csv_data), sep=";", decimal=",", skiprows=1)
        processed_df = pd.DataFrame(columns=required_columns)

        if "sku" in df.columns:
            processed_df["SKU"] = df.get("sku", 0)
            processed_df["Название товара"] = df.get("Название товара", "")
            processed_df["Цена товара, ₽"] = df.get("Цена товара, ₽", 0)
            processed_df["Показы"] = df.get("Показы", 0)
            processed_df["Клики"] = df.get("Клики", 0)
            processed_df["CTR, %"] = df.get("CTR (%)", 0)
            processed_df["Корзины"] = df.get("В корзину", 0)
            processed_df["Стоимость клика, ₽"] = df.get("Средняя цена клика, ₽", 0)
            processed_df["Расход, ₽, с НДС"] = df.get("Расход, ₽, с НДС", 0)
            processed_df["Расход за минусом бонусов, ₽ с НДС"] = df.get(
                "Расход за минусом бонусов, ₽ с НДС", 0)
            processed_df["Заказы, шт"] = df.get("Заказы", 0)
            processed_df["Выручка, ₽"] = df.get("Выручка, ₽", 0)
            processed_df["Заказы модели"] = df.get("Заказы модели", 0)
            processed_df["Выручка с заказов модели, ₽"] = df.get("Выручка с заказов модели, ₽", 0)
            processed_df["Дата"] = "0"
        elif "Ozon ID" in df.columns:
            processed_df["SKU"] = df.get("Ozon ID продвигаемого товара", 0)
            processed_df["Название товара"] = df.get("Наименование", "")
            processed_df["Цена товара, ₽"] = df.get("Цена продажи", 0)
            processed_df["Показы"] = 0
            processed_df["Клики"] = df.get("Количество", 0)
            processed_df["CTR, %"] = 0
            processed_df["Корзины"] = 0
            processed_df["Стоимость клика, ₽"] = 0
            processed_df["Расход, ₽, с НДС"] = df.get("Расход, ₽", 0)
            processed_df["Расход за минусом бонусов, ₽ с НДС"] = 0
            processed_df["Заказы, шт"] = 0
            processed_df["Выручка, ₽"] = df.get("Цена продажи", 0)
            processed_df["Заказы модели"] = 0
            processed_df["Выручка с заказов модели, ₽"] = 0
            processed_df["Дата"] = df.get("Дата", "0")

        else:
            logger.warning(f"Неизвестный формат файла. Пропуск обработки.")
            return None

        processed_df["SKU"] = processed_df["SKU"].astype(str).str.replace(r'\.0$', '', regex=True)

        processed_df = processed_df[processed_df["SKU"].notnull() & (processed_df["SKU"] != "Всего")]
        processed_df = processed_df.fillna(0)
        processed_df = processed_df.replace([np.inf, -np.inf], 0)

        if processed_df.empty:
            logger.warning(f"Данные не были добавлены, так как DataFrame пуст.")
        return processed_df
    except Exception as e:
        logger.error(f"Ошибка обработки CSV данных: {e}")


def _fetch_ozon_ad_campaign_statistics(date_from: datetime.date, date_to: datetime.date, seller_legal: str, client_id: str, client_secret: str) -> pd.DataFrame:
    log_prefix = f"Реклама ozon (стат). {date_from}-{date_to}. {seller_legal}. "

    try:
        formatted_date_from = date_from.strftime("%Y-%m-%dT00:00:00+03:00")
        formatted_date_to = date_to.strftime("%Y-%m-%dT23:59:59+03:00")
        formatted_date_from_short = date_from.strftime("%Y-%m-%d")
        formatted_date_to_short = date_to.strftime("%Y-%m-%d")
    except ValueError as e:
        raise Exception(f"{log_prefix}Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    # Авторизация: получение токена
    auth_url = "https://api-performance.ozon.ru/api/client/token"
    auth_payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    auth_headers = {"Content-Type": "application/json"}

    auth_response = request_with_retries(
        requests.post, url=auth_url, headers=auth_headers, json=auth_payload, err_prefix=log_prefix
    )
    if auth_response.status_code != 200:
        raise Exception(f"{log_prefix}Ошибка авторизации: {auth_response.status_code}, {auth_response.text}")

    token_data = auth_response.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise Exception(f"{log_prefix}Не удалось получить токен доступа.")

    # Получение данных по рекламным кампаниям
    expense_url = "https://api-performance.ozon.ru/api/client/statistics/expense"
    expense_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    expense_params = {
        "dateFrom": formatted_date_from_short,
        "dateTo": formatted_date_to_short
    }

    expense_response = request_with_retries(
        requests.get, url=expense_url, headers=expense_headers, params=expense_params, err_prefix=log_prefix
    )
    if expense_response.status_code != 200:
        raise Exception(
            f"{log_prefix} Ошибка получения данных по рекламным кампаниям: {expense_response.status_code}, {expense_response.text}"
        )

    # Обработка ответа в формате CSV
    try:
        csv_data = expense_response.text  # Получаем текстовый ответ
        df = pd.read_csv(StringIO(csv_data), sep=";", decimal=",")  # Преобразуем в DataFrame
    except Exception as e:
        raise Exception(f"{log_prefix} Ошибка обработки данных: {e}")

    # Получение списка кампаний
    if df.empty:
        logger.info(f"{log_prefix}Данные по рекламным кампаниям отсутствуют в ответе. Завершение функции.")
        return None

    campaign_ids = list(set(df["ID"].astype(str)))
    if not campaign_ids:
        raise Exception(f"{log_prefix}Список ID кампаний пуст.")

    # Выгрузка статистики по 10 кампаний за раз
    statistics_url = "https://api-performance.ozon.ru:443/api/client/statistics"
    report_status_url = "https://api-performance.ozon.ru:443/api/client/statistics"
    report_url = "https://api-performance.ozon.ru:443/api/client/statistics/report"
    statistics_headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    result_data = []

    for i in range(0, len(campaign_ids), 10):
        logger.info(f"{log_prefix} Чанк {i} из {len(campaign_ids)}:")
        campaigns_chunk = campaign_ids[i:i + 10]
        statistics_payload = {
            "campaigns": campaigns_chunk,
            "from": formatted_date_from,
            "to": formatted_date_to,
            "groupBy": "NO_GROUP_BY"
        }

        statistics_response = request_with_retries(
            requests.post, url=statistics_url, headers=statistics_headers, json=statistics_payload, err_prefix=log_prefix
        )

        if statistics_response.status_code != 200:
            raise Exception(
                f"{log_prefix}Ошибка отправки запроса на статистику: {statistics_response.status_code}, {statistics_response.text}"
            )

        report_id = statistics_response.json().get("UUID")
        if not report_id:
            raise Exception(f"{log_prefix}Не удалось получить UUID отчета.")

        # Проверка статуса отчета
        current_state = None
        N = 0
        while True:
            report_status_response = request_with_retries(
                requests.get, url=f"{report_status_url}/{report_id}", headers=statistics_headers, err_prefix=log_prefix
            )

            if report_status_response.status_code == 401:
                logger.info(f"{log_prefix}Проверка {N}: код ошибки: 401. Пробуем заново авторизоваться")
                auth_response = request_with_retries(
                    requests.post, url=auth_url, headers=auth_headers, json=auth_payload, err_prefix=log_prefix
                )
                if auth_response.status_code != 200:
                    raise Exception(f"{log_prefix}Ошибка авторизации: {auth_response.status_code}, {auth_response.text}")

                token_data = auth_response.json()
                access_token = token_data.get("access_token")
                if not access_token:
                    raise Exception(f"{log_prefix}Не удалось получить токен доступа.")
                statistics_headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
                time.sleep(30)
            elif report_status_response.status_code != 200:
                raise Exception(
                    f"{log_prefix}Ошибка проверки статуса отчета: {report_status_response.status_code}, {report_status_response.text}"
                )

            report_status = report_status_response.json()
            current_state = report_status.get("state")
            if current_state == "OK":
                logger.info(f"{log_prefix}Проверка {N}: статус: {current_state}")
                break
            else:
                logger.info(f"{log_prefix}Проверка {N}: статус: {current_state}")
                if N >= (60 - 1) and current_state != "IN_PROGRESS":
                    raise Exception(f"{log_prefix}Отчет не готов после максимального количества попыток.")
                time.sleep(30)
            N += 1

        if current_state == "OK":
            report_response = request_with_retries(
                requests.get, url=f"{report_url}?UUID={report_id}", headers=statistics_headers, err_prefix=log_prefix
            )
            if report_response.status_code == 200:
                content_type = report_response.headers.get("Content-Type", "").lower()
                if "text/csv" in content_type:
                    csv_data = report_response.text
                    result_data.append(_process_csv_data(csv_data))
                elif "application/zip" in content_type:
                    with ZipFile(BytesIO(report_response.content)) as zip_file:
                        for file_name in zip_file.namelist():
                            with zip_file.open(file_name) as file:
                                csv_data = file.read().decode("utf-8")
                                result_data.append(_process_csv_data(csv_data))
                else:
                    raise Exception(f"{log_prefix}Неизвестный формат ответа. Ожидается CSV или ZIP.")
                logger.info(f"{log_prefix}Добавление отчетов из архива закончено")

    result_data = [data for data in result_data if data is not None]
    if result_data:
        result_data = pd.concat(result_data)
        return result_data
    else:
        return None


def fetch_ozon_ad_campaign_statistics(config: Dict[str, Dict[str, str]], date: datetime.date) -> pd.DataFrame:

    def process_seller(seller_legal, data):
        client_id = data['client_id']
        client_secret = data['client_secret']
        df = _fetch_ozon_ad_campaign_statistics(date, date, seller_legal, client_id, client_secret)
        if df is None:
            return None
        df["date"] = pd.to_datetime(date)
        if seller_legal == "inter":
            df["legalEntity"] = "ИНТЕР"
        elif seller_legal == "ut":
            df["legalEntity"] = "АТ"
        else:
            raise Exception(f"Необработанный seller_legal={seller_legal}")
        return df

    result_df = []

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_seller, seller_legal, data) for seller_legal, data in config.items()]

        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                result_df.append(result)

    if result_df:
        result_df = pd.concat(result_df)
        return result_df
    else:
        return None
