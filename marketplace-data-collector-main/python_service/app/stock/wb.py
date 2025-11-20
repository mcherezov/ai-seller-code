from typing import Dict
import uuid
import requests
import pandas as pd
from time import sleep
from network import request_with_retries


def fetch_wb_data(marketplace_keys: Dict[str, str], entities_meta):
    """
    Получает данные об остатках через API Wildberries.

    :param marketplace_keys: Пары ключ-значение с seller_legal и api_key.
    """
    create_report_url = 'https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains?groupByBrand=True&groupBySubject=True&groupBySa=True&groupByNm=True&groupByBarcode=True&groupBySize=True'

    df_list = []
    for seller_legal, api_key in marketplace_keys.items():
        log_prefix = f"Остатки wb. {seller_legal}. "
        err_prefix = f"\033[91mWARNING: {log_prefix}\033[0m"
        headers = {
            "Authorization": api_key
        }
        create_report_response = request_with_retries(
            requests.get, url=create_report_url, headers=headers, err_prefix=err_prefix + '. Создание отчета. '
        )
        if create_report_response.status_code != 200:
            raise Exception(f"{err_prefix}Ошибка при создании отчета: {create_report_response.status_code}, {create_report_response.text}")

        sleep(10)
        data = create_report_response.json()
        task_id = data['data']['taskId']
        status_url = f'https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/status'

        while True:
            check_status_response = request_with_retries(
                requests.get, url=status_url, headers=headers, err_prefix=err_prefix + '. Статус. '
            )
            if check_status_response.status_code != 200:
                raise Exception(f"{err_prefix}Ошибка при запросе статуса отчета: {check_status_response.status_code}, {check_status_response.text}")
            if check_status_response.json()['data']['status'] == "done":
                break
            sleep(10)

        data_url = f'https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/download'

        data_response = request_with_retries(
            requests.get, url=data_url, headers=headers, err_prefix=err_prefix + '. Получение отчета. '
        )
        if data_response.status_code != 200:
            raise Exception(f"{err_prefix}Ошибка при запросе данных: {data_response.status_code}, {data_response.text}")

        # Преобразуем ответ в DataFrame
        data = data_response.json()
        df = pd.DataFrame(data)

        # Проверяем, есть ли данные
        if df.empty:
            print(f"{err_prefix}Нет данных для выбранного периода.")
            continue

        meta = entities_meta.get(seller_legal)
        if meta:
            df["legal_entity"] = meta["display_name"]
            # df["entity_id"]    = meta["id"]
        else:
            df["legal_entity"] = seller_legal
            # df["entity_id"]    = None

        df["warehouses"] = df["warehouses"].astype(str)

        # Удаляем дубликаты по ключевым столбцам
        key_columns = ["vendorCode", "nmId", "barcode"]
        if all(col in df.columns for col in key_columns):
            df = df.drop_duplicates(subset=key_columns)
        df_list.append(df)

    final_df = pd.concat(df_list, ignore_index=True)
    final_df = final_df.drop_duplicates()

    final_df['uuid'] = [str(uuid.uuid4()) for _ in range(len(final_df))]

    return final_df
