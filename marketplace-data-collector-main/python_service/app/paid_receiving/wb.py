import time
import logging
import pandas as pd
from datetime import date
from api_clients.wb import WildberriesAPI


def fetch_paid_acceptance_data(config: dict, entities_meta, date_from: date, date_to: date) -> pd.DataFrame:
    """
    Получить отчет по платной приёмке для всех юрлиц за указанный период (от и до включительно)
    """
    sellers = list(config["wb_marketplace_keys"].keys())
    dfs = []

    date_from_str = date_from.strftime("%Y-%m-%d")
    date_to_str = date_to.strftime("%Y-%m-%d")

    for seller_legal in sellers:
        wb = WildberriesAPI(seller_legal)

        # 1. Запрос задачи
        try:
            data = wb.get_paid_acceptance_report(date_from=date_from_str, date_to=date_to_str)
            task_id = data.get("data", {}).get("taskId")
        except Exception as e:
            logging.warning(f"❌ [{seller_legal}] Ошибка запроса get_paid_acceptance_report: {e}")
            continue

        if not task_id:
            logging.debug(f"⚠️ [{seller_legal}] WB не сформировал задачу (скорее всего, не было приёмок).")
            continue

        # 2. Ожидание готовности
        for attempt in range(60):
            status = wb.get_paid_acceptance_status(task_id)
            if status.get("data", {}).get("status") == "done":
                break
            elif status.get("data", {}).get("status") == "error":
                logging.debug(f"❌ [{seller_legal}] Ошибка генерации отчета, пропускаем.")
                task_id = None
                break
            time.sleep(5)

        if not task_id:
            continue

        # 3. Скачивание
        try:
            report = wb.get_paid_acceptance_data(task_id)

            if not isinstance(report, list):
                raise Exception(f"[{seller_legal}] Некорректный формат данных от API: {type(report)}")

        except Exception as e:
            logging.warning(f"❌ [{seller_legal}] Ошибка при получении данных отчета: {e}")
            continue

        if not report:
            logging.debug(f"⚠️ [{seller_legal}] WB вернул пустой список, приёмок не было.")
            continue

        # 4. Обработка
        df = pd.json_normalize(report)
        df["date"] = date.today().strftime("%Y-%m-%d")
        meta = entities_meta.get(seller_legal)
        df["legal_entity"] = meta["display_name"] if meta else seller_legal

        dfs.append(df)

    if not dfs:
        logging.info("Нет данных ни для одного продавца за указанный период.")
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)
