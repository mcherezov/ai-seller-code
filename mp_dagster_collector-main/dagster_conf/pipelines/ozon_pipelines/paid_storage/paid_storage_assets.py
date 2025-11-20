from dagster import asset, String
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import asyncio
import tempfile
import os
import shutil
import uuid
import json
import pandas as pd
import numpy as np
from selenium.common.exceptions import TimeoutException
from src.connectors.ozon.ozon_api import OzonAsyncClient
from src.db.bronze.models import OzonPaidStorage1d
from dagster_conf.resources.pg_resource import postgres_resource
from src.config_loader import load_config
from src.utils.chrome_setup import setup_chrome_driver, download_file

@asset(
    resource_defs={"ozon_client": OzonAsyncClient, "postgres": postgres_resource},
    required_resource_keys={"ozon_client", "postgres"},
    key_prefix=["bronze"],
    name="ozon_paid_storage_1d",
    description="Сохраняет отчет Ozon платного хранения в bronze.ozon_paid_storage_1d",
    config_schema={"seller_legal": String},
)
async def bronze_ozon_paid_storage_1d(context):
    # 1. Метаданные запуска
    run_uuid = context.run_id
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    raw_dt = datetime.fromisoformat(scheduled_iso) if scheduled_iso else datetime.now(timezone.utc)
    run_dttm = raw_dt.astimezone(ZoneInfo("Europe/Moscow"))
    api_token_id = context.resources.ozon_client.token_id

    # 2. Параметры отчета
    report_date = run_dttm.date() - timedelta(days=1)
    date_str = report_date.strftime("%d.%m.%Y")
    seller_legal = context.op_config["seller_legal"]

    # 3. Функция скачивания и обработки (синхронно)
    def fetch_and_parse():
        cfg = load_config()
        download_dir = tempfile.mkdtemp()
        try:
            driver = setup_chrome_driver(seller_legal, download_dir)
            # жестко задаем поведение загрузок
            driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": download_dir})
            # URL из конфигурации
            base_url = cfg["ozon_paid_storage"][seller_legal]
            file_path = download_file(driver, base_url, date_str, date_str, download_dir)
            # Чтение и очистка данных
            df = pd.read_excel(file_path)
            df = df.fillna(0).replace([np.inf, -np.inf], 0)
            # Преобразование в список словарей
            records = df.to_dict(orient="records")
            response_dttm = datetime.now(timezone.utc)
            response_code = 200
            return records, response_dttm, response_code
        finally:
            try:
                driver.quit()
            except:
                pass
            shutil.rmtree(download_dir, ignore_errors=True)

    # 4. Выполнение в фоновой задаче
    try:
        records, response_dttm, response_code = await asyncio.to_thread(fetch_and_parse)
    except Exception as e:
        context.log.error(f"[ozon_paid_storage] error: {e}")
        raise

    # 5. Подготовка записи для вставки
    record = {
        "api_token_id": api_token_id,
        "run_uuid": run_uuid,
        "run_dttm": run_dttm,
        "request_uuid": uuid.uuid4(),
        "request_dttm": run_dttm,
        "request_parameters": {"date_from": date_str, "date_to": date_str, "seller_legal": seller_legal},
        "request_body": None,
        "response_dttm": response_dttm,
        "response_code": response_code,
        "response_body": json.dumps(records, ensure_ascii=False),
    }

    # 6. Вставка в бронзу
    async with context.resources.postgres() as session:
        await session.execute(OzonPaidStorage1d.__table__.insert().values(**record))
        await session.commit()

    context.log.info(f"[bronze_ozon_paid_storage_1d] Inserted run_uuid={run_uuid}")
