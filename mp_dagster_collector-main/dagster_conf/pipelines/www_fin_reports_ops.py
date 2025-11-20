import time
import base64
import json
from zoneinfo import ZoneInfo
import io
import zipfile
import hashlib
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import text, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from dagster import op, get_dagster_logger, DynamicOut, DynamicOutput, In, Out
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src.db.bronze.models import WbWwwFinReports1d
from src.db.silver.models import WbSales1d, SilverAdjustmentsByProduct, SilverAdjustmentsGeneral, WbLogistics1d


column_type_mapping = {
    "№": int,
    "Номер поставки": int,
    "Предмет": str,
    "Код номенклатуры": int,
    "Бренд": str,
    "Артикул поставщика": str,
    "Название": str,
    "Размер": str,
    "Баркод": str,
    "Тип документа": str,
    "Обоснование для оплаты": str,
    "Дата заказа покупателем": "date",
    "Дата продажи": "date",
    "Кол-во": int,
    "Цена розничная": float,
    "Вайлдберриз реализовал Товар (Пр)": float,
    "Согласованный продуктовый дисконт, %": int,
    "Промокод, %": str,
    "Итоговая согласованная скидка, %": int,
    "Цена розничная с учетом согласованной скидки": float,
    "Размер снижения кВВ из-за рейтинга, %": int,
    "Размер изменения кВВ из-за акции, %": float,
    "Скидка постоянного Покупателя (СПП), %": float,
    "Размер кВВ, %": float,
    "Размер  кВВ без НДС, % Базовый": float,
    "Итоговый кВВ без НДС, %": float,
    "Вознаграждение с продаж до вычета услуг поверенного, без НДС": float,
    "Возмещение за выдачу и возврат товаров на ПВЗ": float,
    "Эквайринг/Комиссии за организацию платежей": float,
    "Размер комиссии за эквайринг/Комиссии за организацию платежей, %": float,
    "Тип платежа за Эквайринг/Комиссии за организацию платежей": str,
    "Вознаграждение Вайлдберриз (ВВ), без НДС": float,
    "НДС с Вознаграждения Вайлдберриз": float,
    "К перечислению Продавцу за реализованный Товар": float,
    "Количество доставок": int,
    "Количество возврата": int,
    "Услуги по доставке товара покупателю": float,
    "Дата начала действия фиксации": str,
    "Дата конца действия фиксации": str,
    "Признак услуги платной доставки": str,
    "Общая сумма штрафов": float,
    "Корректировка Вознаграждения Вайлдберриз (ВВ)": float,
    "Виды логистики, штрафов и корректировок ВВ": str,
    "Стикер МП": str,
    "Наименование банка-эквайера": str,
    "Номер офиса": str,
    "Наименование офиса доставки": str,
    "ИНН партнера": str,
    "Партнер": str,
    "Склад": str,
    "Страна": str,
    "Тип коробов": str,
    "Номер таможенной декларации": str,
    "Номер сборочного задания": int,
    "Код маркировки": str,
    "ШК": int,
    "Srid": str,
    "Возмещение издержек по перевозке/по складским операциям с товаром": float,
    "Организатор перевозки": str,
    "Хранение": float,
    "Удержания": float,
    "Платная приемка": float,
    "Фиксированный коэффициент склада по поставке": float,
    "Признак продажи юридическому лицу": str,
    "Номер короба для платной приемки": str,
    "Скидка по программе софинансирования": float,
    "Скидка Wibes, %": float,
    "Сумма удержанная за начисленные баллы программы лояльности": float,
    "Компенсация скидки по программе лояльности": float,
}


# 1) Константы
MSK = ZoneInfo("Europe/Moscow")
SALE_REPORT_LIST_URL   = "https://seller.wildberries.ru/suppliers-mutual-settlements/reports-implementations/reports-daily"
SALE_REPORT_DETAIL_URL = (
    "https://seller.wildberries.ru/"
    "suppliers-mutual-settlements/reports-implementations/"
    "reports-daily/report/{report_id}?isGlobalBalance=false"
)

# 1.1) Маршрутизация по обоснованию для оплаты
PAYMENT_REASON_COL = "Обоснование для оплаты"
REASONS = {
    "sales": {"Продажа", "Возврат"},
    "logistics": {"Логистика"},
    "retentions": {"Удержание"},
    "fines": {"Штраф"},
    "compensations": {"Добровольная компенсация при возврате"},
}

def filter_by_reason(df: pd.DataFrame, reasons: set[str], logger, target_name: str) -> pd.DataFrame:
    if PAYMENT_REASON_COL not in df.columns:
        raise ValueError(f"Нет колонки '{PAYMENT_REASON_COL}' в исходном DF")
    out = df[df[PAYMENT_REASON_COL].isin(reasons)].copy()
    logger.info(f"[router] {target_name}: {len(out)} строк из {len(df)}")
    return out


# 2) Селекторы
SELECTORS = {
    "open_report_btn":  '//*[@id="app-content-id"]/div[1]/div/div/div/div[3]/div/div[1]/div/div[1]/div[2]/div/div/button',
    "start_date":       '//*[@id="startDate"]',
    "end_date":         '//*[@id="endDate"]',
    "save":             "//button[.//text()[contains(., 'Save') or contains(., 'Сохранить')]]",
    "table_wrapper":    "div[class^='Reports-table__wrapper']",
    "row":              "div[class^='Reports-table-row__']",
}


@op(
    ins={"report_date": In(str)},
    out=DynamicOut(),
    required_resource_keys={"selenium_remote"},
    description="Логинимся в кабинете WB, задаём дату и собираем report_id + cookies + authv3",
)
def get_report_ids(context, report_date: str):
    logger = get_dagster_logger()
    seen: set[str] = set()
    date_str = datetime.fromisoformat(report_date).strftime("%d.%m.%Y")

    for token_id_str, profile_name in context.resources.selenium_remote.profiles.items():
        api_token_id = int(token_id_str)
        driver = context.resources.selenium_remote(api_token_id)
        try:
            driver.get("https://seller.wildberries.ru/suppliers-mutual-settlements/reports-implementations/reports-daily")
            WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="app-content-id"]/div[1]/div/div/div/div[3]/div/div[1]/div/div[1]/div[2]/div/div/button'))
            ).click()

            start_field = WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="startDate"]')))
            start_field.clear(); start_field.send_keys(date_str); start_field.send_keys(Keys.TAB); time.sleep(0.5)

            end_field = WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="endDate"]')))
            end_field.clear(); end_field.send_keys(date_str); end_field.send_keys(Keys.TAB); time.sleep(1)

            WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, "//button[.//text()[contains(., 'Save') or contains(., 'Сохранить')]]"))
            ).click()

            WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[class^='Reports-table__wrapper']"))
            )

            rows = driver.find_elements(By.CSS_SELECTOR, "div[class^='Reports-table-row__']")
            for row in rows:
                try:
                    btn = row.find_element(By.TAG_NAME, "button")
                    raw = btn.text.strip().replace("\u00A0", "").replace(" ", "")
                    if not raw.isdigit() or len(raw) < 10 or raw in seen:
                        continue
                    seen.add(raw)
                    selenium_cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
                    authv3 = driver.execute_script("return window.localStorage.getItem('wb-eu-passport-v2.access-token');")

                    yield DynamicOutput(
                        {
                            "report_id": raw,
                            "report_date": report_date,   # <── добавили
                            "legal_entity": profile_name,
                            "cookies": selenium_cookies,
                            "authv3": authv3,
                            "api_token_id": api_token_id, # <── было company_id
                        },
                        mapping_key=raw,
                    )
                except Exception:
                    logger.debug(f"[{profile_name}] строка без валидного report_id — пропускаем")
        finally:
            driver.quit()


# ──────────────────────────────────────────────────────────────────────────────
# op: скачиваем ZIP и пишем в bronze.wb_www_fin_reports_1d по НОВОЙ схеме миксина
# ──────────────────────────────────────────────────────────────────────────────
@op(
    ins={"report_meta": In(dict)},
    out=Out(WbWwwFinReports1d),
    required_resource_keys={"postgres"},
    description="Скачиваем ZIP-архив через HTTP API и сохраняем его в bronze.wb_www_fin_reports_1d",
)
async def write_fin_report_zip(context, report_meta: dict):
    logger = get_dagster_logger()
    session_maker = context.resources.postgres

    api_token_id = int(report_meta["api_token_id"])
    report_id    = str(report_meta["report_id"])
    report_date  = report_meta.get("report_date")  # ISO (YYYY-MM-DD)
    cookies      = report_meta["cookies"]
    authv3       = report_meta["authv3"]

    url = (
        "https://seller-services.wildberries.ru/ns/reports/seller-wb-balance/api/v1"
        f"/reports/{report_id}/details/archived-excel"
    )

    headers = {
        "accept": "*/*",
        "accept-language": "ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7,kk;q=0.6",
        "authorizev3": authv3,
        "X-Supplier-Id": cookies.get("x-supplier-id") or cookies.get("x-supplier-id-external", ""),
        "content-type": "application/json",
        "origin": "https://seller.wildberries.ru",
        "referer": "https://seller.wildberries.ru/",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138 Safari/537.36",
    }

    # ── служебные тайминги ─────────────────────────────────────────────────────
    # Плановое время запуска из тега Dagster; fallback — сейчас
    scheduled_iso = context.dagster_run.tags.get("dagster/scheduled_execution_time")
    run_schedule_dttm = (
        datetime.fromisoformat(scheduled_iso).astimezone(MSK)
        if scheduled_iso else datetime.now(MSK)
    )
    # Фактическое время (момент выполнения op)
    run_dttm = datetime.now(MSK)

    business_dttm = run_schedule_dttm.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

    # ── запрос ─────────────────────────────────────────────────────────────────
    request_dttm = datetime.now(MSK)
    resp = requests.get(url, headers=headers, cookies=cookies, timeout=60)
    response_dttm = datetime.now(MSK)
    resp.raise_for_status()
    receive_dttm = response_dttm

    raw_zip = base64.b64decode(resp.json()["data"]["file"])

    # ── данные для вставки ─────────────────────────────────────────────────────
    request_parameters = {"report_id": report_id}
    if report_date:
        request_parameters["report_date"] = report_date

    zip_table = WbWwwFinReports1d.__table__
    stmt = (
        insert(zip_table)
        .values(
            api_token_id=api_token_id,
            run_uuid=context.run_id,
            run_dttm=run_dttm,
            run_schedule_dttm=run_schedule_dttm,
            business_dttm=business_dttm,
            request_dttm=request_dttm,
            request_parameters=request_parameters,
            request_body=url,
            response_code=resp.status_code,
            response_dttm=response_dttm,
            receive_dttm=receive_dttm,
            response_body=raw_zip,
        )
        .returning(*zip_table.c)
    )

    async with session_maker() as sess:
        row = (await sess.execute(stmt)).one()
        await sess.commit()

    obj = WbWwwFinReports1d(**row._mapping)
    logger.info(f"✅ Saved ZIP report {report_id} for token {api_token_id} (business_dttm={business_dttm.isoformat()})")
    return obj

def safe_cast_column(df: pd.DataFrame, col: str, dtype) -> pd.Series:
    """Безопасное приведение типа колонки."""
    try:
        if dtype == "date":
            return pd.to_datetime(df[col], errors="coerce").dt.date
        elif dtype in ("Int64", "int"):
            return pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif dtype == float:
            return pd.to_numeric(df[col], errors="coerce")
        elif dtype == str:
            return df[col].astype(str).fillna("")
        else:
            return df[col].astype(dtype)
    except Exception as e:
        get_dagster_logger().warning(f"Не удалось привести колонку '{col}' к типу {dtype}: {e}")
        return df[col]


@op(
    ins={"bronze_row": In(WbWwwFinReports1d)},
    out=Out(pd.DataFrame),
    description="Извлекает ZIP из бронзы → распаковывает XLSX → возвращает DataFrame",
)
def unpack_and_load_excel(context, bronze_row: WbWwwFinReports1d) -> pd.DataFrame:
    logger = get_dagster_logger()

    raw_zip: bytes = bronze_row.response_body
    if not raw_zip:
        raise ValueError(f"В бронзе запись {bronze_row.request_uuid} нет ZIP-архива")

    # Распаковываем ZIP и читаем первый XLSX-файл
    with zipfile.ZipFile(io.BytesIO(raw_zip)) as zf:
        name = zf.namelist()[0]
        logger.debug(f"Unpacking '{name}' from ZIP for request {bronze_row.request_uuid}")
        with zf.open(name) as f:
            df = pd.read_excel(f, engine="openpyxl", dtype={"Баркод": str})

    # Унифицированное приведение типов
    for col, dtype in column_type_mapping.items():
        if col in df.columns:
            df[col] = safe_cast_column(df, col, dtype)

    # Нормализуем «Обоснование для оплаты» (трим пробелов)
    if PAYMENT_REASON_COL in df.columns:
        df[PAYMENT_REASON_COL] = df[PAYMENT_REASON_COL].astype(str).str.strip()

    # Прокидываем служебные поля
    df["request_uuid"]  = bronze_row.request_uuid
    df["business_dttm"]  = bronze_row.business_dttm
    df["response_dttm"]  = bronze_row.response_dttm
    df["company_id"] = bronze_row.api_token_id

    logger.info(f"Loaded Excel for request {bronze_row.request_uuid}: shape={df.shape}")
    return df


async def bulk_insert_records(session_maker, table, records: list[dict], chunk_size: int = 500):
    """Вставка записей чанками."""
    async with session_maker() as session:
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            stmt = insert(table).values(chunk)
            await session.execute(stmt)
        await session.commit()


async def bulk_upsert_records(session_maker, table, records: list[dict], chunk_size: int = 500):
    """Идемпотентная вставка: ON CONFLICT DO NOTHING по составному ключу."""
    async with session_maker() as session:
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            stmt = pg_insert(table).values(chunk)
            stmt = stmt.on_conflict_do_nothing()
            await session.execute(stmt)
        await session.commit()


def make_key_hash(row: dict, keys: list[str]) -> str:
    parts = [(row.get(k) or "").strip() for k in keys]
    return hashlib.md5("||".join(parts).encode("utf-8")).hexdigest()


SALES_COLUMN_MAPPING = {
    "Дата заказа покупателем":            "order_date",
    "Дата продажи":                        "sale_date",
    "Обоснование для оплаты":             "payment_reason",
    "Srid":                               "sr_id",
    "Номер поставки":                     "income_id",
    "Бренд":                              "brand",
    "Название":                           "name",
    "Код номенклатуры":                   "nm_id",
    "Артикул поставщика":                 "supplier_article",
    "Баркод":                             "barcode",
    "Размер":                             "tech_size",
    "Предмет":                            "subject",
    "Кол-во":                             "quantity",
    "Цена розничная":                     "price",
    "Цена розничная с учетом согласованной скидки": "price_with_discount",
    "Вайлдберриз реализовал Товар (Пр)":  "wb_realization_price",
    "Скидка постоянного Покупателя (СПП), %":  "spp",
    "К перечислению Продавцу за реализованный Товар": "seller_payout",
    "Размер кВВ, %":                      "commision_percent",
    "Эквайринг/Комиссии за организацию платежей":    "acquiring_amount",
    "Размер комиссии за эквайринг/Комиссии за организацию платежей, %": "acquiring_percent",
    "Наименование банка-эквайера":        "acquiring_bank",
    "Склад":                              "warehouse_name",
    "Страна":                             "country",
    "Сумма удержанная за начисленные баллы программы лояльности": "loyalty_points_withheld_amount"
}


@op(
    ins={"df": In(pd.DataFrame)},
    required_resource_keys={"postgres"},
    description="Записывает данные продаж в silver.wb_sales_1d"
)
async def load_wb_sales(context, df: pd.DataFrame):
    logger = get_dagster_logger()
    pg = context.resources.postgres
    CHUNK_SIZE = 500

    # Фильтрация по «Продажа»/«Возврат»
    df = filter_by_reason(df, REASONS["sales"], logger, "wb_sales_1d")
    if df.empty:
        logger.info("wb_sales_1d: пусто — пропускаем")
        return

    # 1) проверяем и переименовываем
    expected_index = ["business_dttm", "request_uuid", "response_dttm", "company_id"]
    missing = [c for c in expected_index if c not in df.columns]
    if missing:
        raise ValueError(f"В incoming df нет обязательных колонок: {missing}")

    df_renamed = df.rename(columns=SALES_COLUMN_MAPPING)

    # 2) конвертим даты уже в переименованном df
    for col in ("order_date", "sale_date"):
        if col in df_renamed.columns:
            df_renamed[col] = pd.to_datetime(df_renamed[col], errors="coerce").dt.date

    # 3) проверяем, что все целевые колонки есть
    target_cols = expected_index + list(SALES_COLUMN_MAPPING.values())
    missing = [c for c in target_cols if c not in df_renamed.columns]
    if missing:
        raise ValueError(f"После переименования не нашлось колонок: {missing}")

    # 4) логируем дубликаты
    dupe_key = [
         "business_dttm", "sr_id", "payment_reason"
    ]
    dupes = df_renamed[df_renamed.duplicated(subset=dupe_key, keep=False)]
    if not dupes.empty:
        logger.warning(f"🚨 Найдено {len(dupes)} дубликатов в sales по составному ключу")

    # 5) подготовка и вставка
    records = df_renamed[target_cols].to_dict(orient="records")
    logger.info(f"Всего строк для вставки в wb_sales_1d: {len(records)}")
    await bulk_upsert_records(pg, WbSales1d.__table__, records, chunk_size=CHUNK_SIZE)

    logger.info("Загрузка wb_sales_1d завершена")


LOGISTICS_COLUMN_MAPPING = {
    "Дата заказа покупателем":              "order_date",
    "Дата продажи":                         "sale_date",
    "Обоснование для оплаты":               "payment_reason",
    "Srid":                                 "sr_id",
    "Бренд":                                "brand",
    "Название":                             "name",
    "Код номенклатуры":                     "nm_id",
    "Артикул поставщика":                   "supplier_article",
    "Баркод":                               "barcode",
    "Размер":                               "tech_size",
    "Номер поставки":                       "income_id",
    "Склад":                                "warehouse_name",
    "Страна":                               "country",
    "Услуги по доставке товара покупателю": "logistics_cost",
    "Виды логистики, штрафов и корректировок ВВ": "logistic_type",
    "Количество доставок":                  "delivery_quantity",
}


@op(
    ins={"df": In(pd.DataFrame)},
    required_resource_keys={"postgres"},
    description="Записывает данные по логистике в silver.wb_logistics_1d"
)
async def load_wb_logistics(context, df: pd.DataFrame):
    logger = get_dagster_logger()
    pg = context.resources.postgres
    CHUNK_SIZE = 500

    # Фильтрация по «Логистика»
    df = filter_by_reason(df, REASONS["logistics"], logger, "wb_logistics_1d")
    if df.empty:
        logger.info("wb_logistics_1d: пусто — пропускаем")
        return

    # 1) обязательные служебные колонки (из бронзы)
    expected_index = ["business_dttm", "company_id", "request_uuid", "response_dttm"]
    missing = [c for c in expected_index if c not in df.columns]
    if missing:
        raise ValueError(f"В incoming df нет обязательных колонок: {missing}")

    # 2) переименовываем в модельные названия
    df2 = df.rename(columns=LOGISTICS_COLUMN_MAPPING)

    # 3) приводим ТОЛЬКО полевые даты; business_dttm/response_dttm не трогаем (они TIMESTAMPTZ)
    for col in ("order_date", "sale_date"):
        if col in df2.columns:
            df2[col] = pd.to_datetime(df2[col], errors="coerce").dt.date

    # 4) проверяем, что все целевые колонки присутствуют
    target_cols = list(dict.fromkeys(expected_index + list(LOGISTICS_COLUMN_MAPPING.values())))
    missing = [c for c in target_cols if c not in df2.columns]
    if missing:
        raise ValueError(f"После переименования не найдено колонок: {missing}")

    # 5) диагностируем возможные дубликаты по ключу (PK):
    dupe_key = ["business_dttm", "sr_id", "logistic_type"]
    dupes = df2[df2.duplicated(subset=dupe_key, keep=False)]
    if not dupes.empty:
        logger.warning(f"🚨 Найдено {len(dupes)} дубликатов в логистике по ключу {dupe_key}")

    # 6) вставка
    records = df2[target_cols].to_dict(orient="records")
    logger.info(f"Всего строк для вставки в wb_logistics_1d: {len(records)}")
    await bulk_upsert_records(pg, WbLogistics1d.__table__, records, chunk_size=CHUNK_SIZE)

    logger.info("Загрузка wb_logistics_1d завершена")


def _to_str(v):
    import pandas as pd
    if pd.isna(v):
        return None
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


ADJUSTMENTS_ALLOWED_REASONS = {
    "Штраф",
    "Удержание",
    "Добровольная компенсация при возврате",
    "Компенсация ущерба",
    "Компенсация скидки по программе лояльности",
    "Сумма удержанная за начисленные баллы программы лояльности",
    "Стоимость участия в программе лояльности",
}

ADJ_BY_PRODUCT_COLUMN_MAPPING = {
    "Номер поставки": "income_id",
    "Предмет": "subject",
    "Код номенклатуры": "nomenclature_code",
    "Бренд": "brand",
    "Артикул поставщика": "supplier_article",
    "Название": "name",
    "Размер": "tech_size",
    "Баркод": "barcode",
    "Тип документа": "doc_type_name",
    "Обоснование для оплаты": "supplier_oper_name",
    "Дата заказа покупателем": "order_date",
    "Дата продажи": "sale_date",
    "Общая сумма штрафов": "penalty",
    "Виды логистики, штрафов и доплат": "bonus_type_name",
    "Номер офиса": "office_number",
    "Склад": "warehouse_name",
    "Страна": "country",
    "Тип коробов": "box_type",
    "ШК": "shk_id",
    "Srid": "sr_id",
    "К перечислению Продавцу за реализованный Товар": "seller_payout",
    "Корректировка Вознаграждения Вайлдберриз (ВВ)": "additional_payment",
    "Сумма удержанная за начисленные баллы программы лояльности": "cashback_amount",
    "Компенсация скидки по программе лояльности": "cashback_discount",
}


@op(
    ins={"df": In(pd.DataFrame)},
    required_resource_keys={"postgres"},
    description="Записывает удержания/компенсации по товарам в silver.adjustments_by_product"
)
async def load_wb_adjustments_by_product(context, df: pd.DataFrame):
    logger = get_dagster_logger()
    pg = context.resources.postgres
    CHUNK_SIZE = 500

    # 0) фильтр по допустимым причинам
    df = filter_by_reason(df, ADJUSTMENTS_ALLOWED_REASONS, logger, "adjustments_by_product")
    if df.empty:
        logger.info("adjustments_by_product: пусто — пропускаем")
        return

    # 1) обязательные служебные колонки
    expected_index = ["business_dttm", "company_id", "request_uuid", "response_dttm"]
    missing = [c for c in expected_index if c not in df.columns]
    if missing:
        raise ValueError(f"В incoming df нет обязательных колонок: {missing}")

    # 2) переименование колонок по маппингу
    df1 = df.rename(columns=ADJ_BY_PRODUCT_COLUMN_MAPPING).copy()

    # 3) приведение типов критичных полей
    # даты
    for col in ("order_date", "sale_date"):
        if col in df1.columns:
            df1[col] = safe_cast_column(df1, col, "date")
    # числовые (денежные)
    for col in ("penalty", "seller_payout", "additional_payment", "cashback_amount", "cashback_discount"):
        if col in df1.columns:
            df1[col] = safe_cast_column(df1, col, float)
    # строковые «номера офиса» и прочее
    if "office_number" in df1.columns:
        df1["office_number"] = df1["office_number"].astype(str)
    for col in ("sr_id", "shk_id", "barcode", "supplier_article", "office_number"):
        if col in df1.columns:
            df1[col] = df1[col].map(_to_str)
    # 4) финальный набор полей для вставки
    target_cols = [
        # служебные
        "business_dttm", "sr_id", "supplier_oper_name", "company_id", "request_uuid", "response_dttm",
        # данные
        "income_id", "subject", "nomenclature_code", "brand", "supplier_article", "name", "tech_size",
        "barcode", "doc_type_name", "order_date", "sale_date", "penalty", "bonus_type_name", "office_number",
        "warehouse_name", "country", "box_type", "shk_id", "seller_payout", "additional_payment",
        "cashback_amount", "cashback_discount",
    ]
    for col in target_cols:
        if col not in df1.columns:
            df1[col] = None

    # 5) диагностика дублей по PK
    pk_key = ["business_dttm", "sr_id", "supplier_oper_name"]
    dupes = df1[df1.duplicated(subset=pk_key, keep=False)]
    if not dupes.empty:
        logger.warning(f"🚨 Найдено {len(dupes)} дубликатов в adjustments_by_product по ключу {pk_key}")

    # 6) вставка
    records = df1[target_cols].to_dict(orient="records")
    logger.info(f"Всего строк для вставки в silver.adjustments_by_product: {len(records)}")
    await bulk_upsert_records(pg, SilverAdjustmentsByProduct.__table__, records, chunk_size=CHUNK_SIZE)
    logger.info("Загрузка adjustments_by_product завершена")


ADJ_GENERAL_COLUMN_MAPPING = {
    "Номер поставки": "income_id",
    "Предмет": "subject",
    "Код номенклатуры": "nm_id",
    "Бренд": "brand",
    "Артикул поставщика": "supplier_article",
    "Название": "name",
    "Размер": "tech_size",
    "Баркод": "barcode",
    "Тип документа": "doc_type_name",
    "Обоснование для оплаты": "supplier_oper_name",
    "Дата заказа покупателем": "order_date",
    "Дата продажи": "sale_date",
    "Общая сумма штрафов": "penalty",
    "Виды логистики, штрафов и доплат": "bonus_type_name",
    "Номер офиса": "office_number",
    "Склад": "warehouse_name",
    "Страна": "country",
    "Тип коробов": "box_type",
    "ШК": "shk_id",
    "Srid": "sr_id",
    "Удержания": "deduction",
}


@op(
    ins={"df": In(pd.DataFrame)},
    required_resource_keys={"postgres"},
    description="Записывает удержания/компенсации общего типа в silver.adjustments_general"
)
async def load_wb_adjustments_general(context, df: pd.DataFrame):
    logger = get_dagster_logger()
    pg = context.resources.postgres
    CHUNK_SIZE = 500

    # 0) фильтр по допустимым причинам
    df = filter_by_reason(df, ADJUSTMENTS_ALLOWED_REASONS, logger, "adjustments_general")
    if df.empty:
        logger.info("adjustments_general: пусто — пропускаем")
        return

    # 1) обязательные служебные колонки
    expected_index = ["business_dttm", "company_id", "request_uuid", "response_dttm"]
    missing = [c for c in expected_index if c not in df.columns]
    if missing:
        raise ValueError(f"В incoming df нет обязательных колонок: {missing}")

    # 2) переименование
    df1 = df.rename(columns=ADJ_GENERAL_COLUMN_MAPPING).copy()

    # 3) приведение типов
    for col in ("order_date", "sale_date"):
        if col in df1.columns:
            df1[col] = safe_cast_column(df1, col, "date")
    for col in ("penalty", "deduction"):
        if col in df1.columns:
            df1[col] = safe_cast_column(df1, col, float)
    if "office_number" in df1.columns:
        df1["office_number"] = df1["office_number"].astype(str)
    for col in ("sr_id", "shk_id", "barcode", "supplier_article", "office_number"):
        if col in df1.columns:
            df1[col] = df1[col].map(_to_str)
    # 4) финальный набор полей
    target_cols = [
        # служебные
        "business_dttm", "sr_id", "supplier_oper_name", "company_id", "request_uuid", "response_dttm",
        # данные
        "income_id", "subject", "nm_id", "brand", "supplier_article", "name", "tech_size", "barcode",
        "doc_type_name", "order_date", "sale_date", "penalty", "bonus_type_name", "office_number",
        "warehouse_name", "country", "box_type", "shk_id", "deduction",
    ]
    for col in target_cols:
        if col not in df1.columns:
            df1[col] = None

    # 5) диагностика дублей по PK
    pk_key = ["business_dttm", "sr_id", "supplier_oper_name"]
    dupes = df1[df1.duplicated(subset=pk_key, keep=False)]
    if not dupes.empty:
        logger.warning(f"🚨 Найдено {len(dupes)} дубликатов в adjustments_general по ключу {pk_key}")

    # 6) вставка
    records = df1[target_cols].to_dict(orient="records")
    logger.info(f"Всего строк для вставки в silver.adjustments_general: {len(records)}")
    await bulk_upsert_records(pg, SilverAdjustmentsGeneral.__table__, records, chunk_size=CHUNK_SIZE)
    logger.info("Загрузка adjustments_general завершена")
