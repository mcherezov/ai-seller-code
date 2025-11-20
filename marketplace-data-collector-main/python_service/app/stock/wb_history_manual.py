import os
import uuid
import json
import time
import requests
from requests.exceptions import HTTPError, ConnectionError
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from typing import Dict, Union, List
from functools import wraps
from dotenv import load_dotenv

# Загрузка конфигурации из .env
load_dotenv()
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Декоратор: бесконечный ретрай на 429, 503 и сетевые ошибки, экспоненциальная задержка

def retry_on_errors(backoff_factor: float = 1.0,
                    max_retries_429: int = 5,
                    min_interval: float = 60/3):
    last_call = 0.0
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            nonlocal last_call
            retries_429 = retries_503 = 0
            while True:
                # перед любым новым POST ждём, чтобы не нарушить rate-limit
                wait_for_rate = min_interval - (time.time() - last_call)
                if wait_for_rate > 0:
                    time.sleep(wait_for_rate)
                try:
                    resp = fn(*args, **kwargs)
                    last_call = time.time()
                    return resp
                except HTTPError as e:
                    code = e.response.status_code if e.response else None
                    if code == 429 and retries_429 < max_retries_429:
                        sleep = backoff_factor * 2**retries_429
                        print(f"[Retry] 429 → ждём {sleep:.0f}s…")
                        time.sleep(sleep)
                        retries_429 += 1
                        continue
                    if code == 503:
                        sleep = max(backoff_factor * 2**retries_503, min_interval)
                        print(f"[Retry] 503 → ждём {sleep:.0f}s…")
                        time.sleep(sleep)
                        retries_503 += 1
                        continue
                    raise
                except ConnectionError:
                    # тоже учитываем минимальный интервал
                    print(f"[Retry] network error → ждём {min_interval:.0f}s…")
                    time.sleep(min_interval)
        return wrapper
    return decorator

@retry_on_errors()
def make_post(url, **kwargs):
    resp = requests.post(url, **kwargs)
    resp.raise_for_status()
    return resp


def fetch_wb_history_remains(
    marketplace_keys: Dict[str, str],
    start_date: Union[str, datetime],
    end_date:   Union[str, datetime],
) -> pd.DataFrame:
    # 1) Приводим даты к строкам
    start_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, datetime) else start_date
    end_str   = end_date.strftime('%Y-%m-%d')   if isinstance(end_date, datetime)   else end_date
    print(f"[START] период {start_str} → {end_str}")

    # 2) Собираем все товары
    prod_url = "https://seller-analytics-api.wildberries.ru/api/v2/stocks-report/products/products"
    filters  = ["deficient","actual","balanced","nonActual","nonLiquid","invalidData"]
    limit    = 1000
    all_items: List[Dict] = []

    for seller, api_key in marketplace_keys.items():
        print(f"[{seller}] GET /products…")
        headers = {"Authorization": api_key}
        offset = 0
        while True:
            payload = {
                "currentPeriod":       {"start": start_str, "end": end_str},
                "stockType":           "",
                "skipDeletedNm":       False,
                "availabilityFilters": filters,
                "orderBy":             {"field":"stockCount","mode":"desc"},
                "limit":               limit,
                "offset":              offset
            }
            try:
                resp = make_post(prod_url, json=payload, headers=headers)
            except HTTPError as e:
                if e.response is not None and e.response.status_code == 401:
                    print(f"[{seller}] 401 Unauthorized — пропускаем юрлицо.")
                    break
                else:
                    raise

            data = resp.json().get("data", {})
            items = data.get("items", [])
            print(f"  → получили {len(items)} товаров (offset={offset})")
            if not items:
                break

            for it in items:
                it.setdefault("barcode", " ")
                it.setdefault("techSize", "0")
                it["legal_entity"] = seller
            all_items.extend(items)
            if len(items) < limit:
                break
            offset += limit

    if not all_items:
        print("❌ Нет товаров за период.")
        return pd.DataFrame()

    # 3) Для каждого nmID запрашиваем складские остатки по регионам
    off_url = "https://seller-analytics-api.wildberries.ru/api/v2/stocks-report/offices"
    warehouses_map: Dict[int, List[Dict]] = {}

    for seller, api_key in marketplace_keys.items():
        headers = {"Authorization": api_key}
        seller_items = [it for it in all_items if it["legal_entity"] == seller]
        print(f"[{seller}] разбираем {len(seller_items)} nmID…")

        for it in seller_items:
            nm = it["nmID"]
            payload = {
                "nmIDs":               [nm],
                "subjectIDs":          [],
                "brandNames":          [],
                "tagIDs":              [],
                "currentPeriod":       {"start": start_str, "end": end_str},
                "stockType":           "",
                "skipDeletedNm":       False,
                "availabilityFilters": filters,
                "limit":               1000,
                "offset":              0
            }
            resp_o = make_post(off_url, json=payload, headers=headers)
            regions = resp_o.json().get("data", {}).get("regions", [])

            wh_list: List[Dict] = []
            for reg in regions:
                m = reg.get("metrics", {})
                wh_list.extend([
                    {"warehouseName": "В пути до получателей",     "quantity": m.get("toClientCount", 0)},
                    {"warehouseName": "В пути возвраты на склад WB", "quantity": m.get("fromClientCount", 0)},
                    {"warehouseName": "Всего находится на складах",   "quantity": m.get("stockCount", 0)}
                ])
                for off in reg.get("offices", []):
                    wh_list.append({
                        "warehouseName": off.get("officeName"),
                        "quantity":      off.get("metrics", {}).get("stockCount", 0)
                    })

            warehouses_map[nm] = wh_list
            time.sleep(40)

    # 4) Собираем финальный DataFrame и записываем в БД
    entity_map = {"inter":"ИНТЕР","ut":"АТ","kravchik":"КРАВЧИК","pomazanova":"ПОМАЗАНОВА"}
    records = []
    for it in all_items:
        m  = it.get("metrics", {})
        nm = it["nmID"]
        records.append({
            "brand":                 it.get("brandName"),
            "subjectName":           it.get("subjectName"),
            "vendorCode":            it.get("vendorCode"),
            "nmId":                  nm,
            "barcode":               it.get("barcode"),
            "techSize":              it.get("techSize"),
            "volume":                m.get("volume", 0),
            "inWayToClient":         None,
            "inWayFromClient":       None,
            "quantityWarehousesFull": None,
            "date":                  start_str,
            "legal_entity":          entity_map[it["legal_entity"]],
            "warehouses":            json.dumps(warehouses_map.get(nm, []), ensure_ascii=False),
            "uuid":                  str(uuid.uuid4())
        })
    df = pd.DataFrame(records)

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    with conn, conn.cursor() as cur:
        sql = ('INSERT INTO wb_data (brand,"subjectName","vendorCode","nmId",barcode,"techSize",volume,'
               '"inWayToClient","inWayFromClient","quantityWarehousesFull",date,legal_entity,warehouses,uuid) VALUES %s')
        tuples = [(r["brand"], r["subjectName"], r["vendorCode"], int(r["nmId"]), r["barcode"], r["techSize"],
                   r["volume"], r["inWayToClient"], r["inWayFromClient"], r["quantityWarehousesFull"],
                   r["date"], r["legal_entity"], r["warehouses"], r["uuid"]) for r in records]
        execute_values(cur, sql, tuples)
    conn.close()

    print(f"[END] В базу wb_data вставили {len(records)} строк.")
    return df


if __name__ == '__main__':
    from python_service.app.config_loader import load_config
    cfg = load_config()
    wb_keys = cfg.get('wb_marketplace_keys', {})
    start = datetime.strptime('2025-05-24', '%Y-%m-%d')
    end   = datetime.strptime('2025-05-25', '%Y-%m-%d')
    cur   = start
    while cur <= end:
        d = cur.strftime('%Y-%m-%d')
        print(f"\n=== Обрабатываем {d} ===")
        fetch_wb_history_remains(wb_keys, d, d)
        cur += timedelta(days=1)
