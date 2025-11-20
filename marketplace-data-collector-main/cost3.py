#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
from pathlib import Path
import os
from datetime import date
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# Настройка
# -----------------------------------------------------------------------------
load_dotenv()  # подхватим .env из текущей рабочей директории

DB_PARAMS = {
    'dbname':   os.getenv('DB_NAME'),
    'user':     os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host':     os.getenv('DB_HOST'),
    'port':     os.getenv('DB_PORT'),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


# -----------------------------------------------------------------------------
# Парсинг аргументов
# -----------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Загрузчик себестоимости в таблицу cost"
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Путь к Excel-файлу с колонками 'Артикул Продавца' и "
             "'себестоимость с ндс, плюс транспортные затраты 5%'"
    )
    parser.add_argument(
        "--seller-id",
        type=int,
        default=3,
        help="ID продавца в базе (по умолчанию 3)"
    )
    parser.add_argument(
        "--start",
        type=lambda s: date.fromisoformat(s),
        default=date(2025, 6, 1),
        help="Дата начала (YYYY-MM-DD), по умолчанию 2025-06-01"
    )
    parser.add_argument(
        "--end",
        type=lambda s: date.fromisoformat(s),
        default=date(2025, 6, 30),
        help="Дата конца (YYYY-MM-DD), по умолчанию 2025-06-30"
    )
    return parser.parse_args()


# -----------------------------------------------------------------------------
# Чтение и подготовка DataFrame
# -----------------------------------------------------------------------------
def read_and_clean(path: Path) -> pd.DataFrame:
    if not path.exists():
        logging.error("Файл не найден: %s", path)
        raise FileNotFoundError(path)

    logging.info("Читаем Excel-файл: %s", path)
    df = pd.read_excel(path, engine="openpyxl")

    # Переименуем нужные колонки
    df = df.rename(columns={
        "Артикул Продавца": "sku",
        "себестоимость с ндс, плюс транспортные затраты 5%": "cost_rub"
    })

    # Отфильтруем мусор
    df = df.dropna(subset=["sku", "cost_rub"])
    df = df[df["sku"].astype(str) != "#N/A"]

    # Переведём рубли в копейки
    df["cost_in_kopecks"] = (df["cost_rub"] * 100).round().astype(int)
    logging.info("Оставлено SKU: %d", len(df))
    return df[["sku", "cost_in_kopecks"]]


# -----------------------------------------------------------------------------
# Генерация полных строк (SKU × даты)
# -----------------------------------------------------------------------------
def expand_dates(
    base: pd.DataFrame,
    start_date: date,
    end_date: date,
    seller_id: int
) -> pd.DataFrame:
    logging.info("Генерируем даты с %s по %s", start_date, end_date)
    all_dates = pd.date_range(start=start_date, end=end_date, freq="D")
    dates_df = pd.DataFrame({"date": all_dates})

    # cross join через общий ключ
    base = base.assign(_tmp=1)
    dates_df = dates_df.assign(_tmp=1)
    full = base.merge(dates_df, on="_tmp").drop(columns="_tmp")

    full["seller_id"] = seller_id
    logging.info("Итого строк для вставки: %d", len(full))
    return full[["sku", "date", "cost_in_kopecks", "seller_id"]]


# -----------------------------------------------------------------------------
# Загрузка в базу
# -----------------------------------------------------------------------------
def insert_batch(data: pd.DataFrame):
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        with conn, conn.cursor() as cur:
            sql = """
            INSERT INTO cost (sku, date, cost_in_kopecks, seller_id)
            VALUES %s
            """
            # execute_values быстро вставляет сразу много строк
            execute_values(
                cur, sql,
                list(data.itertuples(index=False, name=None)),
                page_size=1000
            )
            logging.info("Успешно вставлено %d строк", len(data))
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Основной запуск
# -----------------------------------------------------------------------------
def main():
    args = parse_args()
    try:
        df_base = read_and_clean(args.path)
        df_full = expand_dates(df_base, args.start, args.end, args.seller_id)
        insert_batch(df_full)
    except Exception as e:
        logging.exception("Ошибка при загрузке себестоимости:")
        sys.exit(1)


if __name__ == "__main__":
    main()
