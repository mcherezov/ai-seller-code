#!/usr/bin/env python3
import os
import sys
import pandas as pd
from psycopg2 import connect
from functools import wraps
from typing import Callable
from datetime import date
from dotenv import load_dotenv
import calendar

# Подгружаем переменные окружения из .env
load_dotenv()

# Словарь для парсинга русских месяцев
MONTHS = {
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
    'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
    'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
}

def parse_year_month(s: str):
    """
    Возвращает кортеж (year, month) из строки формата "май 2025".
    """
    month_str, year_str = s.strip().split()
    return int(year_str), MONTHS[month_str.lower()]

# Параметры подключения к БД из окружения
DB_PARAMS = {
    'dbname':   os.getenv('DB_NAME'),
    'user':     os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host':     os.getenv('DB_HOST'),
    'port':     os.getenv('DB_PORT'),
}

def inject_closable_cursor(func: Callable):
    """Декоратор для управления подключением и транзакцией"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        conn = connect(**DB_PARAMS)
        try:
            with conn.cursor() as cur:
                result = func(*args, cursor=cur, **kwargs)
                conn.commit()
                return result
        except:
            conn.rollback()
            raise
        finally:
            conn.close()
    return wrapper

@inject_closable_cursor
def save_cost_data(data: pd.DataFrame, **kwargs):
    """
    Вставляет или обновляет строки в таблице cost.
    Ожидает в `data` колонки: sku, date, cost_in_kopecks, seller_id.
    """
    cur = kwargs['cursor']
    insert_sql = """
        INSERT INTO cost (sku, date, cost_in_kopecks, seller_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (sku, date, seller_id)
          DO UPDATE SET cost_in_kopecks = EXCLUDED.cost_in_kopecks;
    """
    for sku, dt, cost, seller in data[['sku','date','cost_in_kopecks','seller_id']].itertuples(index=False):
        cur.execute(insert_sql, (sku, dt, cost, seller))


def main():
    # Получаем путь к файлу из аргумента
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        print("Usage: python cost.py /path/to/file.csv_or_xlsx")
        sys.exit(1)

    # Чтение исходника: Excel или CSV
    if path.lower().endswith(('.xls', '.xlsx')):
        # Ищем лист "Август" (на случай пробелов/года в названии)
        xls = pd.ExcelFile(path, engine='openpyxl')
        sheet = next((s for s in xls.sheet_names if 'август' in s.lower()), None)
        if sheet is None:
            raise RuntimeError("Не найден лист с названием, содержащим 'Август'")

        # Берём только нужные столбцы: A(Период), B(Артикул), C(Описание) и F(себестоимость)
        raw = pd.read_excel(path, sheet_name=sheet, engine='openpyxl',
                            header=1, usecols="A:C,F")

        # В этой книге первая строка после header — фактические заголовки
        new_header = raw.iloc[0].tolist()
        df = raw.iloc[1:].reset_index(drop=True)
        df.columns = new_header

        # Последний столбец = F (себестоимость за август), имя у него динамическое → берём как last_col
        last_col = df.columns[-1]

        # Приводим к нужным именам и оставляем только то, что нужно
        df = (df.rename(columns={
            'Период': 'period',
            'Описание': 'sku',
            last_col: 'cost_rub'
        })[['period', 'sku', 'cost_rub']]
              .dropna(subset=['period', 'sku', 'cost_rub']))
    else:
        raise RuntimeError("Ожидался Excel-файл с листом Август")

    # Преобразование cost_rub в число
    df['cost_rub'] = (df['cost_rub'].astype(str)
                      .str.replace(r'\s+', '', regex=True)
                      .str.replace(',', '.', regex=False)
                      .astype(float))

    # Парсинг года и месяца из 'август 2025'
    df[['year', 'month']] = df['period'].apply(
        lambda s: pd.Series(parse_year_month(s), index=['year', 'month'])
    )

    # Пересчёт в копейки
    df['cost_in_kopecks'] = (df['cost_rub'] * 100).round().astype(int)

    # seller_id как раньше (или подставь свой способ получения)
    seller_id = 1

    # Генерация записей на каждый день месяца
    records = []
    for _, row in df.iterrows():
        days = calendar.monthrange(row['year'], row['month'])[1]
        for d in range(1, days + 1):
            records.append({
                'sku': row['sku'],
                'date': date(row['year'], row['month'], d),
                'cost_in_kopecks': row['cost_in_kopecks'],
                'seller_id': seller_id,
            })
    full_df = pd.DataFrame(records)

    # Сохранение в БД
    save_cost_data(full_df)
    print(f"✅ Загружено {len(full_df)} строк за {sheet} (seller_id={seller_id}).")


if __name__ == '__main__':
    main()