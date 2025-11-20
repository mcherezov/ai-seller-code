import os
import sys
import pandas as pd
from psycopg2 import connect
from functools import wraps
from typing import Callable
from datetime import date
from dotenv import load_dotenv

load_dotenv()

# Параметры подключения
DB_PARAMS = {
    'dbname':   os.getenv('DB_NAME'),
    'user':     os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host':     os.getenv('DB_HOST'),
    'port':     os.getenv('DB_PORT'),
}

def inject_closable_cursor(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        conn = connect(**DB_PARAMS)
        try:
            with conn.cursor() as cur:
                out = func(*args, cursor=cur, **kwargs)
                conn.commit()
                return out
        except:
            conn.rollback()
            raise
        finally:
            conn.close()
    return wrapper

@inject_closable_cursor
def save_cost_data(df: pd.DataFrame, **kwargs):
    cur = kwargs['cursor']
    sql = """
    INSERT INTO cost (sku, date, cost_in_kopecks, seller_id)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (sku, date)
      DO UPDATE SET cost_in_kopecks = EXCLUDED.cost_in_kopecks;
    """
    for sku, dt, cost, seller in df[['sku','date','cost_in_kopecks','seller_id']].itertuples(index=False):
        cur.execute(sql, (sku, dt, cost, seller))

def main():
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        print("Usage: python cost_for_seller2.py path/to/file.xlsx")
        sys.exit(1)

    # 1) читаем Excel, пропуская первую строку с мусором
    df = pd.read_excel(path, engine='openpyxl')
    print("КОЛОНКИ ФАЙЛА:", df.columns.tolist())

    # 2) переименовываем столбцы под наши
    df = df.rename(columns={
        'Артикул поставщика': 'sku',
        'Цена закупки, руб':  'cost_rub'
    })

    # 3) убираем строки без цены (на всякий случай)
    df = df.dropna(subset=['cost_rub'])

    # 4) пересчитываем рубли в копейки и ставим дату 1 мая 2025
    df['cost_in_kopecks'] = (df['cost_rub'] * 100).round().astype(int)
    df['date']            = date(2025, 5, 1)
    df['seller_id']       = 2

    # 5) сохраняем в БД
    save_cost_data(df)
    print(f"✅ Загружено {len(df)} строк для seller_id=2 на 2025-05-01")


if __name__ == '__main__':
    main()
