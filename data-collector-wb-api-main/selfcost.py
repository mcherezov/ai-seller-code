import pandas as pd
from google_sheets_utils import get_sheet_data, add_data_to_sheet_with_headers_and_format
from config_loader import load_config
from datetime import datetime
import asyncio
import asyncpg

CONFIG = load_config()

async def fetch_data_from_db(query: str, params: tuple):
    """Получение данных из PostgreSQL."""
    db_config = {
        "database": CONFIG["postgres_data_storage"]["database_name"],
        "user": CONFIG["postgres_data_storage"]["user"],
        "password": CONFIG["postgres_data_storage"]["password"],
        "host": CONFIG["postgres_data_storage"]["host"],
        "port": CONFIG["postgres_data_storage"]["port"]
    }
    try:
        conn = await asyncpg.connect(**db_config)
        rows = await conn.fetch(query, *params)
        await conn.close()
        return rows
    except asyncpg.PostgresError as e:
        raise Exception(f"Ошибка при работе с базой данных: {e}")

def transfer_selfcost_data(date_from: str, date_to: str, spreadsheet_id: str):
    """
    Получение данных с PostgreSQL с фильтрацией по диапазону дат и выгрузка в Google Sheets.

    :param date_from: Дата начала выборки в формате DD.MM.YYYY.
    :param date_to: Дата окончания выборки в формате DD.MM.YYYY.
    """
    try:
        start_date = datetime.strptime(date_from, "%d.%m.%Y").date()
        end_date = datetime.strptime(date_to, "%d.%m.%Y").date()
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    if start_date > end_date:
        raise Exception("Дата начала не может быть позже даты окончания.")

    query = """
        SELECT sku, date, cost_in_kopecks
        FROM cost
        WHERE date >= $1 AND date <= $2 AND seller_id = 1
    """

    async def process_data():
        rows = await fetch_data_from_db(query, (start_date, end_date))
        result = []
        for row in rows:
            result.append({
                'product_id': row['sku'],
                'date': row['date'].strftime('%d.%m.%Y'),
                'cost': row['cost_in_kopecks'] / 100
            })
        return result

    data = asyncio.run(process_data())
    if not data:
        print(f"Нет данных за период с {date_from} по {date_to}.")
        return

    df = pd.DataFrame(data)

    pivot_df = df.pivot(index='product_id', columns='date', values='cost').reset_index()

    add_data_to_sheet_with_headers_and_format(
        spreadsheet_id,
        pivot_df,
        "SelfCost(ext)",
        clear_range="B1:Z",
        start_col=2
    )
    print(f"Данные за период с {date_from} по {date_to} перенесены на вкладку SelfCost(ext) в таблице {spreadsheet_id}.")

if __name__ == "__main__":
    def test_transfer_selfcost_data():
        try:
            transfer_selfcost_data("16.12.2024", "18.12.2024", "1Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws")
            print("Данные успешно перенесены.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    test_transfer_selfcost_data()
