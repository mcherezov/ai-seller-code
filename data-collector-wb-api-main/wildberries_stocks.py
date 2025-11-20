import asyncio
import datetime
import json
import asyncpg
import pandas as pd
from google_sheets_utils import add_data_to_sheet_without_headers_with_format, clear_range
from config_loader import load_config


CONFIG = load_config()


async def fetch_data_from_db(query: str, params: tuple):
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


def fetch_stocks(date_from: str, date_to: str, seller_legal: str, spreadsheet_id: str):
    try:
        formatted_date_from = datetime.datetime.strptime(date_from, "%d.%m.%Y").date()
        formatted_date_to = datetime.datetime.strptime(date_to, "%d.%m.%Y").date()
    except ValueError as e:
        raise Exception(f"Неверный формат даты. Ожидается DD.MM.YYYY. {str(e)}")

    entity_map = {
        "inter": "ИНТЕР",
        "ut": "АТ",
        "kravchik": "КРАВЧИК"
    }
    entity_text = entity_map.get(seller_legal.lower())
    if not entity_text:
        raise ValueError(f"Неизвестное юридическое лицо: {seller_legal}")

    query = "SELECT * FROM wb_data WHERE date >= $1 AND date <= $2 AND legal_entity = $3"

    async def process_data():
        rows = await fetch_data_from_db(query, (formatted_date_from, formatted_date_to, entity_text))
        return [
            {key: (value.strftime('%d.%m.%Y') if isinstance(value, datetime.date) else value)
             for key, value in row.items()}
            for row in rows
        ]

    result = asyncio.run(process_data())
    df = pd.DataFrame(result)


    if df.empty:
        print("Нет данных для выбранного периода.")
        return

    # Добавление и заполнение недостающих столбцов
    df['Дата и время обновления информации в сервисе'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['Склад'] = ''
    df['Скидка'] = ''
    df['Договор поставки (внутренние технологические данные)'] = ''
    df['Договор реализации (внутренние технологические данные)'] = ''
    df['Код контракта (внутренние технологические данные)'] = ''
    df['Категория'] = ''
    df['Цена'] = ''
    df['Полное (непроданное) количество, которое числится за складом (с оплатой + в пути)'] = ''

    df.rename(columns={
        'brand': 'Бренд',
        'subjectName': 'Предмет',
        'vendorCode': 'Артикул продавца',
        'nmId': 'Артикул WB',
        'barcode': 'Баркод',
        'date': 'Дата Добавления',
        'volume': 'Размер',
    }, inplace=True)

    df['Дата и время обновления информации в сервисе'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['period_id'] = pd.to_datetime(df['Дата Добавления'], format="%d.%m.%Y", errors='coerce').dt.strftime("%d.%m.%Y")

    # --- Разворачивание warehouses ---
    def expand_warehouses(row):
        warehouses_raw = row.get('warehouses', '[]')
        if isinstance(warehouses_raw, str):
            warehouses_raw = warehouses_raw.replace("'", '"')

        try:
            warehouses = json.loads(warehouses_raw)
        except json.JSONDecodeError:
            warehouses = []

        in_way_to = sum(wh["quantity"] for wh in warehouses if wh["warehouseName"] == "В пути до получателей")
        in_way_from = sum(wh["quantity"] for wh in warehouses if wh["warehouseName"] == "В пути возвраты на склад WB")
        available_qty = sum(wh["quantity"] for wh in warehouses if wh["warehouseName"] == "Всего находится на складах")

        real_warehouses = [
            wh for wh in warehouses if wh["warehouseName"] not in {
                "В пути до получателей",
                "В пути возвраты на склад WB",
                "Всего находится на складах"
            }
        ]

        rows = []
        for wh in real_warehouses:
            new_row = row.copy()
            new_row["Склад"] = wh.get("warehouseName", "")
            new_row["Количество"] = wh.get("quantity", 0)
            new_row["В пути к клиенту"] = in_way_to
            new_row["В пути от клиента"] = in_way_from
            new_row["Количество, доступное для продажи (сколько можно добавить в корзину)"] = wh.get("quantity", 0)
            rows.append(new_row)
        return pd.DataFrame(rows)

    expanded_df = pd.concat(df.apply(expand_warehouses, axis=1).tolist(), ignore_index=True)

    if 'warehouses' in expanded_df.columns:
        expanded_df.drop(columns=['warehouses'], inplace=True)

    # Удалим потенциальные лишние поля
    for col in ['Название склада', 'Количество на складе']:
        if col in expanded_df.columns:
            expanded_df.drop(columns=[col], inplace=True)

    column_order = [
        'Дата и время обновления информации в сервисе',
        'Склад',
        'Артикул продавца',
        'Артикул WB',
        'Баркод',
        'Количество, доступное для продажи (сколько можно добавить в корзину)',
        'В пути к клиенту',
        'В пути от клиента',
        'Полное (непроданное) количество, которое числится за складом (с оплатой + в пути)',
        'Категория',
        'Предмет',
        'Бренд',
        'Размер',
        'Цена',
        'Скидка',
        'Договор поставки (внутренние технологические данные)',
        'Договор реализации (внутренние технологические данные)',
        'Код контракта (внутренние технологические данные)',
        'legal_entity',
        'Дата Добавления',
        'period_id'
    ]

    # expanded_df['Название склада'] = expanded_df['Артикул продавца']

    for col in column_order:
        if col not in expanded_df.columns:
            expanded_df[col] = ''

    expanded_df = expanded_df[column_order]
    clear_range(spreadsheet_id, "StocksReport(ext)", "A2:V")
    try:
        add_data_to_sheet_without_headers_with_format(spreadsheet_id, expanded_df, "StocksReport(ext)")
        print("✅ Данные успешно добавлены на вкладку StocksReport(ext).")
    except Exception as e:
        raise Exception(f"Ошибка при добавлении данных в Google Sheets: {e}")


if __name__ == "__main__":
    def test_fetch_stocks():
        try:
            fetch_stocks("26.03.2025", "26.03.2025", "inter", "16O51ACdKhs0HxAnu1AaVCvxguoYban2D8WEzL92toGY")
            print("Тест успешно завершен.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    test_fetch_stocks()
