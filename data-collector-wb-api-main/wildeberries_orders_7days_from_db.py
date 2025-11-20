import sys
import argparse
import psycopg2
import logging
import pandas as pd
from config_loader import load_config
from google_sheets_utils import add_data_to_sheet_with_headers_and_format

# Настраиваем логирование
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

CONFIG = load_config()


def fetch_orders_from_db():
    """
    Получает заказы за последние 7 дней из БД, фильтруя их по юридическим лицам 'ИНТЕР' и 'АТ'.
    Если встречается неизвестное значение legal_entity, выбрасывает исключение.
    """
    db_config = {
        "database": CONFIG["postgres_data_storage"]["database_name"],
        "user": CONFIG["postgres_data_storage"]["user"],
        "password": CONFIG["postgres_data_storage"]["password"],
        "host": CONFIG["postgres_data_storage"]["host"],
        "port": CONFIG["postgres_data_storage"]["port"]
    }

    query = """
        SELECT date, "warehouseName", "supplierArticle", "nmId", "isCancel", legal_entity
        FROM wb_orders
        WHERE date >= CURRENT_DATE - INTERVAL '7 days' AND date < CURRENT_DATE
    """

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

        valid_entities = {"ИНТЕР", "АТ"}
        ignored_entities = {"КРАВЧИК", "ПОМАЗАНОВА", "АВАНГАРД"}  # Временно игнорируем "КРАВЧИК"

        # Проверяем уникальные значения legal_entity во всех данных
        unique_entities = {row[-1] for row in rows}
        valid_entities = {"ИНТЕР", "АТ"}
        ignored_entities = {"КРАВЧИК", "ПОМАЗАНОВА", "АВАНГАРД"}

        unknown_entities = unique_entities - valid_entities - ignored_entities
        if unknown_entities:
            raise ValueError(f"Обнаружены неизвестные значения legal_entity: {unknown_entities}")

        # Разделяем данные по юрлицам
        orders_inter = [row[:-1] for row in rows if row[-1] == "ИНТЕР"]
        orders_ut = [row[:-1] for row in rows if row[-1] == "АТ"]

        logging.info(
            f"Успешно получены данные из БД: {len(orders_inter)} заказов для ИНТЕР, {len(orders_ut)} заказов для АТ."
        )

        return orders_inter, orders_ut

    except Exception as e:
        logging.error(f"Ошибка при получении данных из БД: {e}", exc_info=True)
        raise


def convert_to_dataframe(orders):
    """
    Преобразует список заказов в DataFrame.

    :param orders: Список кортежей с заказами для определенного юрлица.
    :return: DataFrame с заказами.
    """

    columns = ["date", "warehouseName", "supplierArticle", "nmId", "isCancel"]
    df = pd.DataFrame(orders, columns=columns)

    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])

    return df


def sync_wb_orders_7days_from_db(spreadsheet_id, clear_range, start_col):
    """
    Получает заказы за 7 дней из БД, преобразует их в DataFrame и загружает в Google Sheets.

    :param spreadsheet_id: str - ID Google Sheets.
    :param clear_range: str - Диапазон для очистки.
    :param start_col: int - Начальный столбец для вставки данных.
    """
    try:
        logging.info(f"Начало синхронизации данных в Google Sheets (ID: {spreadsheet_id})...")

        # Получаем данные из БД
        orders_inter, orders_ut = fetch_orders_from_db()

        # Преобразуем в DataFrame
        df_inter = convert_to_dataframe(orders_inter)
        df_ut = convert_to_dataframe(orders_ut)

        # Маппинг названий вкладок
        sheet_mapping = {
            "df_inter": "ИНТЕР заказы",
            "df_ut": "АТ заказы"
        }

        # Словарь с данными для загрузки
        dataframes = {"df_inter": df_inter, "df_ut": df_ut}

        # Проходимся по DataFrame и загружаем в Google Sheets
        for key, df in dataframes.items():
            sheet_name = sheet_mapping.get(key)
            if not sheet_name:
                logging.warning(f"Лист для {key} не найден в маппинге, пропускаем.")
                continue

            if df.empty:
                logging.warning(f"Пропускаем загрузку {sheet_name}, так как DataFrame пуст.")
                continue

            add_data_to_sheet_with_headers_and_format(
                spreadsheet_id, df, sheet_name, clear_range, start_col
            )
            logging.info(f"Данные для {sheet_name} загружены. Кол-во строк: {len(df)}")

        logging.info("Синхронизация данных в Google Sheets завершена успешно.")
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных в Google Sheets: {e}", exc_info=True)


if __name__ == "__main__":
    # Для теста
    sync_wb_orders_7days_from_db(spreadsheet_id="11Gjx1UFXW0nZPUiDIrzNedBiJad1B__YQmTjH0uTSnws",
                                 clear_range="A1:E",
                                 start_col=1
                                 )
