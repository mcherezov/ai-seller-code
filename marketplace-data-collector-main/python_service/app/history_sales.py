from datetime import date, timedelta
import logging
import time
from environment import setup_logger
from db import (
    find_skipped_dates_grouped_by,
    load_newest_date,
    delete_existing_and_insert_sales_report,
    get_existing_legal_entities
)
from main import send_telegram_message, today_msk_datetime
from config_loader import load_avangard_seller
from sales_report.wb import fetch_wb_sales_report


def try_get_daily_data(date, log_prefix):
    """
    Пытается получить и записать отчет за указанную дату.
    Возвращает True при успешном выполнении хотя бы одной попытки.
    """
    config = load_avangard_seller()
    attempts = 0
    while attempts < 3:
        try:
            logging.info(f"{log_prefix} Дата {date}, попытка {attempts + 1}")

            df = fetch_wb_sales_report(
                config['wildberries_selenium'],
                config.get('entities_meta', {}),
                date
            )
            df['date'] = date

            if df is not None and not df.empty:
                # 1) Существующие юрлица в БД за эту дату
                existing = get_existing_legal_entities(
                    table_name='wb_sale_report',
                    date_column='date',
                    group_column='legal_entity',
                    date_value=date
                )
                # 2) Юрлица из DataFrame
                df_entities = set(df['legal_entity'].unique())
                # 3) Определяем, кого вставлять
                to_insert = df_entities - set(existing)

                if to_insert:
                    for entity in to_insert:
                        df_sub = df[df['legal_entity'] == entity].copy()
                        delete_existing_and_insert_sales_report(
                            df_sub,
                            table_name='wb_sale_report',
                            date_column='date',
                            group_column='legal_entity'
                        )
                        logging.info(f"{log_prefix} Вставлены данные за {date} для '{entity}'")
                else:
                    logging.info(f"{log_prefix} Нет новых юрлиц для вставки за {date}")

            else:
                msg = f"{log_prefix} Нет данных за {date}, пропускаем"
                logging.info(msg)
                current_hour = today_msk_datetime().hour
                if 12 <= current_hour <= 20:
                    send_telegram_message(msg)

            time.sleep(5)
            return True

        except Exception as e:
            attempts += 1
            logging.error(f"{log_prefix} Ошибка на попытке {attempts}: {e}", exc_info=True)
            time.sleep(60)

    return False


def main():
    setup_logger()
    log_prefix = "Выгрузка отчетов продаж WB."

    # жёстко задаём период
    start_date = date(2025, 5, 1)
    end_date   = date(2025, 5, 31)

    # 1) Пробегаем по всем датам диапазона
    current = start_date
    while current <= end_date:
        success = try_get_daily_data(current, log_prefix)
        if not success:
            err = f"{log_prefix} Дата {current}: не удалось после 3 попыток"
            logging.error(err)
            if 12 <= today_msk_datetime().hour <= 20:
                send_telegram_message(err)
        # шаг вперёд на один день
        current += timedelta(days=1)
        time.sleep(1)


if __name__ == '__main__':
    main()
