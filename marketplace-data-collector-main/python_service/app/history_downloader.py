from datetime import timedelta
import datetime
import logging
import time
import traceback
from environment import setup_logger
from sales_funnel.wb import fetch_wb_sales_funnel
from db import delete_previous_and_insert_new_postgres_table, find_skipped_dates_and_hours_grouped_by, load_oldest_date_and_hour
from main import send_telegram_message, today_msk_datetime
from config_loader import load_config


def try_get_day_data(date, hour, log_prefix):
    config = load_config()
    try_count = 0
    while try_count < 3:
        try:
            logging.info(f"Дата {date} час {hour}, начало попытки {try_count + 1}")

            df = fetch_wb_sales_funnel(config['wb_marketplace_keys'], date, hour)
            df['date'] = date
            df['hour'] = hour
            if df is not None and not df.empty:
                delete_previous_and_insert_new_postgres_table(df, 'wb_sales_funnel', hour_column='hour')
                logging.info("Успешно обновлены или вставлены данные в таблице wb_sales_funnel.")
            else:
                message = f"{log_prefix} Нет данных за дату {date} час {hour} для добавления в wb_sales_funnel, пропускаем и продолжаем без них"
                logging.info(message)
                current_hour = today_msk_datetime().hour
                if current_hour >= 12 and current_hour <= 20:
                    send_telegram_message(message)
            time.sleep(10)
            return True
        except Exception as e:
            try_count += 1
            traceback.print_exc()
            error_message = f"Дата {date} час {hour}, попытка {try_count}. Ошибка: {e}"
            logging.error(error_message)
            time.sleep(60 * 1)
    return False


def main():
    log_prefix = 'Историческое скачивание.'
    skipped_legal_names_and_dates_and_hours = find_skipped_dates_and_hours_grouped_by('wb_sales_funnel', 'legal_entity')
    current_hour = today_msk_datetime().hour
    for date, hour in skipped_legal_names_and_dates_and_hours:
        is_success = try_get_day_data(date, hour, log_prefix)

        if not is_success:
            error_message = f"{log_prefix} Дата {date} час {hour}. Остановка попыток, переход к следующей дате"
            logging.error(error_message)
            if current_hour >= 12 and current_hour <= 20:
                send_telegram_message(error_message)

    selected_date, selected_hour = load_oldest_date_and_hour('wb_sales_funnel')

    if selected_date is None:
        selected_date = today_msk_datetime().date()
    if selected_hour is None:
        selected_hour = today_msk_datetime().hour
    selected_hour -= 1
    if selected_hour < 0:
        selected_hour = 23
        selected_date -= timedelta(days=1)

    while True:
        current_hour = today_msk_datetime().hour

        is_success = try_get_day_data(selected_date, selected_hour, log_prefix)

        if not is_success:
            error_message = f"{log_prefix} Дата {selected_date} час {selected_hour}. Остановка попыток, переход к предыдущему часу."
            logging.error(error_message)
            if 12 <= current_hour <= 20:
                send_telegram_message(error_message)

        # Шаг назад на 1 час
        selected_hour -= 1
        if selected_hour < 0:  # Если час ушел в минус, переходим на предыдущий день
            selected_hour = 23
            selected_date -= timedelta(days=1)


if __name__ == '__main__':
    setup_logger()
    main()
