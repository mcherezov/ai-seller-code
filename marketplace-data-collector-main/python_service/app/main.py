from datetime import datetime, timedelta
import shutil
import traceback
import zipfile
import json
import pytz
import requests
import os
import pandas as pd
from ad_campaign.ozon import fetch_ozon_ad_campaign_statistics as fetch_ozon_ad_campaign_statistics_api
from ad_campaign.ozon_front import fetch_ozon_ad_campaign_statistics as fetch_ozon_ad_campaign_statistics_front
from order.wb import fetch_wb_orders_data
from environment import setup_logger
from localization_report.wb import fetch_wb_localization_index_report
from ad_campaign.wb import fetch_wb_ad_campaign
from sales_funnel.wb import fetch_wb_sales_funnel
from search_text.wb import fetch_wb_search_stats
from stock.betapro import fetch_betapro_data
from stock.wb import fetch_wb_data
from commission.wb import fetch_commission_data
from paid_receiving.wb import fetch_paid_acceptance_data
from paid_storage.wb import fetch_paid_storage_data
from autobidder_scripts.wb_keyword_stats import fetch_keywords_stats
from autobidder_scripts.mpm_competitors_parser import fetch_mpm_competitors
from stock.ozon_new import fetch_ozon_data
from posting.ozon import fetch_ozon_posting_data
from sales_report.wb import fetch_wb_sales_report
from sales_funnel.ozon import fetch_ozon_sales_funnel
from cluster.wb import fetch_cluster_stats
from db import (delete_previous_and_insert_new_postgres_table, load_data, load_current_day_or_latest_cost_sku,
                load_current_day_or_latest_cost_mp_ids, load_newest_date_and_hour, records_presented_at, \
                fill_missing_dates_by_previous_day_data, fill_missing_dates_by_none, load_newest_date, \
                load_newest_date_with_all_values, load_latest_advert_ids, delete_previous_and_insert_new_dest_table)
from config_loader import load_all_configs


def today_msk_datetime():
    moscow_tz = pytz.timezone('Europe/Moscow')
    return datetime.now(moscow_tz)


def backup_table(table_name, backup_folder):
    df = load_data(table_name)
    output_file_name = os.path.join(backup_folder, table_name + '.csv')
    df.to_csv(output_file_name, index=False)


def compress_and_remove(folder_path):
    if not os.path.isdir(folder_path):
        print(f"Ошибка: '{folder_path}' не является папкой.")
        return

    archive_name = f"{folder_path}.zip"
    with zipfile.ZipFile(archive_name, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, start=folder_path)
                archive.write(file_path, arcname)

    shutil.rmtree(folder_path)


def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{os.getenv('TELEGRAM_BOT_TOKEN')}/sendMessage"
    payload = {
        'chat_id': os.getenv('TELEGRAM_CHAT_ID'),
        'text': message
    }
    requests.post(url, data=payload)


def check_cost_data_availability(receivers, deadline_days=5):
    current_datetime = today_msk_datetime()

    latest_date = load_newest_date(table_name='cost')

    if latest_date:
        days_to_deadline = (latest_date - current_datetime.date()).days

        if days_to_deadline <= deadline_days:
            if days_to_deadline < 0:
                message = (
                    f"{receivers} ❌ Данные себестоимости устарели! Последняя доступная дата: {latest_date}.\n"
                    "Необходимо срочно внести данные для следующего периода в БД!"
                )
                send_telegram_message(message)
            else:
                message = (
                    f"{receivers} ⚠️ Значения себестоимости товаров в БД содержатся до {latest_date}, "
                    f"осталось {days_to_deadline} дней. Необходимо внести данные для следующего периода"
                )
                if current_datetime.hour == 12 and current_datetime.weekday() not in [5, 6]:
                    send_telegram_message(message)
    else:
        send_telegram_message(f"{receivers} В таблице cost отсутствуют данные! Необходимо заполнить БД")


def main():
    receivers = '@Vova_Villa @skatset @anyaa_touchin'
    check_cost_data_availability(receivers)

    try:
        fill_missing_dates_by_previous_day_data('cost')
        fill_missing_dates_by_none(
            'betapro_data',
            none_fields=['qnt', 'qnt2', 'qnt3', 'qnt4', 'qnt5', 'qnt6', 'qnt7', 'qnt8', 'Свободный остаток']
        )
        fill_missing_dates_by_none(
            'wb_data',
            none_fields=['inWayToClient', 'inWayFromClient', 'quantityWarehousesFull']
        )
        fill_missing_dates_by_none(
            'ozon_data',
            none_fields=['valid_stock_count', 'waitingdocs_stock_count', 'expiring_stock_count', 'defect_stock_count'],
            empty_list_fields=['warehouses']
        )
    except:
        traceback.print_exc()
        error_message = f"Ошибка при восстановлении данных"
        print(error_message)

    all_configs = load_all_configs()

    today_datetime = today_msk_datetime()
    current_hour = today_datetime.hour
    today = today_datetime.date()

    all_wb_stock_dfs = []
    all_wb_order_dfs = []
    all_wb_sale_reports = []
    all_wb_commission_dfs = []

    # Проходимся по каждому селлеру и выполняем необходимые сборы данных
    for seller_code, config in all_configs.items():
        print(f"\n=== Обработка селлера {seller_code} ===")

        # 1) Betapro - если есть конфигурация, собираем данные
        if config.get('betapro_partners'):
            try:
                df = fetch_betapro_data(config['betapro_partners'])  # запрос данных от Betapro
                df['date'] = today  # добавляем дату
                delete_previous_and_insert_new_postgres_table(df, 'betapro_data')  # сохраняем в БД
                print("Успешно обновлены или вставлены данные в таблице betapro_data.")
            except Exception as e:
                traceback.print_exc()
                msg = f"[{seller_code}] Ошибка при получении остатков на складе для betapro_data: {e}"
                print(msg)
                if current_hour >= 12 and not records_presented_at('betapro_data', today):
                    send_telegram_message(msg)
        else:
            print(f"[{seller_code}] Пропускаем betapro - нет конфигурации")

        # 2) Wildberries stock — остатки
        if config.get('wb_marketplace_keys'):
            try:
                df = fetch_wb_data(config['wb_marketplace_keys'], config['entities_meta'])
                df['date'] = today
                all_wb_stock_dfs.append(df)
                print(f"[{seller_code}] получено {len(df)} строк wb_data.")
            except Exception as e:
                traceback.print_exc()
                msg = f"[{seller_code}] Ошибка при получении wb_data: {e}"
                print(msg)
                if current_hour >= 12 and not records_presented_at('wb_data', today):
                    send_telegram_message(msg)
        else:
            print(f"[{seller_code}] Пропускаем wb_data — нет конфигурации wb_marketplace_keys")

        # 3) Wildberries commission - комиссия
        if config.get('wb_marketplace_keys'):
            try:
                df = fetch_commission_data(config, config['entities_meta'])
                if df is not None and not df.empty:
                    all_wb_commission_dfs.append(df)
                    print(f"[{seller_code}] получено {len(df)} строк wb_commission.")
                else:
                    print(f"[{seller_code}] wb_commission: нет данных.")
            except Exception as e:
                traceback.print_exc()
                msg = f"[{seller_code}] Ошибка при получении комиссии wb_commission: {e}"
                print(msg)
                if current_hour >= 12:
                    send_telegram_message(msg)

        # 4) Ozon stock - остатки
        if config.get('ozon_partners'):
            try:
                sku_dict, date = load_current_day_or_latest_cost_mp_ids()
                if not sku_dict:
                    print(f"[{seller_code}] ❌ Нет mp_id для Ozon")
                else:
                    for entity, skus in sku_dict.items():
                        print(f"{entity}: {len(skus)} шт.")
                df = fetch_ozon_data(sku_dict, config['ozon_partners'])
                df['date'] = today
                delete_previous_and_insert_new_postgres_table(df, 'ozon_data')
                print("Успешно обновлены или вставлены данные в таблице ozon_data")
            except Exception as e:
                traceback.print_exc()
                msg = f"[{seller_code}] Ошибка при получении остатков на складе для ozon_data: {e}"
                print(msg)
                if current_hour >= 12 and not records_presented_at('ozon_data', today):
                    send_telegram_message(msg)
        else:
            print(f"[{seller_code}] Пропускаем ozon_data — нет конфигурации")

        # 5) Wildberries orders — заказы
        if config.get('wb_marketplace_keys'):
            try:
                last_date = load_newest_date(table_name='wb_orders')
                # Всегда перечитываем «сегодняшний» день, если он уже был загружен ранее
                if last_date == today:
                    start_date = today
                elif last_date:
                    start_date = last_date + timedelta(days=1)
                else:
                    start_date = today
                date_cursor = start_date

                while date_cursor <= today:
                    try:
                        df = fetch_wb_orders_data(
                            config['wb_marketplace_keys'],
                            config['entities_meta'],
                            date_cursor
                        )
                        if df is not None and not df.empty:
                            df['date'] = date_cursor
                            all_wb_order_dfs.append(df)
                            print(f"[{seller_code}] Собрали wb_orders за {date_cursor}, rows={len(df)}")
                        else:
                            print(f"[{seller_code}] wb_orders: нет данных за {date_cursor}")
                    except Exception as e_day:
                        traceback.print_exc()
                        msg = f"[{seller_code}] Ошибка fetch_wb_orders_data за {date_cursor}: {e_day}"
                        print(msg)
                        if current_hour >= 12 and not records_presented_at('wb_orders', date_cursor):
                            send_telegram_message(msg)
                    finally:
                        date_cursor += timedelta(days=1)

            except Exception as e:
                traceback.print_exc()
                error_msg = f"[{seller_code}] Глобальная ошибка сборки wb_orders: {e}"
                print(error_msg)
                if current_hour >= 12:
                    send_telegram_message(error_msg)
        else:
            print(f"[{seller_code}] Пропускаем wb_orders — нет ключей WB")

        # # 6) Wildberries keyword stats (Autobidder)
        # if config.get('wb_marketplace_keys'):
        #     try:
        #         advert_ids_by_legal_entity = {
        #             "inter": [21470364, 22501210, 22500426]
        #         }
        #         df = fetch_keywords_stats(config["wb_marketplace_keys"], advert_ids_by_legal_entity, today_datetime,
        #                                   today_datetime)
        #         if not df.empty:
        #             delete_previous_and_insert_new_postgres_table(df, 'wb_keyword_stats', hour_column='hour')
        #             print("Успешно обновлены или вставлены данные в таблице wb_keyword_stats")
        #         else:
        #             print("Статистика по ключевым фразам отсутствует, выгрузка пропущена.")
        #     except Exception as e:
        #         traceback.print_exc()
        #         msg = f"[{seller_code}] Ошибка при получении данных по ключевым фразам wb_keyword_stats: {e}"
        #         print(msg)
        #         if current_hour >= 12:
        #             send_telegram_message(msg)
        # else:
        #     print(f"[{seller_code}] Пропускаем wb_keyword_stats — нет ключей WB")

        # # 7) Wildberries cluster stats — только в окне 07:00–07:59 МСК
        # # if 7 <= current_hour < 8:
        # if config.get('wb_marketplace_keys'):
        #     for seller_legal, api_key in config['wb_marketplace_keys'].items():
        #         try:
        #             advert_ids = load_latest_advert_ids('wb_campaign_stats_5min', seller_legal)
        #             if not advert_ids:
        #                 print(f"[{seller_code}/{seller_legal}] Нет advert_id, пропускаем.")
        #                 continue
        #
        #             df_clusters = fetch_cluster_stats(
        #                 {seller_legal: api_key},
        #                 {seller_legal: advert_ids},
        #                 today
        #             )
        #
        #             if not df_clusters.empty:
        #                 delete_previous_and_insert_new_dest_table(df_clusters, 'cluster_stats')
        #                 print(f"[{seller_code}/{seller_legal}] cluster_stats обновлён, rows={len(df_clusters)}")
        #             else:
        #                 print(f"[{seller_code}/{seller_legal}] Нет данных для cluster_stats")
        #
        #         except Exception as e:
        #             traceback.print_exc()
        #             msg = f"[{seller_code}/{seller_legal}] Ошибка при сборе cluster_stats: {e}"
        #             print(msg)
        #             if current_hour >= 12:
        #                 send_telegram_message(msg)
        # else:
        #     print(f"[{seller_code}] Пропускаем cluster_stats — нет ключей WB")
        # # else:
        # #     print(
        # #         f"[{seller_code}] Сейчас {today_datetime.strftime('%H:%M')} МСК — блок cluster_stats работает только в 07:00–08:00")

        # 8) Ozon posting - статусы размещения
        if config.get('ozon_partners'):
            try:
                df = fetch_ozon_posting_data(config['ozon_partners'], today)  # запрос posting Ozon
                df['date'] = today
                delete_previous_and_insert_new_postgres_table(df, 'ozon_posting')
                print("Успешно обновлены или вставлены данные в таблице ozon_posting.")
            except Exception as e:
                traceback.print_exc()
                msg = f"[{seller_code}] Ошибка при получении остатков на складе для ozon_posting: {e}"
                print(msg)
                if current_hour >= 12 and not records_presented_at('ozon_posting', today):
                    send_telegram_message(msg)
        else:
            print(f"[{seller_code}] Пропускаем ozon_posting — нет конфигурации")

        # 9) WB search text - поисковые запросы
        if config.get('wb_marketplace_keys', {}).get('inter'):
            try:
                sku_df = load_data('sku')
                nm_ids = sku_df[
                    (sku_df.legal_entity == "ИНТЕР") & (sku_df.marketplace_name == "Wildberries")
                ].mp_id.dropna().astype(int).tolist()
                df = fetch_wb_search_stats(config['wb_marketplace_keys']['inter'], today, nm_ids)
                df["legal_entity"] = "ИНТЕР"
                if not df.empty:
                    delete_previous_and_insert_new_postgres_table(df, 'wb_search_text')
                    print("Успешно обновлены или вставлены данные в таблице wb_search_text.")
                else:
                    print("Нет данных для добавления в wb_search_text.")
            except Exception as e:
                traceback.print_exc()
                msg = f"[{seller_code}] Ошибка при получении информации по поисковым запросам для  wb_search_text: {e}"
                print(msg)
                if current_hour >= 12 and not records_presented_at('wb_search_text', today):
                    send_telegram_message(msg)
        else:
            print(f"[{seller_code}] Пропускаем wb_search_text — нет ключей WB")

        # 10) WB Sales Funnel: почасовая воронка продаж
        try:
            date_to_download, hour_to_download = load_newest_date_and_hour(table_name='wb_sales_funnel')
            if not date_to_download:
                date_to_download = today
            if hour_to_download is None:
                hour_to_download = current_hour - 1
                if hour_to_download < 0:
                    hour_to_download = 23
                    date_to_download -= timedelta(days=1)
            else:
                hour_to_download += 1
                if hour_to_download >= 24:
                    hour_to_download = 0
                    date_to_download += timedelta(days=1)

            # Цикл по датам и часам
            while date_to_download <= today:
                while (date_to_download != today and hour_to_download < 24) or hour_to_download < current_hour:
                    try:
                        df = fetch_wb_sales_funnel(
                            config['wb_marketplace_keys'],
                            config['entities_meta'],
                            date_to_download, hour_to_download
                        )
                        df['date'] = date_to_download
                        df['hour'] = hour_to_download
                        if df is not None and not df.empty:
                            delete_previous_and_insert_new_postgres_table(
                                df, 'wb_sales_funnel', hour_column='hour'
                            )
                            print("Успешно обновлены или вставлены данные в таблице wb_sales_funnel.")
                        else:
                            print("Нет данных для добавления в wb_sales_funnel.")
                    except Exception as e:
                        traceback.print_exc()
                        msg = f"[{seller_code}] Ошибка при получении информации по воронке продаж wb через API: {e}"
                        print(msg)
                        if current_hour >= 12:
                            send_telegram_message(msg)
                    hour_to_download += 1
                date_to_download += timedelta(days=1)
                hour_to_download = 0
        except Exception as e:
            traceback.print_exc()
            msg = f"[{seller_code}] Ошибка на глобальном уровне при получении информации по воронке продаж wb через API: {e}"
            print(msg)
            if current_hour >= 12:
                send_telegram_message(msg)

        # 11) WB Ad Campaign: рекламные кампании
        try:
            date_to_download, hour_to_download = load_newest_date_and_hour(table_name='wb_ad_campaign')
            if not date_to_download:
                date_to_download = today
            # Прошедшие дни по 23:00
            while date_to_download < today:
                try:
                    df = fetch_wb_ad_campaign(config['wb_marketplace_keys'], config['entities_meta'], date_to_download)
                    df['date'] = date_to_download
                    df['hour'] = 23
                    if df is not None and not df.empty:
                        delete_previous_and_insert_new_postgres_table(
                            df, 'wb_ad_campaign', hour_column='hour'
                        )
                        print("Успешно обновлены или вставлены данные в таблице wb_ad_campaign.")
                    else:
                        print("Нет данных для добавления в wb_ad_campaign.")
                except:
                    traceback.print_exc()
                    msg = f"Ошибка при получении информации по рекламным кампаниям wb через API: {e}"
                    print(msg)
                    if current_hour >= 12:
                        send_telegram_message(msg)
                date_to_download += timedelta(days=1)
            # Текущий день за предыдущий час
            if hour_to_download is None or hour_to_download != current_hour - 1:
                df = fetch_wb_ad_campaign(config['wb_marketplace_keys'], config['entities_meta'], today)
                df['date'] = today
                df['hour'] = current_hour - 1
                if hour_to_download < 0:
                    df['date'] = today - timedelta(days=1)
                    df['hour'] = 23
                if df is not None and not df.empty:
                    delete_previous_and_insert_new_postgres_table(
                        df, 'wb_ad_campaign', hour_column='hour'
                    )
                    print("Успешно обновлены или вставлены данные в таблице wb_ad_campaign.")
                else:
                    print("Нет данных для добавления в wb_ad_campaign.")
        except Exception as e:
            traceback.print_exc()
            msg = f"[{seller_code}] Ошибка при получении информации по рекламным кампаниям wb через API: {e}"
            print(msg)
            if current_hour >= 12:
                send_telegram_message(msg)

        # # 11) Ozon Sales Funnel: воронка продаж Ozon
        # try:
        #     date_to_download, hour_to_download = load_newest_date_and_hour(table_name='ozon_sales_funnel')
        #     if not date_to_download:
        #         date_to_download = today
        #     if hour_to_download is None:
        #         hour_to_download = current_hour - 1
        #         if hour_to_download < 0:
        #             hour_to_download = 23
        #             date_to_download -= timedelta(days=1)
        #     else:
        #         hour_to_download += 1
        #         if hour_to_download >= 24:
        #             hour_to_download = 0
        #             date_to_download += timedelta(days=1)
        #     while date_to_download <= today:
        #         while (date_to_download != today and hour_to_download < 24) or hour_to_download < current_hour:
        #             try:
        #                 df = fetch_ozon_sales_funnel(
        #                     config['ozon_partners'], date_to_download, hour_to_download
        #                 )
        #                 df['date'] = date_to_download
        #                 df['hour'] = hour_to_download
        #                 if df is not None and not df.empty:
        #                     delete_previous_and_insert_new_postgres_table(
        #                         df, 'ozon_sales_funnel', hour_column='hour'
        #                     )
        #                     print("Успешно обновлены или вставлены данные в таблице ozon_sales_funnel.")
        #                 else:
        #                     print("Нет данных для добавления в ozon_sales_funnel.")
        #             except Exception as e:
        #                 traceback.print_exc()
        #                 msg = f"[{seller_code}] Ошибка при получении информации по воронке продаж ozon через API: {e}"
        #                 print(msg)
        #                 if current_hour >= 12:
        #                     send_telegram_message(msg)
        #             hour_to_download += 1
        #         date_to_download += timedelta(days=1)
        #         hour_to_download = 0
        # except Exception as e:
        #     traceback.print_exc()
        #     msg = f"[{seller_code}] Ошибка на глобальном уровне при получении информации по воронке продаж ozon через API: {e}"
        #     print(msg)
        #     if current_hour >= 12:
        #         send_telegram_message(msg)

    # try:
    # sync_localization_reports_from_db(config["ozon_selenium"]["chrome_profiles"], today)
    #     print("✅ Успешно загружены данные по локализации товаров в таблицу localization_idx_reports.")
    # except Exception as e:
    #     traceback.print_exc()
    #     error_message = f"❌ Ошибка при загрузке данных по локализации товаров: {e}"
    #     print(error_message)
    #     if current_hour >= 12:
    #         send_telegram_message(error_message)

    # try:
    #     df = sync_localization_reports_into_db(config["ozon_selenium"]["chrome_profiles"], today)
    #     if not df.empty:
    #         delete_previous_and_insert_new_postgres_table(df, 'localization_idx_reports', date_column="updated_at")
    #         print("✅ Успешно загружены данные по локализации товаров в таблицу localization_idx_reports.")
    #     else:
    #         print("⚠️ Нет данных для загрузки в localization_idx_reports.")
    # except Exception as e:
    #     traceback.print_exc()
    #     error_message = f"❌ Ошибка при загрузке данных по локализации товаров: {e}"
    #     print(error_message)
    #     if current_hour >= 12:
    #         send_telegram_message(error_message)

        # Обрабатываем данные за вчерашний день
        yesterday = today - timedelta(days=1)

        # 1) Отчёты по продаже для WB (wb_sale_report)
        if config.get('wildberries_selenium'):
            try:
                date_to_download = load_newest_date_with_all_values(
                    table_name='wb_sale_report',
                    column_with_all_values='legal_entity'
                ) + timedelta(days=1)

                while date_to_download <= yesterday:
                    try:
                        df = fetch_wb_sales_report(
                            config['wildberries_selenium'],
                            config['entities_meta'],
                            date_to_download
                        )
                        if df is not None and not df.empty:
                            df['date'] = date_to_download
                            all_wb_sale_reports.append(df)
                            print(f"[{seller_code}] wb_sale_report: загружено {len(df)} строк за {date_to_download}.")
                        else:
                            print(f"[{seller_code}] wb_sale_report: нет данных за {date_to_download}.")
                            if current_hour >= 12 and not records_presented_at('wb_sale_report', date_to_download):
                                send_telegram_message(
                                    f"[{seller_code}] ALERT: нет данных wb_sale_report за {date_to_download}"
                                )
                    except Exception as e:
                        traceback.print_exc()
                        msg = f"[{seller_code}] Ошибка при получении wb_sale_report за {date_to_download}: {e}"
                        print(msg)
                        # алерт на исключение
                        if current_hour >= 12 and not records_presented_at('wb_sale_report', date_to_download):
                            send_telegram_message(msg)
                    finally:
                        date_to_download += timedelta(days=1)

            except Exception as e:
                traceback.print_exc()
                msg = f"[{seller_code}] Глобальная ошибка wb_sale_report: {e}"
                print(msg)
                # глобальный алерт, если упал блок целиком
                if current_hour >= 12:
                    send_telegram_message(msg)
        else:
            print(f"[{seller_code}] Пропускаем wb_sale_report — нет конфигурации wildberries_selenium")

        # 2) Комментированные блоки по индексу локализации (wb_localization_index)
        # Стас: пока не требуется, закомменчено, но оставлено для истории
        # if config.get('wildberries_selenium'):
        #     try:
        #         latest_date = load_newest_date_with_all_values(
        #             table_name='wb_localization_index',
        #             column_with_all_values='legalEntity'
        #         )
        #         start_date = latest_date + timedelta(days=1) if latest_date else yesterday
        #         date_cursor = start_date
        #         while date_cursor <= yesterday:
        #             try:
        #                 df = fetch_wb_localization_index_report(
        #                     config['wildberries_selenium'],
        #                     date_cursor
        #                 )
        #                 df['date'] = date_cursor
        #                 if df is not None and not df.empty:
        #                     delete_previous_and_insert_new_postgres_table(
        #                         df,
        #                         'wb_localization_index'
        #                     )
        #                     print(f"[{seller_code}] wb_localization_index за {date_cursor} загружен.")
        #                 else:
        #                     print(f"[{seller_code}] wb_localization_index: нет данных за {date_cursor}.")
        #             except Exception as e:
        #                 traceback.print_exc()
        #                 msg = f"[{seller_code}] Ошибка wb_localization_index за {date_cursor}: {e}"
        #                 print(msg)
        #                 if current_hour >= 12 and not records_presented_at('wb_localization_index', date_cursor):
        #                     send_telegram_message(msg)
        #             date_cursor += timedelta(days=1)
        #     except Exception as e:
        #         traceback.print_exc()
        #         msg = f"[{seller_code}] Глобальная ошибка wb_localization_index: {e}"
        #         print(msg)
        #         if current_hour >= 12:
        #             send_telegram_message(msg)

        # 3) Реклама Ozon через web-интерфейс (ozon_ad_campaigns_from_front)
        if config.get('ozon_selenium', {}).get('chrome_profiles'):
            try:
                date_to_download = load_newest_date(table_name='ozon_ad_campaigns_from_front') + timedelta(days=1)
                while date_to_download <= yesterday:
                    try:
                        df = fetch_ozon_ad_campaign_statistics_front(
                            config['ozon_selenium']['chrome_profiles'],
                            date_to_download
                        )
                        if df is not None and not df.empty:
                            delete_previous_and_insert_new_postgres_table(
                                df,
                                'ozon_ad_campaigns_from_front'
                            )
                            print(f"[{seller_code}] ozon_ad_campaigns_from_front за {date_to_download} загружен.")
                        else:
                            print(f"[{seller_code}] ozon_ad_campaigns_from_front: нет данных за {date_to_download}.")
                    except Exception as e:
                        traceback.print_exc()
                        msg = f"[{seller_code}] Ошибка ozon_ad_campaigns_from_front за {date_to_download}: {e}"
                        print(msg)
                        if current_hour >= 12 and not records_presented_at('ozon_ad_campaigns_from_front', date_to_download):
                            send_telegram_message(msg)
                    date_to_download += timedelta(days=1)
            except Exception as e:
                traceback.print_exc()
                msg = f"[{seller_code}] Глобальная ошибка ozon_ad_campaigns_from_front: {e}"
                print(msg)
                if current_hour >= 12:
                    send_telegram_message(msg)
        else:
            print(f"[{seller_code}] Пропускаем ozon_ad_campaigns_from_front — нет конфигурации ozon_selenium")

        # 4) Платная приёмка (wb_paid_acceptance)
        try:
            if records_presented_at('wb_paid_acceptance', today):
                print(f"[{seller_code}] wb_paid_acceptance за {today} уже есть, пропускаем.")
            else:
                df = fetch_paid_acceptance_data(config, config['entities_meta'], today, today)
                if df.empty:
                    print(f"[{seller_code}] wb_paid_acceptance: нет данных за {today}.")
                else:
                    delete_previous_and_insert_new_postgres_table(df, 'wb_paid_acceptance')
                    print(f"[{seller_code}] wb_paid_acceptance за {today} загружен.")
        except Exception as e:
            traceback.print_exc()
            msg = f"[{seller_code}] Ошибка wb_paid_acceptance: {e}"
            print(msg)
            if current_hour >= 12:
                send_telegram_message(msg)

        # 5) Платное хранение (wb_paid_storage)
        try:
            if records_presented_at('wb_paid_storage', today):
                print(f"[{seller_code}] wb_paid_storage за {today} уже есть, пропускаем.")
            else:
                df = fetch_paid_storage_data(config, config['entities_meta'], today, today)
                if df.empty:
                    print(f"[{seller_code}] wb_paid_storage: нет данных за {today}.")
                else:
                    delete_previous_and_insert_new_postgres_table(df, 'wb_paid_storage')
                    print(f"[{seller_code}] wb_paid_storage за {today} загружен.")
        except Exception as e:
            traceback.print_exc()
            msg = f"[{seller_code}] Ошибка wb_paid_storage: {e}"
            print(msg)
            if current_hour >= 12:
                send_telegram_message(msg)

        # # 6) Конкуренты MPM (wb_competitor_info)
        # try:
        #     df = fetch_mpm_competitors(config, today, current_hour)
        #     if df.empty:
        #         print(f"[{seller_code}] wb_competitor_info: нет данных.")
        #     else:
        #         delete_previous_and_insert_new_postgres_table(
        #             df,
        #             'wb_competitor_info',
        #             date_column='date',
        #             hour_column='hour'
        #         )
        #         print(f"[{seller_code}] wb_competitor_info загружен.")
        # except Exception as e:
        #     traceback.print_exc()
        #     msg = f"[{seller_code}] Ошибка wb_competitor_info: {e}"
        #     print(msg)
        #     # здесь телеграм-уведомления можно отключить, чтобы не спамить
        #     # if current_hour >= 12:
        #     #     send_telegram_message(msg)

        # 7) Реклама Ozon через API (ozon_ad_campaign)
        # выполняется долго, всегда последний
        if config.get('ozon_ad_campaign'):
            try:
                date_to_download = load_newest_date(table_name='ozon_ad_campaign') + timedelta(days=1)
                while date_to_download <= yesterday:
                    try:
                        df = fetch_ozon_ad_campaign_statistics_api(
                            config['ozon_ad_campaign'],
                            date_to_download
                        )
                        if df is not None and not df.empty:
                            delete_previous_and_insert_new_postgres_table(df, 'ozon_ad_campaign')
                            print(f"[{seller_code}] ozon_ad_campaign за {date_to_download} загружен.")
                        else:
                            print(f"[{seller_code}] ozon_ad_campaign: нет данных за {date_to_download}.")
                    except Exception as e:
                        traceback.print_exc()
                        msg = f"[{seller_code}] Ошибка ozon_ad_campaign за {date_to_download}: {e}"
                        print(msg)
                        if current_hour >= 15 and not records_presented_at('ozon_ad_campaign', date_to_download):
                            send_telegram_message(msg)
                    date_to_download += timedelta(days=1)
            except Exception as e:
                traceback.print_exc()
                msg = f"[{seller_code}] Глобальная ошибка ozon_ad_campaign: {e}"
                print(msg)
                if current_hour >= 12:
                    send_telegram_message(msg)
        else:
            print(f"[{seller_code}] Пропускаем ozon_ad_campaign — нет конфигурации ozon_ad_campaign")

    if all_wb_stock_dfs:
        combined_wb_data = pd.concat(all_wb_stock_dfs, ignore_index=True)
        delete_previous_and_insert_new_postgres_table(
            combined_wb_data,
            'wb_data',
            date_column='date'
        )
        print(f"Успешно обновлены или вставлены данные в таблице wb_data для всех селлеров за {today}.")
    else:
        print("Не было ни одного wb_marketplace_keys — пропускаем wb_data.")

    if all_wb_sale_reports:
        # объединяем, но будем обрабатывать по-датам:
        combined = pd.concat(all_wb_sale_reports, ignore_index=True)

        # группируем по колонке 'date'
        for date_value, group_df in combined.groupby('date'):
            # удаляем все старые строки за эту дату
            delete_previous_and_insert_new_postgres_table(
                group_df,
                table_name='wb_sale_report',
                date_column='date'
            )
            print(f"wb_sale_report успешно обновлён за {date_value}")

    if all_wb_order_dfs:
        combined_orders = pd.concat(all_wb_order_dfs, ignore_index=True)
        # группируем по дате и перезаписываем день за днём
        for date_value, group_df in combined_orders.groupby('date'):
            delete_previous_and_insert_new_postgres_table(
                group_df,
                table_name='wb_orders',
                date_column='date'
            )
            print(f"wb_orders единоразово обновлён за {date_value}, rows={len(group_df)}")
    else:
        print("Не было ни одного wb_marketplace_keys — пропускаем итоговую запись all_wb_orders.")

    if all_wb_commission_dfs:
        combined_commission = pd.concat(all_wb_commission_dfs, ignore_index=True)
        for date_value, group_df in combined_commission.groupby('date'):
            delete_previous_and_insert_new_postgres_table(
                group_df,
                table_name='wb_commission',
                date_column='date'
            )
            print(f"wb_commission успешно обновлён за {date_value}, rows={len(group_df)}")
    else:
        print("Не было данных по wb_commission — пропускаем итоговую запись.")

    # try:
    #     backup_folder = os.getenv('BACKUP_FOLDER')
    #     current_backup_folder = os.path.join(backup_folder, today_msk_datetime().strftime('%Y-%m-%d'))
    #     os.makedirs(current_backup_folder, exist_ok=True)
    #     backup_table('cost', current_backup_folder)
    #     backup_table('betapro_data', current_backup_folder)
    #     backup_table('wb_data', current_backup_folder)
    #     backup_table('ozon_data', current_backup_folder)
    #     backup_table('wb_orders', current_backup_folder)
    #     backup_table('ozon_posting', current_backup_folder)
    #     backup_table('ozon_ad_campaign', current_backup_folder)
    #     backup_table('ozon_ad_campaigns_from_front', current_backup_folder)
    #     backup_table('localization_idx_reports', current_backup_folder)
    #     backup_table('wb_localization_index', current_backup_folder)
    #     backup_table('wb_sale_report', current_backup_folder)
    #     backup_table('wb_search_text', current_backup_folder)
    #     backup_table('ozon_sales_funnel', current_backup_folder)
    #     backup_table('wb_sales_funnel', current_backup_folder)
    #     backup_table('wb_ad_campaign', current_backup_folder)
    #     compress_and_remove(current_backup_folder)
    # except:
    #     traceback.print_exc()
    #     error_message = f"Ошибка при создании бекапа"
    #     print(error_message)


if __name__ == '__main__':
    try:
        setup_logger()
        main()
    except Exception as e:
        traceback.print_exc()
        send_telegram_message(f"Произошла необработанная ошибка: {e}")
