import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import requests

log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(log_dir / 'export_to_excel.log')]
)
logger = logging.getLogger(__name__)

class SessionState:
    def __init__(self):
        self.product_id = '163743606'
        self.start_date = datetime.now().date() - timedelta(days=21)
        self.end_date = datetime.now().date() - timedelta(days=1)
        self.cost_price = 923.0
        self.margin_rate = 0.001
        self.commission_rate = 0.235
        self.log_messages = []
        self.recommendations = pd.DataFrame()
        self.max_cpc = 0

st = SessionState()

def load_db_config():
    logger = logging.getLogger(__name__)
    possible_env_paths = [
        Path(os.getenv("AKO_CONFIG_PATH", "")) / '.env',
        Path(__file__).parent / 'config' / '.env',
        Path.home() / 'Documents' / 'ako' / 'config' / '.env',
        Path(__file__).parent.parent / 'config' / '.env',
    ]

    logger.info(f"Проверяемые пути для файла .env: {', '.join(str(p) for p in possible_env_paths if p)}")

    env_path = None
    for path in [p for p in possible_env_paths if p]:
        if path.exists():
            env_path = path
            logger.info(f"Файл .env найден по пути: {path}")
            break
        else:
            logger.info(f"Файл .env не найден по пути: {path}")

    if not env_path:
        error_msg = f"Файл .env не найден в путях: {', '.join(str(p) for p in possible_env_paths if p)}"
        logger.error(error_msg)
        st.log_messages.append(error_msg)
        return {}

    load_dotenv(dotenv_path=env_path)
    logger.info(f"Файл .env успешно загружен из {env_path}")

    config_dir = env_path.parent
    cert_path = config_dir / 'CA.pem'
    logger.info(f"Проверка наличия SSL-сертификата по пути: {cert_path}")
    if not cert_path.exists():
        logger.warning(f"SSL-сертификат не найден по пути {cert_path}. Продолжаем без SSL.")
        st.log_messages.append(f"SSL-сертификат не найден по пути {cert_path}")
    else:
        logger.info(f"SSL-сертификат найден по пути: {cert_path}")

    db_config = {
        'host': os.getenv('DEST_DB_HOST'),
        'port': os.getenv('DEST_DB_PORT', '5432'),
        'name': os.getenv('DEST_DB_NAME'),
        'user': os.getenv('DEST_DB_USER'),
        'password': os.getenv('DEST_DB_PASSWORD'),
        'sslmode': os.getenv('DEST_DB_SSLMODE', 'prefer'),
        'sslrootcert': str(cert_path) if cert_path.exists() else None,
        'telegram_bot_token': os.getenv('TELEGRAM_BOT_TOKEN'),
        'telegram_chat_id': os.getenv('TELEGRAM_CHAT_ID')
    }

    missing_params = [key for key, value in db_config.items() if not value and key != 'sslrootcert']
    if missing_params:
        error_msg = f"Отсутствуют параметры в .env: {', '.join(missing_params)}"
        logger.error(error_msg)
        st.log_messages.append(error_msg)
        return {}

    logger.debug(f"Загружена конфигурация: {db_config}")
    return db_config

def send_telegram_message(message: str, bot_token: str, chat_id: str):
    """Отправка сообщения в Telegram."""
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'Markdown'
        }
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            logger.info("Уведомление успешно отправлено в Telegram")
            st.log_messages.append("Уведомление успешно отправлено в Telegram")
        else:
            error_msg = f"Ошибка отправки в Telegram: {response.status_code} - {response.text}"
            logger.error(error_msg)
            st.log_messages.append(error_msg)
    except Exception as e:
        error_msg = f"Ошибка при отправке уведомления в Telegram: {str(e)}"
        logger.error(error_msg)
        st.log_messages.append(error_msg)

def get_ad_stats_dataframe(product_id: str = None, start_date: str = None, end_date: str = None):
    """Извлечение данных из таблицы silver.wb_adv_product_stats_1d."""
    try:
        logger.info("Начало извлечения данных из таблицы wb_adv_product_stats_1d")
        db_config = load_db_config()
        if not db_config:
            logger.error("Невозможно продолжить из-за некорректной конфигурации базы данных")
            st.log_messages.append("Невозможно продолжить из-за некорректной конфигурации базы данных")
            return None

        conn_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['name']}?sslmode={db_config['sslmode']}"
            f"{'&sslrootcert=' + db_config['sslrootcert'] if db_config.get('sslrootcert') else ''}"
        )
        engine = create_engine(conn_string)

        query = "SELECT advert_id as campaign_id, nm_id as product_id, date, revenue, items, cost FROM silver.wb_adv_product_stats_1d"
        params = {}
        conditions = []
        if product_id:
            conditions.append("nm_id = :product_id")
            params['product_id'] = product_id
        if start_date:
            conditions.append("date >= :start_date")
            params['start_date'] = start_date
        if end_date:
            conditions.append("date <= :end_date")
            params['end_date'] = end_date
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY date, advert_id, nm_id"

        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params, index_col='date')
            if df.empty:
                logger.error(f"Данные за период {start_date} - {end_date} отсутствуют")
                st.log_messages.append(f"Данные за период {start_date} - {end_date} отсутствуют")
                return None
            unique_products = df['product_id'].nunique()
            if unique_products > 1:
                logger.error(f"В кампании обнаружено {unique_products} товаров, ожидается 1")
                st.log_messages.append(f"В кампании обнаружено {unique_products} товаров, ожидается 1")
                raise ValueError("В кампании должен быть ровно 1 товар")
            df = df[['campaign_id', 'product_id', 'revenue', 'items', 'cost']]

        logger.info("Данные успешно извлечены")
        return df

    except Exception as e:
        logger.error(f"Ошибка при извлечении данных: {str(e)}")
        st.log_messages.append(f"Ошибка при извлечении данных: {str(e)}")
        return None

def get_cluster_stats_dataframe(ad_id: str = None, start_date: str = None, end_date: str = None):
    """Извлечение данных из таблиц silver.wb_adv_keyword_stats_1d и silver.wb_adv_keyword_clusters_1d с джойном."""
    try:
        logger.info("Начало извлечения данных из таблиц wb_adv_keyword_stats_1d и wb_adv_keyword_clusters_1d")
        db_config = load_db_config()
        if not db_config:
            logger.error("Некорректная конфигурация базы данных")
            st.log_messages.append("Некорректная конфигурация базы данных")
            return None

        conn_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['name']}?sslmode={db_config['sslmode']}"
            f"{'&sslrootcert=' + db_config['sslrootcert'] if db_config.get('sslrootcert') else ''}"
        )
        engine = create_engine(conn_string)

        query = """
            SELECT 
                s.advert_id as ad_id, 
                s.date, 
                c.keyword_cluster as cluster_name, 
                s.cost as sum, 
                s.clicks, 
                c.is_excluded
            FROM silver.wb_adv_keyword_stats_1d s
            LEFT JOIN silver.wb_adv_keyword_clusters_1d c
                ON s.advert_id = c.advert_id AND s.keyword = c.keyword
            WHERE s.advert_id = :ad_id
                AND s.date >= :start_date
                AND s.date <= :end_date
                AND date(c.run_dttm) = date(now() at local)
            ORDER BY s.date, s.advert_id, c.keyword_cluster
        """
        params = {
            'ad_id': ad_id,
            'start_date': start_date,
            'end_date': end_date
        }

        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params, index_col='date')
            if df.empty:
                logger.error(f"Данные кластеров за период {start_date} - {end_date} отсутствуют")
                st.log_messages.append(f"Данные кластеров за период {start_date} - {end_date} отсутствуют")
                return None
            df['cpc'] = df['sum'].div(df['clicks'].replace(0, float('nan')))
            df = df[['ad_id', 'cluster_name', 'cpc', 'clicks', 'sum', 'is_excluded']]

        zero_clicks_clusters = df[df['clicks'] == 0]['cluster_name'].nunique()
        if zero_clicks_clusters > 0:
            logger.warning(f"Найдено {zero_clicks_clusters} кластеров с нулевыми кликами")
            st.log_messages.append(f"Найдено {zero_clicks_clusters} кластеров с нулевыми кликами")
        excluded_clusters = df[df['is_excluded'] == True]['cluster_name'].nunique()
        if excluded_clusters > 0:
            logger.info(f"Найдено {excluded_clusters} уже исключенных кластеров")
            st.log_messages.append(f"Найдено {excluded_clusters} уже исключенных кластеров")

        logger.info("Данные успешно извлечены")
        return df

    except Exception as e:
        logger.error(f"Ошибка: {str(e)}")
        st.log_messages.append(f"Ошибка: {str(e)}")
        return None

class AdCPCOptimizer:
    def __init__(self, margin_rate=0.001):
        self.margin_rate = margin_rate

    def optimize_cpc(self, campaign_data, clusters_data):
        """Обновлённая логика с учётом отсутствующих CPC и is_excluded."""
        unit_price = campaign_data['revenue'] / campaign_data['items'] if campaign_data['items'] > 0 else 0
        ad_budget = unit_price - campaign_data['cost_price'] - (unit_price * self.margin_rate)
        coeff = ad_budget / campaign_data['avg_cpi'] if campaign_data['avg_cpi'] > 0 else 1
        max_cpc = campaign_data['avg_cpc'] * coeff

        if max_cpc <= 0 or not np.isfinite(max_cpc):
            logger.warning(f"Некорректный max_cpc: {max_cpc}, возвращается пустой DataFrame")
            st.log_messages.append(f"Некорректный max_cpc: {max_cpc}")
            return pd.DataFrame(columns=['cluster', 'avg_cpc', 'total_clicks', 'total_sum', 'is_excluded']), 0

        condition_with_cpc = clusters_data['avg_cpc'].notna() & (clusters_data['avg_cpc'] <= max_cpc)
        condition_without_cpc = clusters_data['avg_cpc'].isna() & (clusters_data['total_sum'] < max_cpc)
        condition_not_excluded = clusters_data['is_excluded'] == False

        valid_clusters = clusters_data[(condition_with_cpc | condition_without_cpc) & condition_not_excluded]
        logger.info(f"Результаты оптимизации: max_cpc={max_cpc}, valid_clusters={len(valid_clusters)}")
        return valid_clusters, max_cpc

def save_optimization_results(recommendations: pd.DataFrame, campaign_id: int, product_id: str, max_cpc: float):
    """Сохраняет результаты оптимизации в таблицу algo.cluster_optimization_results."""
    try:
        logger.info("Сохранение результатов оптимизации в базу данных")
        db_config = load_db_config()
        if not db_config:
            logger.error("Невозможно сохранить данные из-за некорректной конфигурации базы данных")
            st.log_messages.append("Невозможно сохранить данные из-за некорректной конфигурации базы данных")
            return False

        conn_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['name']}?sslmode={db_config['sslmode']}"
            f"{'&sslrootcert=' + db_config['sslrootcert'] if db_config.get('sslrootcert') else ''}"
        )
        engine = create_engine(conn_string)

        data_to_insert = recommendations.copy()
        data_to_insert['campaign_id'] = campaign_id
        data_to_insert['product_id'] = product_id
        data_to_insert['max_cpc'] = max_cpc
        data_to_insert['optimization_date'] = datetime.now().date()
        data_to_insert = data_to_insert.rename(columns={
            'cluster': 'cluster_name',
            'total_sum': 'total_sum'
        })

        with engine.connect() as conn:
            data_to_insert.to_sql(
                'cluster_optimization_results',
                schema='algo',
                con=conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            conn.commit()

        logger.info(f"Успешно сохранено {len(recommendations)} записей в algo.cluster_optimization_results")
        st.log_messages.append(f"Успешно сохранено {len(recommendations)} записей в базу данных")
        return True

    except Exception as e:
        logger.error(f"Ошибка при сохранении результатов в базу данных: {str(e)}")
        st.log_messages.append(f"Ошибка при сохранении результатов в базу данных: {str(e)}")
        return False

def run_optimization(product_id: str, start_date: str, end_date: str, cost_price: float,
                     margin_rate: float, commission_rate: float) -> tuple[pd.DataFrame, float, int, str]:
    try:
        logger.info(f"Запуск оптимизации для product_id={product_id} за {start_date} - {end_date}")

        ad_stats_df = get_ad_stats_dataframe(product_id=product_id, start_date=start_date, end_date=end_date)
        if ad_stats_df is None or ad_stats_df.empty:
            logger.error(f"Не удалось извлечь данные для product_id={product_id}")
            st.log_messages.append(f"Не удалось извлечь данные для product_id={product_id}")
            return pd.DataFrame(), 0, 0, product_id

        if ad_stats_df['campaign_id'].isna().any():
            logger.error("В campaign_id обнаружены значения NaN")
            st.log_messages.append("В campaign_id обнаружены значения NaN")
            return pd.DataFrame(), 0, 0, product_id

        campaign_data_raw = {
            'campaign_id': int(ad_stats_df['campaign_id'].iloc[0]),
            'revenue': ad_stats_df['revenue'].sum(),
            'items': ad_stats_df['items'].sum(),
            'cost': ad_stats_df['cost'].sum()
        }

        campaign_id = campaign_data_raw['campaign_id']
        clusters_df = get_cluster_stats_dataframe(ad_id=str(campaign_id), start_date=start_date, end_date=end_date)
        if clusters_df is None or clusters_df.empty:
            logger.error(f"Не удалось извлечь данные кластеров для campaign_id={campaign_id}")
            st.log_messages.append(f"Не удалось извлечь данные кластеров для campaign_id={campaign_id}")
            return pd.DataFrame(), 0, campaign_id, product_id

        clusters_agg = clusters_df.groupby('cluster_name').agg({
            'cpc': 'mean',
            'clicks': 'sum',
            'sum': 'sum',
            'is_excluded': 'first'
        }).reset_index()
        clusters_agg = clusters_agg.rename(columns={
            'cluster_name': 'cluster',
            'cpc': 'avg_cpc',
            'clicks': 'total_clicks',
            'sum': 'total_sum'
        })

        nan_clusters = clusters_agg[clusters_agg['avg_cpc'].isna()]
        if not nan_clusters.empty:
            logger.info(f"Найдено {len(nan_clusters)} кластеров с NaN CPC из-за нулевых кликов")
            st.log_messages.append(
                f"Найдено {len(nan_clusters)} кластеров с NaN CPC из-за нулевых кликов")

        net_revenue = campaign_data_raw['revenue'] * (1 - commission_rate)
        total_clicks = clusters_agg['total_clicks'].sum()
        avg_cpc = campaign_data_raw['cost'] / total_clicks if total_clicks > 0 else 0
        avg_cpi = campaign_data_raw['cost'] / campaign_data_raw['items'] if campaign_data_raw['items'] > 0 else 0

        campaign_data = {
            'revenue': net_revenue,
            'items': campaign_data_raw['items'],
            'cost_price': cost_price,
            'cost': campaign_data_raw['cost'],
            'avg_cpi': avg_cpi,
            'avg_cpc': avg_cpc
        }

        logger.info(f"Данные кампании: {campaign_data}")
        logger.info(f"Параметры оптимизации: total_clicks={total_clicks}, avg_cpc={avg_cpc}, avg_cpi={avg_cpi}")

        optimizer = AdCPCOptimizer(margin_rate=margin_rate)
        valid_clusters, max_cpc = optimizer.optimize_cpc(campaign_data, clusters_agg)

        recommendations = clusters_agg.copy()
        recommendations['status'] = recommendations.apply(
            lambda row: 'Исключить' if row['is_excluded'] else (
                'Оставить' if row['cluster'] in valid_clusters['cluster'].values else 'Исключить'
            ), axis=1
        )
        recommendations['recommendation'] = recommendations.apply(
            lambda row: (
                'Кластер уже исключен в базе' if row['is_excluded']
                else (
                    f'Кластер эффективен, CPC ({row["avg_cpc"]:.3f}) <= max_cpc ({max_cpc:.3f})'
                    if row['status'] == 'Оставить' and not pd.isna(row['avg_cpc'])
                    else (
                        f'Кластер оставлен (без кликов), total_sum ({row["total_sum"]:.2f} ₽) < max_cpc ({max_cpc:.3f} ₽)'
                        if row['status'] == 'Оставить' and pd.isna(row['avg_cpc'])
                        else (
                            f'Кластер неэффективен, CPC ({row["avg_cpc"]:.3f}) > max_cpc ({max_cpc:.3f})'
                            if row['status'] == 'Исключить' and not pd.isna(row['avg_cpc'])
                            else f'Кластер исключен (без кликов), total_sum ({row["total_sum"]:.2f} ₽) >= max_cpc ({max_cpc:.3f} ₽)'
                        )
                    )
                )
            ), axis=1
        )
        recommendations = recommendations[
            ['cluster', 'avg_cpc', 'total_clicks', 'total_sum', 'status', 'recommendation', 'is_excluded']]
        recommendations['avg_cpc'] = recommendations['avg_cpc'].round(3)

        logger.info(f"Оптимизация кластеров ключевых фраз завершена: {len(valid_clusters)} валидных кластеров, max_cpc={max_cpc}")
        return recommendations, max_cpc, campaign_id, product_id

    except Exception as e:
        logger.error(f"Ошибка оптимизации: {str(e)}")
        st.log_messages.append(f"Ошибка оптимизации: {str(e)}")
        return pd.DataFrame(), 0, 0, product_id

def main():
    recommendations, max_cpc, campaign_id, product_id = run_optimization(
        product_id=st.product_id,
        start_date=st.start_date.isoformat(),
        end_date=st.end_date.isoformat(),
        cost_price=st.cost_price,
        margin_rate=st.margin_rate,
        commission_rate=st.commission_rate
    )

    db_config = load_db_config()
    bot_token = db_config.get('telegram_bot_token')
    chat_id = db_config.get('telegram_chat_id')

    if not recommendations.empty:
        excluded_clusters = len(recommendations[recommendations['status'] == 'Исключить'])
        total_clusters = len(recommendations)
        print("\nРезультаты оптимизации:")
        print(f"Кампания ID: {campaign_id}, Товар ID: {product_id}")
        print(f"Максимальный CPC: {max_cpc:.3f}")
        print(f"Валидных кластеров: {len(recommendations[recommendations['status'] == 'Оставить'])}")
        print(f"Исключено кластеров: {excluded_clusters}")

        print("\nРекомендации:")
        print(recommendations.to_string(index=False))

        if excluded_clusters == 0 and bot_token and chat_id:
            message = (
                f"ℹ️ *Оптимизация кластеров ключевых фраз завершена*\n"
                f"Кампания ID: {campaign_id}\n"
                f"Товар ID: {product_id}\n"
                f"Проанализировано текущих кластеров: {total_clusters}\n"
                f"Максимальный CPC: {max_cpc:.3f} ₽\n"
                f"Исключено кластеров: {excluded_clusters}\n"
                f"Результаты сохранены в базу данных."
            )
            send_telegram_message(message, bot_token, chat_id)

        if save_optimization_results(recommendations, campaign_id, product_id, max_cpc):
            print("\nРезультаты успешно сохранены в базу данных")
            if bot_token and chat_id and excluded_clusters > 0:
                message = (
                    f"ℹ️*Оптимизация кластеров ключевых фраз завершена*\n"
                    f"Кампания ID: {campaign_id}\n"
                    f"Товар ID: {product_id}\n"
                    f"Проанализировано текущих кластеров: {total_clusters}\n"
                    f"Максимальный CPC: {max_cpc:.3f} ₽\n"
                    f"Исключено кластеров: {excluded_clusters}\n"
                    f"Результаты сохранены в базу данных."
                )
                send_telegram_message(message, bot_token, chat_id)
        else:
            print("\nНе удалось сохранить результаты в базу данных")
            if bot_token and chat_id:
                message = (
                    f"⚠️ *Ошибка оптимизации*\n"
                    f"Кампания ID: {campaign_id}\n"
                    f"Товар ID: {product_id}\n"
                    f"Проанализировано кластеров: {total_clusters}\n"
                    f"Не удалось сохранить результаты в базу данных."
                )
                send_telegram_message(message, bot_token, chat_id)
    else:
        print("Не удалось выполнить оптимизацию. Проверьте логи для получения дополнительной информации.")
        if bot_token and chat_id:
            message = (
                f"❌ *Ошибка оптимизации*\n"
                f"Товар ID: {product_id}\n"
                f"Не удалось выполнить оптимизацию. Проверьте логи:\n"
                f"{'; '.join(st.log_messages[-3:])}"
            )
            send_telegram_message(message, bot_token, chat_id)

    if st.log_messages:
        print("\nЛоги:")
        for msg in st.log_messages:
            print(f"- {msg}")

if __name__ == "__main__":
    main()