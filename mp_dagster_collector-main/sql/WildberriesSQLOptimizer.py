import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy import create_engine
from src.ako import AdvancedKeywordOptimizer


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/keyword_optimizer.log')
    ]
)
logger = logging.getLogger(__name__)


class WildberriesSQLOptimizer:
    def __init__(self, db_params: dict, min_views: int = 5, min_ctr: float = 0.005, max_cpc: float = 25.0):
        """Инициализация оптимизатора с использованием SQL."""
        logger.info("Инициализация WildberriesSQLOptimizer")
        if not db_params:
            logger.error("Параметры подключения к базе данных не предоставлены")
            raise ValueError("Параметры подключения к базе данных не предоставлены")
        self.db_params = db_params
        self.connection_string = (
            f"postgresql://{db_params['user']}:{db_params['password']}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )
        self.optimizer = AdvancedKeywordOptimizer(
            min_views=min_views,
            min_ctr=min_ctr,
            max_cpc=max_cpc,
        )
        logger.info(
            f"WildberriesSQLOptimizer успешно инициализирован с параметрами: min_views={min_views}, min_ctr={min_ctr}, max_cpc={max_cpc}")

    def get_stats_from_db(self, campaign_id: int, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Получает статистику из таблицы ads.cluster_stats для указанной кампании и периода."""
        try:
            logger.info(f"Подключаемся к PostgreSQL для получения данных кампании {campaign_id}...")
            engine = create_engine(self.connection_string)
            logger.info("Успешное подключение!")

            query = """
                SELECT 
                    ad_id,
                    date,
                    cluster_name,
                    views,
                    clicks,
                    sum,
                    ctr
                FROM ads.cluster_stats
                WHERE ad_id = %s
                AND date BETWEEN %s AND %s
            """
            logger.info(f"Выполняем запрос для кампании {campaign_id} за период {start_date} - {end_date}")

            df = pd.read_sql_query(query, engine, params=(campaign_id, start_date, end_date))

            if not df.empty:
                logger.info(f"Получено {len(df)} строк для кампании {campaign_id}")
                logger.debug(f"Колонки в DataFrame до переименования: {list(df.columns)}")

                df = df.rename(columns={
                    'ad_id': 'advertId',
                    'cluster_name': 'keyword',
                    'views': 'shows',
                    'sum': 'spend'
                })
                logger.debug(f"Колонки в DataFrame после переименования: {list(df.columns)}")

                df['date'] = pd.to_datetime(df['date'])
                df['shows'] = df['shows'].astype(int)
                df['clicks'] = df['clicks'].astype(int)
                df['spend'] = df['spend'].astype(float)
                df['ctr'] = df['ctr'].astype(float).clip(upper=1.0)

                invalid_rows = df[df['clicks'] > df['shows']]
                if not invalid_rows.empty:
                    logger.warning(f"Найдено {len(invalid_rows)} строк с clicks > shows: "
                                   f"{invalid_rows[['date', 'keyword', 'shows', 'clicks']].to_dict('records')}")
                    df.loc[df['clicks'] > df['shows'], 'clicks'] = df['shows']
                    df['ctr'] = (df['clicks'] / df['shows'].replace(0, 1)).clip(upper=1.0)

                required_columns = ['date', 'keyword', 'advertId', 'shows', 'clicks', 'spend', 'ctr']
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    logger.error(f"Отсутствуют колонки в DataFrame: {missing_columns}")
                    return None

                logger.debug(f"Первые 5 строк DataFrame:\n{df.head().to_dict('records')}")
                return df[required_columns]
            else:
                logger.warning(f"Нет данных для кампании {campaign_id} за период {start_date} - {end_date}")
                return None

        except Exception as e:
            logger.error(f"Ошибка при получении данных из базы: {str(e)}")
            return None
        finally:
            engine.dispose() if 'engine' in locals() else None
            logger.info("Соединение с базой закрыто.")

    def optimize_campaign(self, campaign_id: int, start_date: str = None, end_date: str = None, days: int = None) -> \
    Optional[pd.DataFrame]:
        """
        Оптимизирует кампанию на основе данных из ads.cluster_stats.

        Args:
            campaign_id: ID кампании (соответствует ad_id в таблице).
            start_date: Начальная дата ('YYYY-MM-DD'). Если не указано, используется days.
            end_date: Конечная дата ('YYYY-MM-DD'). Если не указано, используется days.
            days: Количество дней для анализа (если start_date и end_date не указаны).

        Returns:
            pd.DataFrame: Результаты оптимизации с колонками advertId, total_shows, total_clicks, ctr, cpm, total_spend, efficiency_score, decision.
        """
        if days is not None and (start_date is None or end_date is None):
            if not (1 <= days <= 31):
                logger.error("days должно быть в диапазоне 1–31")
                return None
            end = datetime.now().date() - timedelta(days=1)
            start = end - timedelta(days=days - 1)
            start_date = start.isoformat()
            end_date = end.isoformat()

        if start_date is None or end_date is None:
            logger.error("Не указаны start_date и end_date, и days не предоставлен")
            return None

        logger.info(f"Запуск оптимизации для кампании {campaign_id} за период {start_date} - {end_date}")
        df = self.get_stats_from_db(campaign_id, start_date, end_date)

        if df is None or df.empty:
            logger.warning(f"Пустой DataFrame для кампании {campaign_id}")
            return None

        required_columns = ['date', 'keyword', 'advertId', 'shows', 'clicks', 'spend']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Отсутствуют необходимые колонки в данных: {missing_columns}")
            return None

        logger.info(f"Первые 5 строк данных для кампании {campaign_id}:\n{df.head().to_dict('records')}")

        optimization_result = self.optimizer.optimize(df)

        if optimization_result is None or optimization_result.empty:
            logger.warning(f"Результат оптимизации пуст для кампании {campaign_id}")
            return None

        logger.info(f"Результаты оптимизации для кампании {campaign_id}:\n{optimization_result.to_dict('records')}")
        print(f"\nРекомендации по оптимизации кампании {campaign_id}:")
        print(f"{'Кластер':<25} {'Решение':<12} {'Причина':<20} {'CTR':>6} {'Ставка':>8} {'Эффективность':>12}")
        print("-" * 85)
        for _, row in optimization_result.iterrows():
            print(f"{row['advertId'][:25]:<25} {row['decision']:<12} {row['reason'][:20]:<20} "
                  f"{row['ctr']:>6.2%} {row.get('bid_modifier', 1.0):>8.2f} {row['efficiency_score']:>12.2f}")

        return optimization_result

    def filter_and_update_campaign(self, campaign_id: int, start_date: str = None, end_date: str = None,
                                   days: int = None) -> Optional[pd.DataFrame]:
        """
        Фильтрует данные кампании на основе статистики из ads.cluster_stats.

        Args:
            campaign_id: ID кампании (соответствует ad_id в таблице).
            start_date: Начальная дата ('YYYY-MM-DD'). Если не указано, используется days.
            end_date: Конечная дата ('YYYY-MM-DD'). Если не указано, используется days.
            days: Количество дней для анализа (если start_date и end_date не указаны).

        Returns:
            pd.DataFrame: Отфильтрованные данные с колонками date, keyword, advertId, shows, clicks, spend, ctr.
        """
        if days is not None and (start_date is None or end_date is None):
            if not (1 <= days <= 31):
                logger.error("days должно быть в диапазоне 1–31")
                return None
            end = datetime.now().date() - timedelta(days=1)
            start = end - timedelta(days=days - 1)
            start_date = start.isoformat()
            end_date = end.isoformat()

        if start_date is None or end_date is None:
            logger.error("Не указаны start_date и end_date, и days не предоставлен")
            return None

        logger.info(f"Фильтрация данных для кампании {campaign_id} за период {start_date} - {end_date}")
        df = self.get_stats_from_db(campaign_id, start_date, end_date)

        if df is None or df.empty:
            logger.warning(f"Пустой DataFrame для кампании {campaign_id}")
            return None

        required_columns = ['date', 'keyword', 'advertId', 'shows', 'clicks', 'spend']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Отсутствуют необходимые колонки в данных: {missing_columns}")
            return None

        filtered_df = self.optimizer.filter_dataframe(df)

        if filtered_df is None or filtered_df.empty:
            logger.warning(f"Отфильтрованный DataFrame пуст для кампании {campaign_id}")
            return None

        logger.info(f"Отфильтрованные данные для кампании {campaign_id}:\n{filtered_df.to_dict('records')}")
        print(f"\nОтфильтрованные ключевые слова для кампании {campaign_id}:")
        self.print_keyword_stats(filtered_df.to_dict('records'))

        return filtered_df

    def print_keyword_stats(self, stats: list[dict]):
        """Выводит статистику по ключевым словам в табличном формате."""
        if not stats:
            print("Нет данных по ключевым фразам за запрошенный период.")
            return

        print(f"{'Дата':<12} {'Ключевая фраза':<25} {'Просм.':>6} {'Кл.':>4} {'CTR':>6} {'Сумма':>8}")
        print("-" * 65)
        for rec in stats:
            shows = rec.get('shows', 0)
            clicks = rec.get('clicks', 0)
            ctr = rec.get('ctr', 0.0)
            spend = rec.get('spend', 0.0)
            date_str = rec['date'].strftime('%Y-%m-%d')[:12]
            print(f"{date_str:<12} {rec['keyword'][:25]:<25} "
                  f"{shows:>6} {clicks:>4} {ctr:>5.2f}% {spend:>8.2f}₽")

    def save_results(self, optimization_result: pd.DataFrame, filtered_df: pd.DataFrame, campaign_id: int):
        """Сохраняет результаты оптимизации и отфильтрованные данные в CSV."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if optimization_result is not None and not optimization_result.empty:
            optimization_result.to_csv(f"optimization_{campaign_id}_{timestamp}.csv", index=False)
            logger.info(f"Сохранены результаты оптимизации в optimization_{campaign_id}_{timestamp}.csv")
        if filtered_df is not None and not filtered_df.empty:
            filtered_df.to_csv(f"filtered_keywords_{campaign_id}_{timestamp}.csv", index=False)
            logger.info(f"Сохранены отфильтрованные ключевые слова в filtered_keywords_{campaign_id}_{timestamp}.csv")



import os
db_params = {
    "user": os.getenv("DEST_DB_USER", "aiadmin"),
    "password": os.getenv("DEST_DB_PASSWORD", "b1g8fqrgbp56ppg4uucc8jfi4"),
    "host": os.getenv("DEST_DB_HOST", "rc1a-6ic32g9da782j8kh.mdb.yandexcloud.net"),
    "port": os.getenv("DEST_DB_PORT", "6432"),
    "dbname": os.getenv("DEST_DB_NAME", "app"),
    "sslmode": os.getenv("DEST_DB_SSLMODE", "verify-full"),
    "sslrootcert": "CA.pem"
}
optimizer = WildberriesSQLOptimizer(db_params, min_views=5, min_ctr=0.005, max_cpc=25.0)
result = optimizer.optimize_campaign(campaign_id=22196238, days=7)
filtered_df = optimizer.filter_and_update_campaign(campaign_id=22196238, days=7)
optimizer.save_results(result, filtered_df, campaign_id=22196238)