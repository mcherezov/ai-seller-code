import logging
import pandas as pd
from datetime import datetime, timedelta
import json
import os
from ako import AdvancedKeywordOptimizer
from wb_api import WildberriesAPI

logger = logging.getLogger(__name__)


class WildberriesOptimizer:
    def __init__(self, token, min_views=5, min_ctr=0.005, max_cpc=25.0):
        logger.info("Инициализация WildberriesOptimizer")
        if not token:
            logger.error("Токен API не предоставлен")
            raise ValueError("Токен API не предоставлен")
        self.api = WildberriesAPI(token)
        self.optimizer = AdvancedKeywordOptimizer(
            min_views=min_views,
            min_ctr=min_ctr,
            max_cpc=max_cpc,
        )
        logger.info(
            f"WildberriesOptimizer успешно инициализирован с параметрами: min_views={min_views}, min_ctr={min_ctr}, max_cpc={max_cpc}")

    def update_cluster_mapping(self, campaign_id: int):
        clusters = self.api.get_keyword_clusters(campaign_id)
        if clusters:
            try:
                self.optimizer.cluster_mapping = {item['keyword']: item['cluster'] for item in clusters}
                logger.info(f"Updated cluster_mapping with {len(self.optimizer.cluster_mapping)} keywords")
            except KeyError as e:
                logger.warning(
                    f"Ошибка формата данных кластеров: отсутствует ключ {e}. Используем ключевые слова как advertId")
                self.optimizer.cluster_mapping = {}
        else:
            logger.warning(
                f"Failed to fetch clusters for campaign {campaign_id}. Используем ключевые слова как advertId")
            self.optimizer.cluster_mapping = {}

    def optimize_campaign(self, campaign_id: int, start_date: str = None, end_date: str = None, days: int = None):
        """
        Оптимизирует кампанию на основе статистики за указанный период.

        Args:
            campaign_id: ID кампании.
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
        self.update_cluster_mapping(campaign_id)
        stats = self.api.get_stats_keywords(campaign_id, start_date, end_date)
        if not stats:
            logger.warning(f"Нет данных для кампании {campaign_id}")
            return None

        df = pd.DataFrame(stats).rename(columns={'views': 'shows', 'sum': 'spend'})
        if df.empty:
            logger.warning(f"Пустой DataFrame для кампании {campaign_id}")
            return None

        required_columns = ['date', 'keyword', 'shows', 'clicks', 'spend']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Отсутствуют необходимые колонки в данных: {missing_columns}")
            return None

        logger.info(f"Первые 5 строк данных из API для кампании {campaign_id}:\n{df.head().to_dict('records')}")


        invalid_rows = df[df['clicks'] > df['shows']]
        if not invalid_rows.empty:
            logger.warning(f"Найдено {len(invalid_rows)} строк с clicks > shows: "
                           f"{invalid_rows[['date', 'keyword', 'shows', 'clicks']].to_dict('records')}")
            df.loc[df['clicks'] > df['shows'], 'clicks'] = df['shows']

        df['ctr'] = df['clicks'] / df['shows'].replace(0, 1)

        df['advertId'] = df['keyword'].apply(self.optimizer._map_keyword_to_cluster)
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
                                   days: int = None):
        """
        Фильтрует ключевые слова кампании на основе статистики за указанный период.

        Args:
            campaign_id: ID кампании.
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
        stats = self.api.get_stats_keywords(campaign_id, start_date, end_date)
        if not stats:
            logger.warning(f"Нет данных для кампании {campaign_id}")
            return None

        df = pd.DataFrame(stats).rename(columns={'views': 'shows', 'sum': 'spend'})
        if df.empty:
            logger.warning(f"Пустой DataFrame для кампании {campaign_id}")
            return None

        required_columns = ['date', 'keyword', 'shows', 'clicks', 'spend']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Отсутствуют необходимые колонки в данных: {missing_columns}")
            return None

        invalid_rows = df[df['clicks'] > df['shows']]
        if not invalid_rows.empty:
            logger.warning(f"Найдено {len(invalid_rows)} строк с clicks > shows: "
                           f"{invalid_rows[['date', 'keyword', 'shows', 'clicks']].to_dict('records')}")
            df.loc[df['clicks'] > df['shows'], 'clicks'] = df['shows']
        df['ctr'] = df['clicks'] / df['shows'].replace(0, 1)

        df['advertId'] = df['keyword'].apply(self.optimizer._map_keyword_to_cluster)
        filtered_df = self.optimizer.filter_dataframe(df)

        if filtered_df is None or filtered_df.empty:
            logger.warning(f"Отфильтрованный DataFrame пуст для кампании {campaign_id}")
            return None

        logger.info(f"Отфильтрованные данные для кампании {campaign_id}:\n{filtered_df.to_dict('records')}")
        print(f"\nОтфильтрованные ключевые слова для кампании {campaign_id}:")
        self.api.print_keyword_stats(filtered_df.to_dict('records'))

        return filtered_df

    def save_results(self, optimization_result, filtered_df, campaign_id):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if optimization_result is not None and not optimization_result.empty:
            optimization_result.to_csv(f"optimization_{campaign_id}_{timestamp}.csv", index=False)
            logger.info(f"Saved optimization results to optimization_{campaign_id}_{timestamp}.csv")
        if filtered_df is not None and not filtered_df.empty:
            filtered_df.to_csv(f"filtered_keywords_{campaign_id}_{timestamp}.csv", index=False)
            logger.info(f"Saved filtered keywords to filtered_keywords_{campaign_id}_{timestamp}.csv")