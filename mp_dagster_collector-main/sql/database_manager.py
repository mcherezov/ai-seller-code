import logging
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from datetime import datetime, timedelta
from typing import Dict, Optional
import pytz

logger = logging.getLogger(__name__)
MSK_TZ = pytz.timezone('Europe/Moscow')

class UCBBanditDatabase:
    """Класс для управления взаимодействием с базой данных в UCBBanditCPM."""

    def __init__(self, db_config: Dict):
        connect_args = {
            "sslmode": db_config["sslmode"],
            "sslrootcert": db_config["sslrootcert"]
        }
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}",
            connect_args=connect_args
        )
        logger.info("Инициализировано подключение к базе данных PostgreSQL")

    def load_bandit_state(self, advert_id: str, arms: list) -> tuple[Dict[str, int], Dict[str, float], Dict[str, float]]:
        """Загружает состояние бандита из таблицы algo.autobidder."""
        query = """
            SELECT arm, reward, current_cpm
            FROM algo.autobidder
            WHERE campaign_id = :advert_id
        """
        counts = {arm: 0 for arm in arms}
        rewards = {arm: 0.0 for arm in arms}
        cpms = {arm: 0.0 for arm in arms}
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(
                    text(query),
                    self.engine,
                    params={"advert_id": advert_id}
                )
                if not df.empty:
                    arm_counts = df.groupby('arm').size().to_dict()
                    arm_rewards = df.groupby('arm')['reward'].sum().to_dict()
                    arm_cpms = df.groupby('arm')['current_cpm'].mean().to_dict()
                    for arm in arms:
                        counts[arm] = arm_counts.get(arm, 0)
                        rewards[arm] = arm_rewards.get(arm, 0.0)
                        cpms[arm] = arm_cpms.get(arm, 0.0)
                    logger.info(f"Загружено состояние бандита для advert_id {advert_id}: counts={counts}, rewards={rewards}")
                else:
                    logger.info(f"Состояние бандита для advert_id {advert_id} не найдено в algo.autobidder")
        except Exception as e:
            logger.error(f"Не удалось загрузить состояние бандита из algo.autobidder: {e}")
        return counts, rewards, cpms

    def fetch_sql_data(self, advert_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Получает данные из таблиц silver.wb_adv_product_stats_1d и silver.wb_adv_product_rates_1d."""
        params = {
            "advert_id": advert_id,
            "start_date": start_date,
            "end_date": end_date
        }

        query_stats = """
            SELECT advert_id, CAST(date AS DATE) AS date, views AS ad_views, clicks AS ad_clicks,
                   carts AS ad_atbs, orders AS ad_orders, cost AS ad_cost, items, revenue
            FROM silver.wb_adv_product_stats_1d
            WHERE advert_id = :advert_id
              AND CAST(date AS DATE) BETWEEN :start_date AND :end_date
        """
        try:
            df_stats = pd.read_sql(text(query_stats), self.engine, params=params)
            logger.debug(f"Выполнен запрос статистики: {query_stats}")
        except Exception as e:
            logger.error(f"Не удалось выполнить запрос к silver.wb_adv_product_stats_1d: {e}")
            df_stats = pd.DataFrame()

        query_rates = """
            SELECT advert_id, CAST(run_dttm AS DATE) AS date, cpm_current AS ad_rate
            FROM silver.wb_adv_product_rates_1d
            WHERE advert_id = :advert_id
              AND CAST(run_dttm AS DATE) BETWEEN :start_date AND :end_date
        """
        try:
            df_rates = pd.read_sql(text(query_rates), self.engine, params=params)
            logger.debug(f"Выполнен запрос rates: {query_rates}")
        except Exception as e:
            logger.error(f"Не удалось выполнить запрос к silver.wb_adv_product_rates_1d: {e}")
            df_rates = pd.DataFrame()

        if not df_stats.empty:
            df_stats['date'] = pd.to_datetime(df_stats['date']).dt.date
        if not df_rates.empty:
            df_rates['date'] = pd.to_datetime(df_rates['date']).dt.date

        if not df_stats.empty:
            df = df_stats.merge(df_rates[['advert_id', 'date', 'ad_rate']], on=['advert_id', 'date'], how='left')
            df['ad_rate'] = df['ad_rate'].fillna(0)
        elif not df_rates.empty:
            df = df_rates
            for col in ['ad_views', 'ad_clicks', 'ad_atbs', 'ad_orders', 'ad_cost', 'items', 'revenue']:
                df[col] = 0
        else:
            logger.error(f"Данные для advert_id {advert_id} между {start_date} и {end_date} не найдены")
            return pd.DataFrame()

        column_types = {
            'ad_rate': 'float64',
            'ad_cost': 'float64',
            'revenue': 'float64',
            'ad_views': 'int64',
            'ad_clicks': 'int64',
            'ad_atbs': 'int64',
            'ad_orders': 'int64',
            'items': 'int64'
        }
        for col, dtype in column_types.items():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(dtype)
        df = df.sort_values("date")
        logger.info(f"Получено {len(df)} строк для advert_id {advert_id} между {start_date} и {end_date}")
        return df

    def reassess_initial_cpm(self, advert_id: str) -> Optional[float]:
        """Переоценивает начальный CPM на основе данных за последние 7 дней."""
        start_date = (datetime.now() - timedelta(days=7)).isoformat()
        query_rates = """
            SELECT AVG(cpm_current)
            FROM silver.wb_adv_product_rates_1d
            WHERE advert_id = :advert_id AND cpm_current IS NOT NULL 
              AND CAST(run_dttm AS DATE) >= :start_date
        """
        try:
            with self.engine.connect() as conn:
                result_rates = conn.execute(text(query_rates), {"advert_id": advert_id, "start_date": start_date}).fetchone()
                if result_rates and result_rates[0]:
                    initial_cpm = float(result_rates[0])
                    logger.info(f"Переоценен начальный CPM для {advert_id} из silver.wb_adv_product_rates_1d: {initial_cpm:.2f}")
                    return initial_cpm
        except Exception as e:
            logger.error(f"Не удалось переоценить CPM из silver.wb_adv_product_rates_1d: {e}")

        logger.warning(f"Данные для переоценки initial_cpm для {advert_id} не найдены")
        return None

    def estimate_budget(self, advert_id: str, days: int = 7) -> float:
        """Оценивает бюджет на основе среднего CPM и просмотров за указанный период."""
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        query = """
            SELECT AVG(rates.cpm_current) as avg_cpm, AVG(stats.views) as avg_views
            FROM silver.wb_adv_product_rates_1d rates
            LEFT JOIN silver.wb_adv_product_stats_1d stats ON rates.advert_id = stats.advert_id
                AND CAST(rates.run_dttm AS DATE) = stats.date
            WHERE rates.advert_id = :advert_id AND rates.cpm_current IS NOT NULL
              AND CAST(rates.run_dttm AS DATE) >= :start_date
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"advert_id": advert_id, "start_date": start_date}).fetchone()
                if result and result[0] and result[1]:
                    avg_cpm, avg_views = float(result[0]), float(result[1])
                    budget = (avg_cpm / 1000) * avg_views * days
                    logger.info(f"Оценен бюджет для {advert_id}: {budget:.2f} (avg_cpm={avg_cpm:.2f}, avg_views={avg_views:.2f}, days={days})")
                    return budget
        except Exception as e:
            logger.error(f"Не удалось оценить бюджет из silver.wb_adv_product_rates_1d: {e}")

        logger.warning(f"Данные для оценки бюджета для {advert_id} не найдены")
        return 0.0

    def save_recommendation(self, recommendation: Dict) -> bool:
        """Сохраняет рекомендацию в таблицу algo.autobidder."""
        insert_query = """
            INSERT INTO algo.autobidder (
                campaign_id, timestamp, arm, reward, metric, current_cpm, recommended_cpm,
                cpa, cr_to_cart, cr_to_order, roi_carts, roi_orders, roi_weighted,
                atbs, orders, clicks, views, cost, items, revenue,
                cost_price, marketplace_fee, warehouse_cost, stocks, stop_iteration, recommendation
            ) VALUES (
                :campaign_id, :timestamp, :arm, :reward, :metric, :current_cpm, :recommended_cpm,
                :cpa, :cr_to_cart, :cr_to_order, :roi_carts, :roi_orders, :roi_weighted,
                :atbs, :orders, :clicks, :views, :cost, :items, :revenue,
                :cost_price, :marketplace_fee, :warehouse_cost, :stocks, :stop_iteration, :recommendation
            )
        """
        try:

            recommendation['timestamp'] = datetime.now(MSK_TZ).isoformat()
            with self.engine.connect() as conn:
                conn.execute(text(insert_query), recommendation)
                conn.commit()
                logger.info(f"Рекомендация для advert_id {recommendation['campaign_id']} сохранена в algo.autobidder")
                return True
        except Exception as e:
            logger.error(f"Не удалось сохранить рекомендацию в algo.autobidder: {str(e)}")
            return False

    def check_table_exists(self) -> bool:
        """Проверяет существование таблицы algo.autobidder."""
        inspector = inspect(self.engine)
        if not inspector.has_table('autobidder', schema='algo'):
            logger.error("Таблица algo.autobidder не существует")
            return False
        columns = inspector.get_columns('autobidder', schema='algo')
        logger.info(f"Таблица algo.autobidder содержит столбцы: {[col['name'] for col in columns]}")
        return True

    def get_active_campaign_ids(self) -> list[str]:
        """
        Извлекает все campaign_id из таблицы core.algo_states, где model = 'autobidder_ucb'
        и текущая дата находится в диапазоне valid_from и valid_to.

        Returns:
            list[str]: Список campaign_id, соответствующих условиям.

        Raises:
            ValueError: Если данные не найдены или условие не выполняется.
        """
        try:
            logger.info("Извлечение активных campaign_id из таблицы core.algo_states")
            current_date = datetime.now().date().isoformat()
            query = """
                SELECT campaign_id, product_id, model, valid_from, valid_to
                FROM core.algo_states
                WHERE model = 'autobidder_ucb';
            """
            params = {'model': 'autobidder_ucb', 'current_date': current_date}

            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
                if df.empty:
                    error_msg = f"Не найдено записей с model='autobidder_ucb' для даты {current_date}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                campaign_ids = df[['campaign_id', 'product_id']].astype(str).to_dict('records')
                logger.info(f"Успешно извлечены campaign_id: {[row['campaign_id'] for row in campaign_ids]}")
                return campaign_ids

        except Exception as e:
            error_msg = f"Ошибка при извлечении активных campaign_id: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
