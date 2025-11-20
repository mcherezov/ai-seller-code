from sqlalchemy import create_engine, text
import pandas as pd
import logging
from datetime import datetime

class DatabaseManager:
    """
    Управляет взаимодействием с базой данных PostgreSQL.

    Attributes:
        db_config (dict): Конфигурация базы данных.
        logger (logging.Logger): Логгер для записи событий и ошибок.
        session_state (SessionState): Объект состояния сессии для логирования сообщений.
    """

    def __init__(self, db_config: dict, session_state, logger: logging.Logger):
        """
        Инициализирует менеджер базы данных с заданной конфигурацией.

        Args:
            db_config (dict): Конфигурация базы данных (host, port, name, user, password, sslmode, sslrootcert).
            session_state (SessionState): Объект состояния сессии для логирования.
            logger (logging.Logger): Логгер для записи событий.
        """
        self.db_config = db_config
        self.logger = logger
        self.session_state = session_state

    def _create_engine(self):
        """
        Создаёт движок SQLAlchemy для подключения к базе данных.

        Returns:
            sqlalchemy.engine.Engine: Движок для подключения к базе данных.
        """
        conn_string = (
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
            f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['name']}?sslmode={self.db_config['sslmode']}"
            f"{'&sslrootcert=' + self.db_config['sslrootcert'] if self.db_config.get('sslrootcert') else ''}"
        )
        return create_engine(conn_string)

    def get_ad_stats(self, product_id: str = None, campaign_id: int = None, start_date: str = None, end_date: str = None):
        """
        Извлекает данные из таблицы silver.wb_adv_product_stats_1d.

        Args:
            product_id (str, optional): Идентификатор продукта (nm_id).
            campaign_id (int, optional): Идентификатор кампании (advert_id).
            start_date (str, optional): Начальная дата периода.
            end_date (str, optional): Конечная дата периода.

        Returns:
            pandas.DataFrame or None: DataFrame с данными или None в случае ошибки.
        """
        try:
            self.logger.debug(f"Начало извлечения данных из таблицы wb_adv_product_stats_1d для product_id={product_id}, campaign_id={campaign_id}")
            if not self.db_config:
                self.logger.error("Невозможно продолжить из-за некорректной конфигурации базы данных")
                if self.session_state:
                    self.session_state.add_log_message(
                        "Невозможно продолжить из-за некорректной конфигурации базы данных")
                return None

            engine = self._create_engine()
            query = "SELECT advert_id as campaign_id, nm_id as product_id, date, revenue, items, cost FROM silver.wb_adv_product_stats_1d"
            params = {}
            conditions = []
            if product_id is not None:
                conditions.append("nm_id = :product_id")
                params['product_id'] = int(product_id)
            if campaign_id is not None:
                conditions.append("advert_id = :campaign_id")
                params['campaign_id'] = int(campaign_id)
            if start_date:
                conditions.append("date::date >= :start_date")
                params['start_date'] = start_date
            if end_date:
                conditions.append("date::date <= :end_date")
                params['end_date'] = end_date
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY date, advert_id, nm_id"

            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params, index_col='date')
                if df.empty:
                    self.logger.error(f"Отсутствуют данные wb_adv_product_stats_1d за период {start_date} - {end_date} для product_id={product_id}, campaign_id={campaign_id}")
                    if self.session_state:
                        self.session_state.add_log_message(f"Отсутствуют данные wb_adv_product_stats_1d за период {start_date} - {end_date} для product_id={product_id}, campaign_id={campaign_id}")
                    return None
                unique_products = df['product_id'].nunique()
                unique_campaigns = df['campaign_id'].nunique()
                if unique_products > 1:
                    self.logger.error(f"В кампании обнаружено {unique_products} товаров, ожидается 1")
                    if self.session_state:
                        self.session_state.add_log_message(
                            f"В кампании обнаружено {unique_products} товаров, ожидается 1")
                    raise ValueError("В кампании должен быть ровно 1 товар")
                if unique_campaigns > 1:
                    self.logger.error(f"Обнаружено {unique_campaigns} кампаний, ожидается 1")
                    if self.session_state:
                        self.session_state.add_log_message(
                            f"Обнаружено {unique_campaigns} кампаний, ожидается 1")
                    raise ValueError("Ожидается ровно 1 кампания")
                df = df[['campaign_id', 'product_id', 'revenue', 'items', 'cost']]

            self.logger.debug("Данные успешно извлечены")
            return df

        except Exception as e:
            self.logger.error(f"Ошибка при извлечении данных: {str(e)}")
            if self.session_state:
                self.session_state.add_log_message(f"Ошибка при извлечении данных: {str(e)}")
            return None

    def get_cluster_stats(self, ad_id: str = None, start_date: str = None, end_date: str = None):
        """
        Извлекает данные из таблицы silver.wb_adv_keyword_stats_1d.

        Args:
            ad_id (str, optional): Идентификатор рекламной кампании.
            start_date (str, optional): Начальная дата периода.
            end_date (str, optional): Конечная дата периода.

        Returns:
            pandas.DataFrame or None: DataFrame с данными или None в случае ошибки.
        """
        try:
            self.logger.debug("Начало извлечения данных из таблицы wb_adv_keyword_stats_1d")
            if not self.db_config:
                self.logger.error("Некорректная конфигурация базы данных")
                if self.session_state:
                    self.session_state.add_log_message("Некорректная конфигурация базы данных")
                return None

            engine = self._create_engine()
            query = """
                SELECT 
                    advert_id as ad_id, 
                    date, 
                    keyword as cluster_name, 
                    cost as sum, 
                    clicks
                FROM silver.wb_adv_keyword_stats_1d
                WHERE advert_id = :ad_id
                    AND date::date >= :start_date
                    AND date::date <= :end_date
                ORDER BY date, advert_id, keyword
            """
            params = {
                'ad_id': ad_id,
                'start_date': start_date,
                'end_date': end_date
            }

            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params, index_col='date')
                if df.empty:
                    self.logger.error(f"Отсутствуют данные wb_adv_keyword_stats_1d за период {start_date} - {end_date} для product_id={product_id}, campaign_id={campaign_id}")
                    if self.session_state:
                        self.session_state.add_log_message(f"Отсутствуют данные wb_adv_keyword_stats_1d за период {start_date} - {end_date} для product_id={product_id}, campaign_id={campaign_id}")
                    return None
                df['cpc'] = df['sum'].div(df['clicks'].replace(0, float('nan')))
                df = df[['ad_id', 'cluster_name', 'cpc', 'clicks', 'sum']]

            zero_clicks_keywords = df[df['clicks'] == 0]['cluster_name'].nunique()
            if zero_clicks_keywords > 0:
                self.logger.warning(f"Найдено {zero_clicks_keywords} ключевых слов с нулевыми кликами")
                if self.session_state:
                    self.session_state.add_log_message(
                        f"Найдено {zero_clicks_keywords} ключевых слов с нулевыми кликами")
            self.logger.debug("Данные успешно извлечены")
            return df

        except Exception as e:
            self.logger.error(f"Ошибка: {str(e)}")
            if self.session_state:
                self.session_state.add_log_message(f"Ошибка: {str(e)}")
            return None

    def save_optimization_results(self, recommendations: pd.DataFrame, campaign_id: int, product_id: str,
                                  max_cpc: float):
        """
        Сохраняет результаты оптимизации в таблицу algo.cluster_optimization_results.

        Args:
            recommendations (pd.DataFrame): DataFrame с результатами оптимизации.
            campaign_id (int): Идентификатор кампании.
            product_id (str): Идентификатор продукта.
            max_cpc (float): Максимальная стоимость за клик.

        Returns:
            bool: True, если сохранение успешно, иначе False.
        """
        try:
            self.logger.debug("Сохранение результатов оптимизации в базу данных")
            if not self.db_config:
                self.logger.error("Невозможно сохранить данные из-за некорректной конфигурации базы данных")
                if self.session_state:
                    self.session_state.add_log_message(
                        "Невозможно сохранить данные из-за некорректной конфигурации базы данных")
                return False

            engine = self._create_engine()
            data_to_insert = recommendations.copy()
            data_to_insert['campaign_id'] = campaign_id
            data_to_insert['product_id'] = product_id
            data_to_insert['max_cpc'] = max_cpc
            data_to_insert['optimization_date'] = pd.Timestamp.now().date()
            data_to_insert['is_excluded'] = data_to_insert['status'].apply(
                lambda x: True if x == 'Исключить' else False)
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

            self.logger.info(f"Успешно сохранено {len(recommendations)} записей в algo.cluster_optimization_results")
            if self.session_state:
                self.session_state.add_log_message(f"Успешно сохранено {len(recommendations)} записей в базу данных")
            return True

        except Exception as e:
            self.logger.error(f"Ошибка при сохранении результатов в базу данных: {str(e)}")
            if self.session_state:
                self.session_state.add_log_message(f"Ошибка при сохранении результатов в базу данных: {str(e)}")
            return False

    def get_commission_rate(self, product_id: str, date: str = None) -> float:
        """
        Извлекает commission_rate (kgvp_booking) из таблицы silver.wb_commission_1d,
        присоединяя к silver.wb_mp_skus_1d по subject_id и дате.

        Args:
            product_id (str): Идентификатор продукта (nm_id).
            date (str, optional): Дата в формате ISO (например, '2025-07-04').
                                 Если не указана, берётся последняя доступная дата.

        Returns:
            float: Значение commission_rate (kgvp_booking).

        Raises:
            ValueError: Если данные не найдены или произошла ошибка.
        """
        try:
            self.logger.info(f"Извлечение commission_rate для product_id={product_id}" +
                             (f" на дату {date}" if date else ""))
            if not self.db_config:
                error_msg = "Некорректная конфигурация базы данных"
                self.logger.error(error_msg)
                if self.session_state:
                    self.session_state.add_log_message(error_msg)
                raise ValueError(error_msg)

            engine = self._create_engine()
            query = """
                SELECT c.kgvp_booking
                FROM silver.wb_commission_1d c
                JOIN silver.wb_mp_skus_1d s
                    ON c.subject_id = s.subject_id
                    AND DATE(c.date) = DATE(s.response_dttm)
                WHERE s.nm_id = :product_id
            """
            params = {'product_id': int(product_id)}

            if date:
                query += " AND DATE(c.date) = :date"
                params['date'] = date

            query += " ORDER BY c.date DESC LIMIT 1"

            with engine.connect() as conn:
                result = pd.read_sql(text(query), conn, params=params)
                if result.empty:
                    error_msg = f"Commission_rate для product_id={product_id}" + \
                                (f" на дату {date}" if date else "") + " не найден"
                    self.logger.error(error_msg)
                    if self.session_state:
                        self.session_state.add_log_message(error_msg)
                    raise ValueError(error_msg)

                commission_rate = float(result['kgvp_booking'].iloc[0] / 100)
                self.logger.info(f"Commission_rate={commission_rate} успешно извлечён для product_id={product_id}")
                if self.session_state:
                    self.session_state.add_log_message(f"Commission_rate={commission_rate} успешно извлечён")
                return commission_rate

        except Exception as e:
            error_msg = f"Ошибка при извлечении commission_rate: {str(e)}"
            self.logger.error(error_msg)
            if self.session_state:
                self.session_state.add_log_message(error_msg)
            raise ValueError(error_msg)

    def get_cost_price(self, product_id: str, date: str = None) -> float:
        """
        Извлекает self_cost из таблицы core.product_costs,
        присоединяя к silver.wb_mp_skus_1d по barcode и диапазону дат.
        Если данные для указанной даты не найдены, возвращает последний self_cost для barcode.

        Args:
            product_id (str): Идентификатор продукта (nm_id).
            date (str, optional): Дата в формате ISO (например, '2025-07-04').
                                 Если не указана, берётся последняя доступная дата.

        Returns:
            float: Значение self_cost.

        Raises:
            ValueError: Если данные не найдены или произошла ошибка подключения/конфигурации.
        """
        try:
            self.logger.info(f"Извлечение cost_price для product_id={product_id}" +
                             (f" на дату {date}" if date else ""))
            if not self.db_config:
                error_msg = "Некорректная конфигурация базы данных"
                self.logger.error(error_msg)
                if self.session_state:
                    self.session_state.add_log_message(error_msg)
                raise ValueError(error_msg)

            engine = self._create_engine()

            barcode_query = """
                SELECT barcode
                FROM silver.wb_mp_skus_1d
                WHERE nm_id = :product_id
            """
            barcode_params = {'product_id': int(product_id)}
            if date:
                barcode_query += " AND DATE(response_dttm) = :date"
                barcode_params['date'] = date
            barcode_query += " ORDER BY response_dttm DESC LIMIT 1"

            with engine.connect() as conn:
                barcode_df = pd.read_sql(text(barcode_query), conn, params=barcode_params)

                if barcode_df.empty:
                    error_msg = f"Barcode для product_id={product_id}" + \
                                (f" на дату {date}" if date else "") + " не найден в silver.wb_mp_skus_1d"
                    self.logger.error(error_msg)
                    if self.session_state:
                        self.session_state.add_log_message(error_msg)
                    raise ValueError(error_msg)

                barcode = barcode_df['barcode'].iloc[0]

                cost_query = """
                    SELECT self_cost
                    FROM core.product_costs
                    WHERE barcode = :barcode
                """
                cost_params = {'barcode': barcode}
                if date:
                    cost_query += " AND CAST(:date AS DATE) BETWEEN date_from AND date_to"
                    cost_params['date'] = date
                cost_query += " ORDER BY date_from DESC LIMIT 1"

                cost_df = pd.read_sql(text(cost_query), conn, params=cost_params)
                if cost_df.empty and date:
                    self.logger.info(f"Cost_price для product_id={product_id} (barcode={barcode})" +
                                     (f" на дату {date}" if date else "") + " не найден, ищем последний self_cost")
                    cost_query = """
                        SELECT self_cost
                        FROM core.product_costs
                        WHERE barcode = :barcode
                        ORDER BY date_from DESC LIMIT 1
                    """
                    cost_params = {'barcode': barcode}
                    cost_df = pd.read_sql(text(cost_query), conn, params=cost_params)

                if cost_df.empty:
                    error_msg = f"Cost_price для product_id={product_id} (barcode={barcode})" + \
                                (f" на дату {date}" if date else "") + " не найден в core.product_costs"
                    self.logger.error(error_msg)
                    if self.session_state:
                        self.session_state.add_log_message(error_msg)
                    raise ValueError(error_msg)

                cost_price = float(cost_df['self_cost'].iloc[0])
                self.logger.info(f"Cost_price={cost_price} успешно извлечён для product_id={product_id}")
                if self.session_state:
                    self.session_state.add_log_message(f"Cost_price={cost_price} успешно извлечён")
                return cost_price

        except Exception as e:
            error_msg = f"Ошибка при извлечении cost_price: {str(e)}"
            self.logger.error(error_msg)
            if self.session_state:
                self.session_state.add_log_message(error_msg)
            raise ValueError(error_msg)

    def get_active_product_id(self) -> list[tuple[str, int]]:
        """
        Извлекает все product_id и campaign_id из таблицы core.algo_states, где model = 'keyword_optimizator'
        и текущая дата и время (NOW()) находятся в диапазоне valid_from и valid_to.

        Returns:
            list[tuple[str, int]]: Список кортежей (product_id, campaign_id), соответствующих условиям.

        Raises:
            ValueError: Если данные не найдены или произошла ошибка.
        """
        try:
            self.logger.debug("Извлечение активных product_id и campaign_id из таблицы core.algo_states")
            if not self.db_config:
                error_msg = "Некорректная конфигурация базы данных"
                self.logger.error(error_msg)
                if self.session_state:
                    self.session_state.add_log_message(error_msg)
                raise ValueError(error_msg)

            engine = self._create_engine()
            query = """
                SELECT campaign_id, product_id, model, valid_from, valid_to
                FROM core.algo_states
                WHERE model = :model
                  AND NOW() BETWEEN valid_from AND valid_to
            """
            params = {'model': 'keyword_optimizator'}

            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
                if df.empty:
                    error_msg = f"Не найдено записей с model='keyword_optimizator' для текущего времени"
                    self.logger.error(error_msg)
                    if self.session_state:
                        self.session_state.add_log_message(error_msg)
                    raise ValueError(error_msg)

                result = [(str(row['product_id']), int(row['campaign_id'])) for _, row in df.iterrows()]
                self.logger.info(f"Успешно извлечено {len(result)} активных записей product_id и campaign_id")
                if self.session_state:
                    self.session_state.add_log_message(f"Успешно извлечено {len(result)} активных кампаний")
                return result

        except Exception as e:
            error_msg = f"Ошибка при извлечении активных product_id и campaign_id: {str(e)}"
            self.logger.error(error_msg)
            if self.session_state:
                self.session_state.add_log_message(error_msg)
            raise ValueError(error_msg)