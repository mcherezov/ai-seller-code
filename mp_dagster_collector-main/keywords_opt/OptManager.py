import pandas as pd
import logging
import time
from typing import List
from api.wb_api_actions import WildberriesAPIactions
from api.wb_api import WildberriesAPI

class OptimizationManager:
    """
    Координирует процесс оптимизации ключевых слов для рекламной кампании.

    Attributes:
        session_state (SessionState): Объект состояния сессии для хранения параметров и логов.
        db_manager (DatabaseManager): Менеджер базы данных для извлечения и сохранения данных.
        optimizer (AdCPCOptimizer): Оптимизатор стоимости за клик (CPC).
        notification_manager (NotificationManager): Менеджер уведомлений для отправки сообщений в Telegram.
        logger (logging.Logger): Логгер для записи событий и ошибок.
    """
    def __init__(self, session_state, db_manager, optimizer, notification_manager, logger: logging.Logger):
        """
        Инициализирует менеджер оптимизации с необходимыми компонентами.

        Args:
            session_state (SessionState): Объект состояния сессии.
            db_manager (DatabaseManager): Менеджер базы данных.
            optimizer (AdCPCOptimizer): Оптимизатор CPC.
            notification_manager (NotificationManager): Менеджер уведомлений.
            logger (logging.Logger): Логгер для записи событий.
        """
        self.session_state = session_state
        self.db_manager = db_manager
        self.optimizer = optimizer
        self.notification_manager = notification_manager
        self.logger = logger
        token = self.db_manager.db_config.get('WB_API_TOKEN')
        if not token:
            self.logger.error("WB_API_TOKEN отсутствует в конфигурации")
            raise ValueError("WB_API_TOKEN не предоставлен")
        self.logger.debug(f"WB_API_TOKEN (первые 10 символов): {token[:10]}...")
        self.wb_api = WildberriesAPIactions(
            token=token,
            logger=self.logger,
            use_bearer=True
        )
        self.wb_api_requests = WildberriesAPI(token)

    def run_optimization(self, product_id: str, campaign_id: int, start_date: str, end_date: str, cost_price: float,
                        margin_rate: float, commission_rate: float) -> tuple[pd.DataFrame, float, int, str]:
        """
        Выполняет процесс оптимизации ключевых слов.

        Args:
            product_id (str): Идентификатор продукта.
            campaign_id (int): Идентификатор кампании.
            start_date (str): Начальная дата периода в формате ISO.
            end_date (str): Конечная дата периода в формате ISO.
            cost_price (float): Себестоимость продукта.
            margin_rate (float): Ставка маржи.
            commission_rate (float): Ставка комиссии.

        Returns:
            tuple: DataFrame с рекомендациями, max_cpc, campaign_id, product_id.
        """
        try:
            self.logger.info(
                f"Запуск оптимизации для product_id={product_id}, campaign_id={campaign_id} за {start_date} - {end_date}")

            ad_stats_df = self.db_manager.get_ad_stats(product_id=product_id, campaign_id=campaign_id,
                                                      start_date=start_date, end_date=end_date)
            if ad_stats_df is None or ad_stats_df.empty:
                self.logger.error(f"Метод get_ad_stats() не вернул данные для product_id={product_id}, campaign_id={campaign_id}")
                self.session_state.add_log_message(
                     f"Метод get_ad_stats() не вернул данные для product_id={product_id}, campaign_id={campaign_id}")
                return pd.DataFrame(), 0, campaign_id, product_id

            if ad_stats_df['campaign_id'].isna().any():
                self.logger.error("В campaign_id обнаружены значения NaN")
                self.session_state.add_log_message("В campaign_id обнаружены значения NaN")
                return pd.DataFrame(), 0, campaign_id, product_id

            campaign_data_raw = {
                'campaign_id': int(ad_stats_df['campaign_id'].iloc[0]),
                'revenue': ad_stats_df['revenue'].sum(),
                'items': ad_stats_df['items'].sum(),
                'cost': ad_stats_df['cost'].sum()
            }

            campaign_id = campaign_data_raw['campaign_id']
            clusters_df = self.db_manager.get_cluster_stats(ad_id=str(campaign_id), start_date=start_date,
                                                           end_date=end_date)
            if clusters_df is None or clusters_df.empty:
                self.logger.error(f"Не удалось извлечь данные ключевых слов для campaign_id={campaign_id}")
                self.session_state.add_log_message(
                    f"Не удалось извлечь данные ключевых слов для campaign_id={campaign_id}")
                return pd.DataFrame(), 0, campaign_id, product_id

            clusters_agg = clusters_df.groupby('cluster_name').agg({
                'cpc': 'mean',
                'clicks': 'sum',
                'sum': 'sum'
            }).reset_index()
            clusters_agg = clusters_agg.rename(columns={
                'cluster_name': 'cluster',
                'cpc': 'avg_cpc',
                'clicks': 'total_clicks',
                'sum': 'total_sum'
            })

            nan_keywords = clusters_agg[clusters_agg['avg_cpc'].isna()]
            if not nan_keywords.empty:
                self.logger.info(f"Найдено {len(nan_keywords)} ключевых слов с NaN CPC из-за нулевых кликов")
                self.session_state.add_log_message(
                    f"Найдено {len(nan_keywords)} ключевых слов с NaN CPC из-за нулевых кликов")

            total_clicks = clusters_agg['total_clicks'].sum()
            avg_cpc = campaign_data_raw['cost'] / total_clicks if total_clicks > 0 else 0
            avg_cpi = campaign_data_raw['cost'] / campaign_data_raw['items'] if campaign_data_raw['items'] > 0 else 0

            campaign_data = {
                'revenue': campaign_data_raw['revenue'],
                'items': campaign_data_raw['items'],
                'cost_price': cost_price,
                'cost': campaign_data_raw['cost'],
                'avg_cpi': avg_cpi,
                'avg_cpc': avg_cpc,
                'commission_rate': commission_rate
            }

            self.logger.info(f"Данные кампании: {campaign_data}")
            self.logger.info(
                f"Параметры оптимизации: total_clicks={total_clicks}, avg_cpc={avg_cpc:.2f}, avg_cpi={avg_cpi:.2f}")

            valid_clusters, max_cpc, profit = self.optimizer.optimize_cpc(campaign_data, clusters_agg)

            recommendations = clusters_agg.copy()
            recommendations['status'] = recommendations.apply(
                lambda row: 'Оставить' if row['cluster'] in valid_clusters['cluster'].values else 'Исключить', axis=1
            )
            recommendations['recommendation'] = recommendations.apply(
                lambda row: (
                    f'Ключевое слово эффективно, CPC ({row["avg_cpc"]:.2f}) <= max_cpc ({max_cpc:.2f})'
                    if row['status'] == 'Оставить' and not pd.isna(row['avg_cpc']) and max_cpc > 0
                    else (
                        f'Ключевое слово оставлено (без кликов), total_sum ({row["total_sum"]:.2f} ₽) <= profit ({profit:.2f} ₽)'
                        if row['status'] == 'Оставить' and max_cpc == 0
                        else (
                            f'Ключевое слово неэффективно, CPC ({row["avg_cpc"]:.2f}) > max_cpc ({max_cpc:.2f})'
                            if row['status'] == 'Исключить' and not pd.isna(row['avg_cpc']) and max_cpc > 0
                            else f'Ключевое слово исключено (без кликов), total_sum ({row["total_sum"]:.2f} ₽) > profit ({profit:.2f} ₽)'
                        )
                    )
                ), axis=1
            )
            recommendations = recommendations[
                ['cluster', 'avg_cpc', 'total_clicks', 'total_sum', 'status', 'recommendation']]
            recommendations['avg_cpc'] = recommendations['avg_cpc'].round(2)

            self.logger.info(
                f"Оптимизация ключевых слов завершена: {len(valid_clusters)} валидных ключевых слов, max_cpc={max_cpc:.2f}")
            return recommendations, max_cpc, campaign_id, product_id

        except Exception as e:
            self.logger.error(f"Ошибка оптимизации: {str(e)}")
            self.session_state.add_log_message(f"Ошибка оптимизации: {str(e)}")
            return pd.DataFrame(), 0, campaign_id, product_id

    def execute(self):
        """
        Выполняет полный цикл оптимизации для всех активных кампаний.

        Returns:
            list[tuple[pd.DataFrame, float, int, str]]: Список результатов оптимизации для каждой кампании
            (DataFrame с рекомендациями, max_cpc, campaign_id, product_id).
        """
        self.logger.debug("Начало выполнения оптимизации для всех активных кампаний")
        results = []
        try:
            active_campaigns = self.db_manager.get_active_product_id()
            if not active_campaigns:
                self.logger.error("Нет активных кампаний для оптимизации")
                self.session_state.add_log_message("Нет активных кампаний для оптимизации")
                message = "❌ *Ошибка оптимизации*\nНет активных кампаний для оптимизации."
                self.notification_manager.send_message(message)
                return results

            self.logger.info(f"Найдено {len(active_campaigns)} активных кампаний для оптимизации")
            for product_id, campaign_id in active_campaigns:
                self.logger.info(f"Оптимизация для product_id={product_id}, campaign_id={campaign_id}")
                try:
                    self.session_state.set_commission_rate(self.db_manager, product_id=product_id)
                    self.session_state.set_cost_price(self.db_manager, product_id=product_id)

                    params = self.session_state.get_parameters()
                    params['product_id'] = product_id
                    params['campaign_id'] = campaign_id
                    params['start_date'] = self.session_state.start_date
                    params['end_date'] = self.session_state.end_date
                    params['cost_price'] = self.session_state.cost_price
                    params['margin_rate'] = self.session_state.margin_rate
                    params['commission_rate'] = self.session_state.commission_rate

                    recommendations, max_cpc, campaign_id, product_id = self.run_optimization(
                        product_id=product_id,
                        campaign_id=campaign_id,
                        start_date=params['start_date'].isoformat(),
                        end_date=params['end_date'].isoformat(),
                        cost_price=params['cost_price'],
                        margin_rate=params['margin_rate'],
                        commission_rate=params['commission_rate']
                    )

                    if not recommendations.empty:
                        excluded_keywords = recommendations[recommendations['status'] == 'Исключить']['cluster'].tolist()
                        valid_keywords = recommendations[recommendations['status'] == 'Оставить']['cluster'].tolist()
                        total_keywords = len(recommendations)
                        self.logger.info(f"Результаты оптимизации: Кампания ID: {campaign_id}, Товар ID: {product_id}")
                        self.logger.info(f"Максимальный CPC: {max_cpc:.2f}")
                        self.logger.info(f"Валидных ключевых слов: {len(valid_keywords)}")
                        self.logger.info(f"Исключено ключевых слов: {len(excluded_keywords)}")
                        self.logger.info("Рекомендации:")
                        for index, row in recommendations.iterrows():
                            self.logger.debug(
                                f"{row['cluster']} | {row['avg_cpc']:.2f} | {row['total_clicks']} | {row['total_sum']:.2f} | {row['status']} | {row['recommendation']}")

                        update_success = False
                        if excluded_keywords:
                            campaign_info = self.wb_api_requests.get_campaigns_info([campaign_id])
                            if not campaign_info or not isinstance(campaign_info, list):
                                self.logger.error(f"Не удалось получить информацию о кампании {campaign_id}")
                                self.session_state.add_log_message(
                                    f"Не удалось получить информацию о кампании {campaign_id}")
                                result = False
                            else:
                                campaign_type = campaign_info[0].get("type")
                                if campaign_type == 8:
                                    result = self.wb_api.set_excluded_phrases(
                                        campaign_id=campaign_id,
                                        excluded_phrases=excluded_keywords
                                    )
                                elif campaign_type == 9:
                                    result = self.wb_api.set_search_excluded_phrases(
                                        campaign_id=campaign_id,
                                        excluded_phrases=excluded_keywords
                                    )
                                else:
                                    self.logger.error(
                                        f"Кампания {campaign_id} имеет неподдерживаемый тип: {campaign_type}")
                                    self.session_state.add_log_message(
                                        f"Кампания {campaign_id} имеет неподдерживаемый тип: {campaign_type}")
                                    result = False

                                if result is not None:
                                    self.logger.info(
                                        f"Минус-фразы обновлены для campaign_id={campaign_id}: {excluded_keywords}")
                                    self.session_state.add_log_message(
                                        f"Минус-фразы обновлены для campaign_id={campaign_id}")
                                    update_success = True
                                else:
                                    self.logger.error(f"Не удалось обновить минус-фразы для campaign_id={campaign_id}")
                                    self.session_state.add_log_message(
                                        f"Не удалось обновить минус-фразы для campaign_id={campaign_id}")
                                    message = (
                                        f"⚠️ *Ошибка обновления минус-фраз*\n"
                                        f"Кампания ID: {campaign_id}\n"
                                        f"Товар ID: {product_id}\n"
                                        f"Не удалось обновить минус-фразы. Проверьте логи."
                                    )
                                    self.notification_manager.send_message(message)

                        if self.db_manager.save_optimization_results(recommendations, campaign_id, product_id, max_cpc):
                            self.logger.info("Результаты успешно сохранены в базу данных")
                            summary = (
                                f"ℹ️ *Оптимизация ключевых слов завершена*\n"
                                f"Кампания ID: {campaign_id}\n"
                                f"Товар ID: {product_id}\n"
                                f"Проанализировано ключевых слов: {total_keywords}\n"
                                f"Максимальный CPC: {max_cpc:.2f} ₽\n"
                                f"Валидных ключевых слов: {len(valid_keywords)}\n"
                                f"Исключено ключевых слов: {len(excluded_keywords)}\n"
                                f"Минус-фразы обновлены: {len(excluded_keywords) if update_success else 'ошибка'}\n"
                                f"Результаты сохранены в базу данных."
                            )
                            self.notification_manager.send_message(summary)
                        else:
                            self.logger.error("Не удалось сохранить результаты в базу данных")
                            error_msg = (
                                f"⚠️ *Ошибка оптимизации*\n"
                                f"Кампания ID: {campaign_id}\n"
                                f"Товар ID: {product_id}\n"
                                f"Проанализировано ключевых слов: {total_keywords}\n"
                                f"Не удалось сохранить результаты в базу данных."
                            )
                            self.notification_manager.send_message(error_msg)
                    else:
                        self.logger.error(
                            f"Не удалось выполнить оптимизацию для campaign_id={campaign_id}, product_id={product_id}")
                        message = (
                            f"❌ *Ошибка оптимизации*\n"
                            f"Кампания ID: {campaign_id}\n"
                            f"Товар ID: {product_id}\n"
                            f"Не удалось выполнить оптимизацию. Проверьте логи:\n"
                            f"{'; '.join(self.session_state.log_messages[-3:])}"
                        )
                        self.notification_manager.send_message(message)

                    results.append((recommendations, max_cpc, campaign_id, product_id))

                    if excluded_keywords:
                        time.sleep(0.5)

                except Exception as e:
                    self.logger.error(
                        f"Ошибка оптимизации для campaign_id={campaign_id}, product_id={product_id}: {str(e)}")
                    self.session_state.add_log_message(
                        f"Ошибка оптимизации для campaign_id={campaign_id}, product_id={product_id}: {str(e)}")
                    message = (
                        f"❌ *Ошибка оптимизации*\n"
                        f"Кампания ID: {campaign_id}\n"
                        f"Товар ID: {product_id}\n"
                        f"Проверьте логи:\n"
                        f"{'; '.join(self.session_state.log_messages[-3:])}"
                    )
                    self.notification_manager.send_message(message)
                    continue

            unique_log_messages = list(dict.fromkeys(self.session_state.log_messages))
            if unique_log_messages:
                for msg in unique_log_messages:
                    self.logger.info(f"Лог: {msg}")

            if not results:
                self.logger.info("Оптимизация не выполнена: нет результатов")
            else:
                self.logger.info(f"Оптимизация завершена для {len(results)} кампаний")

            return results

        except Exception as e:
            self.logger.error(f"Критическая ошибка выполнения оптимизации: {str(e)}")
            self.session_state.add_log_message(f"Критическая ошибка выполнения оптимизации: {str(e)}")
            message = (
                f"❌ *Критическая ошибка оптимизации*\n"
                f"Проверьте логи:\n"
                f"{'; '.join(self.session_state.log_messages[-3:])}"
            )
            self.notification_manager.send_message(message)
            return results

if __name__ == "__main__":
    from logs.logging_setup import LoggerManager
    from session_state import SessionState
    from sql.database_manager_ako import DatabaseManager
    from optimizer import AdCPCOptimizer
    from bot.notifications import NotificationManager
    from config import ConfigManager

    logger_manager = LoggerManager(log_file='optimization.log')
    logger = logger_manager.get_logger()
    config_manager = ConfigManager(logger=logger)
    db_config, error_msg = config_manager.get_config()

    if error_msg:
        logger.error(error_msg)
        message = f"❌ *Критическая ошибка конфигурации*\nОшибка: {error_msg}"
        if db_config.get('telegram_bot_token') and db_config.get('telegram_chat_id'):
            notification_manager = NotificationManager(
                db_config.get('telegram_bot_token'),
                db_config.get('telegram_chat_id'),
                None,
                logger
            )
            notification_manager.send_message(message)
        raise ValueError(error_msg)

    logger.debug(
        f"Загруженная конфигурация: { {k: v[:10] + '...' if k in ['password', 'WB_API_TOKEN', 'telegram_bot_token'] else v for k, v in db_config.items()} }"
    )

    try:
        db_manager = DatabaseManager(db_config, None, logger)
        session_state = SessionState(db_manager)
        db_manager.session_state = session_state
        optimizer = AdCPCOptimizer(session_state.margin_rate, session_state, logger)
        notification_manager = NotificationManager(
            db_config.get('telegram_bot_token'),
            db_config.get('telegram_chat_id'),
            session_state,
            logger
        )
        optimization_manager = OptimizationManager(session_state, db_manager, optimizer, notification_manager, logger)

        results = optimization_manager.execute()

        if results:
            for recommendations, max_cpc, camp_id, prod_id in results:
                logger.info(f"Кампания ID: {camp_id}, Товар ID: {prod_id}, max_cpc: {max_cpc:.2f}")
        else:
            logger.info("Оптимизация не выполнена: нет результатов")

    except Exception as e:
        logger.error(f"Критическая ошибка в главном блоке: {str(e)}")
        message = f"❌ *Критическая ошибка оптимизации*\nОшибка: {str(e)}"
        if db_config.get('telegram_bot_token') and db_config.get('telegram_chat_id'):
            notification_manager = NotificationManager(
                db_config.get('telegram_bot_token'),
                db_config.get('telegram_chat_id'),
                None,
                logger
            )
            notification_manager.send_message(message)
        raise