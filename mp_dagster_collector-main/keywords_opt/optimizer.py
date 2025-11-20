import pandas as pd
import numpy as np
import logging

class AdCPCOptimizer:
    """
    Управляет оптимизацией стоимости за клик (CPC) для рекламных кампаний.

    Attributes:
        margin_rate (float): Ставка маржи для расчёта бюджета.
        logger (logging.Logger): Логгер для записи событий и ошибок.
        session_state (SessionState): Объект состояния сессии для логирования сообщений.
    """
    def __init__(self, margin_rate: float, session_state, logger: logging.Logger):
        self.margin_rate = margin_rate
        self.logger = logger
        self.session_state = session_state

    def optimize_cpc(self, campaign_data: dict, clusters_data: pd.DataFrame):
        """
        Выполняет оптимизацию CPC на основе данных кампании и кластеров.

        Args:
            campaign_data (dict): Данные кампании (revenue, items, cost_price, cost, avg_cpi, avg_cpc, commission_rate).
            clusters_data (pd.DataFrame): Данные кластеров с колонками cluster, avg_cpc, total_clicks, total_sum.

        Returns:
            tuple: DataFrame с валидными кластерами, максимальная стоимость за клик (max_cpc), прибыль (profit).
        """
        unit_price = campaign_data['revenue'] / campaign_data['items'] if campaign_data['items'] > 0 else 0
        ad_budget = unit_price * (1 - campaign_data['commission_rate']) - campaign_data['cost_price'] - (unit_price * self.margin_rate)
        coeff = ad_budget / campaign_data['avg_cpi'] if campaign_data['avg_cpi'] > 0 else 1
        max_cpc = campaign_data['avg_cpc'] * coeff

        profit = campaign_data['revenue'] * (1 - campaign_data['commission_rate']) - campaign_data['cost_price']

        total_clicks = clusters_data['total_clicks'].sum()
        ad_cost_per_item = campaign_data['cost'] / campaign_data['items'] if campaign_data['items'] > 0 else float('inf')

        if max_cpc <= 0 or not np.isfinite(max_cpc) or (total_clicks == 0 and ad_cost_per_item >= ad_budget):
            if profit < ad_cost_per_item:
                self.logger.warning(
                    f"Прибыль ({profit:.2f}) меньше затрат на рекламу одного товара ({ad_cost_per_item:.2f}), возвращается пустой DataFrame")
                self.session_state.add_log_message(
                    f"Прибыль ({profit:.2f}) меньше затрат на рекламу одного товара ({ad_cost_per_item:.2f})")
                return pd.DataFrame(columns=['cluster', 'avg_cpc', 'total_clicks', 'total_sum']), 0, profit
            valid_clusters = clusters_data[clusters_data['total_sum'] <= profit]
            self.logger.info(f"Отфильтровано {len(valid_clusters)} кластеров с total_sum <= profit ({profit:.2f})")
            return valid_clusters, 0, profit

        condition_with_cpc = clusters_data['avg_cpc'].notna() & (clusters_data['avg_cpc'] <= max_cpc)
        condition_without_cpc = clusters_data['avg_cpc'].isna() & (clusters_data['total_sum'] < profit)

        valid_clusters = clusters_data[condition_with_cpc | condition_without_cpc]
        self.logger.info(f"Результаты оптимизации: max_cpc={max_cpc:.2f}, valid_keywords={len(valid_clusters)}")
        return valid_clusters, max_cpc, profit