import math
from typing import Dict, List

class UCBBanditRewards:
    """Класс для расчета метрик и наград в UCBBanditCPM."""

    def __init__(self, config):
        self.config = config
        self.reward_history: List[float] = []

    def calculate_avg_warehouse_cost(self, items: float, period_hours: float) -> float:
        """Рассчитывает среднюю стоимость хранения на складе."""
        if items <= 0 or period_hours <= 0:
            return 0.0
        period_days = period_hours / 24.0
        sales_rate = items / period_days
        if sales_rate == 0:
            return 0.0
        days_to_sell = self.config.stocks / sales_rate
        return self.config.warehouse_cost * days_to_sell

    def normalize_reward(self, reward: float) -> float:
        """Нормализует награду с использованием указанного метода."""
        self.reward_history.append(reward)
        if len(self.reward_history) > 1000:
            self.reward_history.pop(0)
        if not self.reward_history:
            return reward
        if self.config.normalization_method == "minmax":
            min_reward = min(self.reward_history)
            max_reward = max(self.reward_history)
            if max_reward == min_reward:
                return 0.5 if reward > 0 else 0.0
            return (reward - min_reward) / (max_reward - min_reward)
        elif self.config.normalization_method == "exp":
            return 1 - math.exp(-reward)
        return reward

    def calculate_metric_reward(
            self,
            metric: str,
            stat: Dict,
            views: float,
            cost: float,
            clicks: float,
            orders: float,
            atbs: float,
            items: float,
            revenue: float
    ) -> float:
        """Рассчитывает награду для указанной метрики."""
        cpm = float(stat.get("ad_rate", 0.0))
        ctr = (clicks / views * 100) if views > 0 else 0.0
        cpa = (cost / atbs) if atbs > 0 else 1000.0
        cr_to_cart = (atbs / views * 100) if views > 0 else 0.0
        cr_to_order = (orders / views * 100) if views > 0 else 0.0

        avg_warehouse_cost = self.calculate_avg_warehouse_cost(items, self.config.period_max_hours)
        if atbs > 0:
            total_costs_carts = (
                self.config.cost_price + self.config.marketplace_fee + cost / atbs + avg_warehouse_cost
            ) * atbs
            roi_carts = (revenue / total_costs_carts - 1) * 100 if total_costs_carts > 0 else 0.0
        else:
            roi_carts = 0.0
        if orders > 0:
            total_costs_orders = (
                self.config.cost_price + self.config.marketplace_fee + cost / orders + avg_warehouse_cost
            ) * orders
            roi_orders = (revenue / total_costs_orders - 1) * 100 if total_costs_orders > 0 else 0.0
        else:
            roi_orders = 0.0

        if metric == "ctr":
            return ctr
        elif metric == "clicks":
            return clicks
        elif metric == "sum":
            return cost
        elif metric == "cpm":
            return 1000 / (cpm + 1) if cpm > 0 else 0.0
        elif metric == "ctr/cpm":
            return ctr / (cpm + 1) if cpm > 0 else 0.0
        elif metric == "orders":
            return orders
        elif metric == "orders/cpm":
            return orders / (cpm + 1) if cpm > 0 else 0.0
        elif metric == "atbs":
            return atbs
        elif metric == "atbs/cpm":
            return atbs / (cpm + 1) if cpm > 0 else 0.0
        elif metric == "orders/atbs":
            return (orders / (atbs + 1)) if orders > 0 else 0.0
        elif metric == "cpa":
            return 1 / (cpa + 1) if cpa > 0 else 0.0
        elif metric == "cr_to_cart":
            return cr_to_cart
        elif metric == "cr_to_order":
            return cr_to_order
        elif metric == "roi_carts":
            return roi_carts
        elif metric == "roi_orders":
            return roi_orders
        elif metric == "roi_weighted":
            return (
                self.config.composite_metric_weights.get("roi_carts", 0.0) * roi_carts +
                self.config.composite_metric_weights.get("roi_orders", 0.0) * roi_orders
            )
        return 0.0

    def apply_reward_penalties(
            self,
            reward: float,
            cpa: float,
            total_atbs: float,
            total_orders: float,
            cr_to_cart: float,
            cr_to_order: float,
            total_cost: float
    ) -> float:
        """Применяет штрафы к награде на основе пороговых значений метрик."""
        if (
            self.config.target_cpa and
            cpa > self.config.target_cpa * self.config.reward_penalty_thresholds["cpa"]
        ):
            reward *= 0.5
        if (
            total_atbs > 0 and
            total_orders > 0 and
            total_orders / total_atbs < self.config.reward_penalty_thresholds["orders_atbs"]
        ):
            reward *= 0.7
        if cr_to_cart < self.config.reward_penalty_thresholds["cr_to_cart"]:
            reward *= 0.8
        if cr_to_order < self.config.reward_penalty_thresholds["cr_to_order"]:
            reward *= 0.8
        if self.config.daily_budget and total_cost > self.config.daily_budget * 1.2:
            penalty = max(0.3, self.config.daily_budget / total_cost)
            reward *= penalty
        return reward