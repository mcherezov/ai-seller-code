import os
from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class UCBBanditConfig:
    """Класс для управления конфигурацией параметров UCBBanditCPM."""

    def __init__(
            self,
            advert_id: str,
            reward_metric: str = "roi_weighted",
            daily_budget: float = None,
            max_cpm_change: float = 0.3,
            db_config: Dict = None,
            target_cpa: float = None,
            exploration_factor: float = 2.0,
            exploration_decay_rate: float = 0.001,
            adaptive_exploration: bool = False,
            min_pulls_to_prune: int = 10,
            reward_penalty_thresholds: Dict = None,
            normalization_method: str = "minmax",
            composite_metric_weights: Dict = None,
            min_cpm_absolute: float = 50.0,
            max_cpm_absolute: float = None,
            cpm_step_size: float = 25.0,
            auto_pause_threshold: int = 5,
            reassess_initial_cpm_interval: int = 7,
            period_min_carts: int = 0,
            period_max_hours: int = 24,
            cost_price: float = 500.0,
            marketplace_fee: float = 100.0,
            warehouse_cost: float = 1.0,
            min_bid: Optional[float] = None,
            stocks: float = 1000.0
    ):
        self.advert_id = advert_id
        self.reward_metric = reward_metric.lower()
        self.daily_budget = daily_budget
        self.max_cpm_change = max_cpm_change
        self.cost_price = cost_price
        self.min_bid = min_bid if min_bid is None or min_bid >= 0 else None
        self.marketplace_fee = marketplace_fee
        self.warehouse_cost = warehouse_cost
        self.stocks = stocks
        self.db_config = db_config or {
           
        }
        self.target_cpa = target_cpa
        self.exploration_factor = exploration_factor
        self.exploration_decay_rate = exploration_decay_rate
        self.adaptive_exploration = adaptive_exploration
        self.min_pulls_to_prune = min_pulls_to_prune
        self.reward_penalty_thresholds = reward_penalty_thresholds or {
            "cpa": 1.5,
            "orders_atbs": 0.05,
            "cr_to_cart": 0.5,
            "cr_to_order": 0.1
        }
        self.normalization_method = normalization_method
        self.composite_metric_weights = composite_metric_weights or {
            "roi_carts": 0.5,
            "roi_orders": 0.5
        }
        self.min_cpm_absolute = min_cpm_absolute
        self.max_cpm_absolute = max_cpm_absolute
        self.cpm_step_size = cpm_step_size
        self.auto_pause_threshold = auto_pause_threshold
        self.reassess_initial_cpm_interval = reassess_initial_cpm_interval
        self.period_min_carts = period_min_carts
        self.period_max_hours = period_max_hours
        self.arms = [f"{i}%" for i in range(-50, 51, 5)] + ["double", "reset"]
        self.initial_arms = [f"{i}%" for i in range(-10, 11, 5)] + ["0%"]

        valid_metrics = [
            "ctr", "clicks", "sum", "cpm", "ctr/cpm",
            "orders", "orders/cpm", "atbs", "atbs/cpm", "orders/atbs", "cpa",
            "cr_to_cart", "cr_to_order", "roi_carts", "roi_orders", "roi_weighted"
        ]
        for metric in list(self.composite_metric_weights):
            if metric not in valid_metrics:
                logger.warning(f"Недопустимая метрика '{metric}' в composite_metric_weights, игнорируется")
                self.composite_metric_weights.pop(metric)
        if not self.composite_metric_weights:
            self.composite_metric_weights = {"roi_weighted": 1.0}
            logger.warning("Нет допустимых метрик в composite_metric_weights, используется 'roi_weighted' по умолчанию")

        logger.info(
            f"Инициализирована конфигурация UCBBandit для advert_id {advert_id}, "
            f"метрика '{self.reward_metric}', бюджет={daily_budget}, target_cpa={target_cpa}"
        )
