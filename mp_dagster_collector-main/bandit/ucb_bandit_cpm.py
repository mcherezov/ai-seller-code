import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict
from config_usb import UCBBanditConfig
from sql.database_manager import UCBBanditDatabase
from metrics_calculator import UCBBanditRewards
from ucb_algorithm import UCBBanditLogic

logger = logging.getLogger(__name__)

class UCBBanditCPM:
    """Основной класс для управления UCB-алгоритмом оптимизации CPM."""

    def __init__(self, **kwargs):
        self.config = UCBBanditConfig(**kwargs)
        self.db = UCBBanditDatabase(self.config.db_config)
        self.rewards = UCBBanditRewards(self.config)
        self.bandit = UCBBanditLogic(self.config)
        self.initial_cpm = self.db.reassess_initial_cpm(self.config.advert_id)
        self.current_cpm = self.initial_cpm or self.config.min_bid or 150.0
        self.failed_attempts = 0
        self.recommendations = pd.DataFrame(columns=[
            "campaign_id", "timestamp", "arm", "reward", "metric",
            "current_cpm", "recommended_cpm", "cpa", "cr_to_cart",
            "cr_to_order", "roi_carts", "roi_orders", "roi_weighted", "recommendation"
        ])

        counts, rewards, cpms = self.db.load_bandit_state(self.config.advert_id, self.config.arms)
        self.bandit.counts.update(counts)
        self.bandit.rewards.update(rewards)
        self.bandit.cpms.update(cpms)
        self.bandit.total_pulls = sum(counts.values())
        logger.info(f"Инициализирован UCBBanditCPM для advert_id {self.config.advert_id}, initial_cpm={self.initial_cpm}, current_cpm={self.current_cpm}")

    def recommend_action(
            self,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            days: int = 5,
            max_days: int = 60
    ) -> Optional[Dict]:
        """Формирует рекомендацию по изменению CPM на основе данных и UCB-алгоритма."""
        if not self.config.advert_id:
            logger.error("Не указан advert_id")
            return {
                "arm'sarm": "", "reward": 0.0, "metric": self.config.reward_metric,
                "current_cpm": 0.0, "recommended_cpm": 0.0,
                "recommendation": "Не указан advert_id"
            }

        # Переоценка initial_cpm, если необходимо
        if self.bandit.total_pulls % self.config.reassess_initial_cpm_interval == 0:
            self.initial_cpm = self.db.reassess_initial_cpm(self.config.advert_id)
            self.current_cpm = self.initial_cpm or self.config.min_bid or 150.0
            logger.info(f"Переоценен initial_cpm для {self.config.advert_id}: {self.initial_cpm}, current_cpm={self.current_cpm}")

        # Извлечение данных
        if start_date is None or end_date is None:
            end_date_dt = datetime.now().date() - timedelta(days=1)
            start_date_dt = end_date_dt - timedelta(days=days - 1)
            start_date = start_date_dt.isoformat()
            end_date = end_date_dt.isoformat()

        df = self.db.fetch_sql_data(self.config.advert_id, start_date, end_date)
        if df.empty and days < max_days:
            max_end_date = datetime.now().date() - timedelta(days=1)
            max_start_date = max_end_date - timedelta(days=max_days - 1)
            df = self.db.fetch_sql_data(self.config.advert_id, max_start_date.isoformat(), max_end_date.isoformat())
            logger.info(f"Расширен период для {self.config.advert_id}: {max_start_date} - {max_end_date}")

        # Получение current_cpm из данных или использование значения по умолчанию
        if not df.empty and 'ad_rate' in df.columns and not df['ad_rate'].isna().all():
            self.current_cpm = float(df['ad_rate'].iloc[-1])
            logger.info(f"Извлечен current_cpm из данных: {self.current_cpm}")
        else:
            logger.warning(f"Не удалось извлечь current_cpm из данных, используется current_cpm={self.current_cpm}")

        # Выбор руки с использованием current_cpm и initial_cpm
        selected_arm = self.bandit.select_arm(current_cpm=self.current_cpm,initial_cpm=self.initial_cpm)

        stop_iteration = False
        recommended_cpm = self.bandit.apply_cpm_change(selected_arm, self.current_cpm, self.initial_cpm)
        cpa = 0.0
        cr_to_cart = 0.0
        cr_to_order = 0.0
        roi_carts = 0.0
        roi_orders = 0.0
        roi_weighted = 0.0
        total_atbs = 0
        total_orders = 0
        total_clicks = 0
        total_views = 0
        total_cost = 0.0
        total_items = 0
        total_revenue = 0.0
        reward = 0.0
        recommendation = f"Нет данных для advert_id {self.config.advert_id}"

        if not self.db.check_table_exists():
            logger.error("Таблица algo.autobidder не существует")
            return {
                "arm": selected_arm, "reward": reward, "metric": self.config.reward_metric,
                "current_cpm": self.current_cpm, "recommended_cpm": recommended_cpm,
                "recommendation": "Таблица algo.autobidder не существует"
            }

        if not df.empty:
            stats = df.to_dict('records')
            if self.initial_cpm is None:
                self.initial_cpm = self.current_cpm
                logger.info(f"Установлен initial_cpm для {self.config.advert_id}: {self.initial_cpm}")

            total_clicks = df['ad_clicks'].sum()
            total_views = df['ad_views'].sum()
            total_cost = df['ad_cost'].sum()
            total_orders = df['ad_orders'].sum()
            total_atbs = df['ad_atbs'].sum()
            total_items = df['items'].sum()
            total_revenue = df['revenue'].sum()

            cpa = (total_cost / total_atbs) if total_atbs > 0 else 1000.0
            cr_to_cart = (total_atbs / total_views * 100) if total_views > 0 else 0.0
            cr_to_order = (total_orders / total_views * 100) if total_views > 0 else 0.0

            avg_warehouse_cost = self.rewards.calculate_avg_warehouse_cost(total_items, self.config.period_max_hours)
            if total_atbs > 0:
                total_costs_carts = (
                    self.config.cost_price + self.config.marketplace_fee + total_cost / total_atbs + avg_warehouse_cost
                ) * total_atbs
                roi_carts = (total_revenue / total_costs_carts - 1) * 100 if total_costs_carts > 0 else 0.0
            else:
                roi_carts = 0.0
            if total_orders > 0:
                total_costs_orders = (
                    self.config.cost_price + self.config.marketplace_fee + total_cost / total_orders + avg_warehouse_cost
                ) * total_orders
                roi_orders = (total_revenue / total_costs_orders - 1) * 100 if total_costs_orders > 0 else 0.0
            else:
                roi_orders = 0.0
            roi_weighted = (
                self.config.composite_metric_weights.get("roi_carts", 0.0) * roi_carts +
                self.config.composite_metric_weights.get("roi_orders", 0.0) * roi_orders
            )

            if stats:
                total_reward = 0.0
                count = len(stats)
                for stat in stats:
                    clicks = float(stat.get("ad_clicks", 0))
                    views = float(stat.get("ad_views", 1))
                    cost = float(stat.get("ad_cost", 0.0))
                    orders = float(stat.get("ad_orders", 0))
                    atbs = float(stat.get("ad_atbs", 0))
                    items = float(stat.get("items", 0))
                    revenue = float(stat.get("revenue", 0))

                    metric_reward = sum(
                        weight * self.rewards.calculate_metric_reward(
                            metric, stat, views, cost, clicks, orders, atbs, items, revenue
                        )
                        for metric, weight in self.config.composite_metric_weights.items()
                    )
                    total_reward += metric_reward
                reward = total_reward / count if count > 0 else 0.0

                reward = self.rewards.apply_reward_penalties(
                    reward, cpa, total_atbs, total_orders, cpa, cr_to_cart, cr_to_order
                )

            reward = self.rewards.normalize_reward(reward)
            recommended_cpm = max(self.config.min_bid or 150.0, recommended_cpm)
            recommendation = (
                f"Настройте кампанию {self.config.advert_id}: Установите CPM равный {recommended_cpm:.2f} ₽ "
                f"(текущий CPA: {cpa:.2f} ₽, ROI carts: {roi_carts:.2f}%, ROI orders: {roi_orders:.2f}%)"
            )
            if reward == 0.0 or total_views == 0:
                recommendation = f"Кампания {self.config.advert_id} неактивна или без показов, рассмотрите оптимизацию"
                stop_iteration = True
        else:
            self.failed_attempts += 1
            if self.failed_attempts >= self.config.auto_pause_threshold:
                stop_iteration = True
                recommendation = f"Нет данных для advert_id {self.config.advert_id}, рассмотрите паузу"
            self.bandit.update(selected_arm, 0.0, self.current_cpm)

        recommendation_data = {
            "campaign_id": self.config.advert_id,
            "timestamp": datetime.now().isoformat(),
            "arm": selected_arm,
            "reward": float(reward),
            "metric": self.config.reward_metric,
            "current_cpm": float(round(self.current_cpm)),
            "recommended_cpm": float(round(recommended_cpm)),
            "cpa": float(cpa),
            "cr_to_cart": float(cr_to_cart),
            "cr_to_order": float(cr_to_order),
            "roi_carts": float(roi_carts),
            "roi_orders": float(roi_orders),
            "roi_weighted": float(roi_weighted),
            "atbs": int(total_atbs),
            "orders": int(total_orders),
            "clicks": int(total_clicks),
            "views": int(total_views),
            "cost": float(total_cost),
            "items": int(total_items),
            "revenue": float(total_revenue),
            "cost_price": float(self.config.cost_price),
            "marketplace_fee": float(self.config.marketplace_fee),
            "warehouse_cost": float(self.config.warehouse_cost),
            "stocks": float(self.config.stocks),
            "stop_iteration": stop_iteration,
            "recommendation": recommendation
        }
        if not self.db.save_recommendation(recommendation_data):
            logger.error(f"Не удалось сохранить рекомендацию для {self.config.advert_id}")
            return {
                "arm": selected_arm, "reward": reward, "metric": self.config.reward_metric,
                "current_cpm": self.current_cpm, "recommended_cpm": recommended_cpm,
                "recommendation": "Не удалось сохранить в базу данных"
            }

        new_recommendation = pd.DataFrame([{
            "campaign_id": self.config.advert_id,
            "timestamp": datetime.now().isoformat(),
            "arm": selected_arm,
            "reward": float(reward),
            "metric": self.config.reward_metric,
            "current_cpm": float(self.current_cpm),
            "recommended_cpm": float(recommended_cpm),
            "cpa": float(cpa),
            "cr_to_cart": float(cr_to_cart),
            "cr_to_order": float(cr_to_order),
            "roi_carts": float(roi_carts),
            "roi_orders": float(roi_orders),
            "roi_weighted": float(roi_weighted),
            "recommendation": recommendation
        }])
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            if not new_recommendation.dropna(how='all').empty:
                self.recommendations = pd.concat([self.recommendations, new_recommendation], ignore_index=True)
            else:
                logger.warning("Пустой или полностью NA DataFrame рекомендации, пропускается объединение")

        return {
            "arm": selected_arm,
            "reward": reward,
            "metric": self.config.reward_metric,
            "current_cpm": self.current_cpm,
            "recommended_cpm": recommended_cpm,
            "recommendation": recommendation,
            "atbs": total_atbs,
            "orders": total_orders,
            "cpa": cpa,
            "cr_to_cart": cr_to_cart,
            "cr_to_order": cr_to_order,
            "roi_carts": roi_carts,
            "roi_orders": roi_orders,
            "roi_weighted": roi_weighted
        }

    def get_recommendations(self) -> pd.DataFrame:
        """Возвращает историю рекомендаций."""
        return self.recommendations

    def estimate_budget(self, days: int = 7) -> float:
        """Оценивает бюджет на основе данных за указанный период."""
        return self.db.estimate_budget(self.config.advert_id, days)

    def get_arm_stats(self) -> Dict:
        """Возвращает статистику по рукам."""
        return self.bandit.get_arm_stats()

    def update_arms(self, new_arms: Optional[list] = None):
        """Обновляет список рук."""
        self.bandit.update_arms(new_arms)