from wb_api import WildberriesAPI
import math
from typing import Dict, List, Optional
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class UCBBandit:
    """
    UCB1 Многорукий бандит для управления рекламными кампаниями Wildberries.
    Каждый рычаг (arm) — это ID кампании. Награда — метрика производительности (например, CTR, CPM, CTR/CPM).
    """
    def __init__(self, arms: List[str], reward_metric: str = "ctr", daily_budget: float = None, max_cpm_change: float = 0.3):
        """
        Инициализация бандита.

        Args:
            arms: Список идентификаторов рычагов (например, ID кампаний).
            reward_metric: Метрика для оптимизации ('ctr', 'clicks', 'sum', 'cpm', 'ctr/cpm').
            daily_budget: Дневной бюджет в рублях (опционально, для ограничения CPM).
            max_cpm_change: Максимальное изменение CPM относительно исходного в долях (по умолчанию 0.3, т.е. ±30%).
        """
        self.arms = arms
        self.reward_metric = reward_metric.lower()
        self.daily_budget = daily_budget
        self.max_cpm_change = max_cpm_change
        self.counts: Dict[str, int] = {arm: 0 for arm in arms}
        self.rewards: Dict[str, float] = {arm: 0.0 for arm in arms}
        self.cpms: Dict[str, float] = {arm: 0.0 for arm in arms}
        self.initial_cpms: Dict[str, float] = {arm: None for arm in arms} 
        self.total_pulls = 0
        valid_metrics = ["ctr", "clicks", "sum", "cpm", "ctr/cpm"]
        if self.reward_metric not in valid_metrics:
            logger.warning(f"Недопустимая метрика '{reward_metric}', установлена 'ctr'")
            self.reward_metric = "ctr"
        if not arms:
            logger.warning("Бандит инициализирован с пустым списком рычагов")
        else:
            logger.info(f"Инициализирован UCB1 бандит с {len(arms)} рычагами, метрикой '{self.reward_metric}', бюджет={daily_budget}, max_cpm_change={max_cpm_change}")

    def select_arm(self) -> str:
        """
        Выбор рычага по алгоритму UCB1 с учётом бюджета.

        Raises:
            ValueError: Если список рычагов пуст.
        """
        if not self.arms:
            logger.error("Список рычагов пуст, выбор невозможен")
            raise ValueError("Список рычагов пуст")

        self.total_pulls += 1

        for arm in self.arms:
            if self.counts[arm] == 0:
                logger.debug(f"Исследование рычага {arm} (ещё не выбирался)")
                return arm

        ucb_values = {}
        for arm in self.arms:
            avg_reward = self.rewards[arm] / self.counts[arm] if self.counts[arm] > 0 else 0
            exploration_term = math.sqrt(3 * math.log(self.total_pulls) / self.counts[arm])  
            ucb_values[arm] = avg_reward + exploration_term
            if self.daily_budget and self.cpms[arm] > 0:
                avg_cpm = self.cpms[arm]
                if avg_cpm > self.daily_budget:
                    ucb_values[arm] *= 0.1
                    logger.debug(f"Штраф для {arm}: CPM={avg_cpm:.2f} превышает бюджет {self.daily_budget}")
            logger.debug(f"Рычаг {arm}: AVG_REWARD={avg_reward:.4f}, EXP={exploration_term:.4f}, UCB={ucb_values[arm]:.4f}")

        selected_arm = max(ucb_values, key=ucb_values.get)
        logger.info(f"Выбран рычаг: {selected_arm} с UCB={ucb_values[selected_arm]:.4f}")
        return selected_arm

    def update(self, arm: str, reward: float, cpm: float = None):
        """
        Обновление состояния бандита после получения награды и CPM.

        Args:
            arm: Выбранный рычаг.
            reward: Полученная награда (зависит от reward_metric).
            cpm: CPM кампании (опционально, для отслеживания).
        """
        if arm not in self.arms:
            logger.warning(f"Неизвестный рычаг {arm} проигнорирован")
            return
        self.counts[arm] += 1
        self.rewards[arm] += reward
        if cpm is not None:
            self.cpms[arm] = ((self.cpms[arm] * (self.counts[arm] - 1)) + cpm) / self.counts[arm] if self.counts[arm] > 1 else cpm
        logger.debug(f"Обновлён {arm}: reward={reward:.4f}, cpm={cpm:.2f}, count={self.counts[arm]}, total_reward={self.rewards[arm]:.4f}")

    def recommend_action(self, wb_api: WildberriesAPI, start_date: str = None, end_date: str = None, days: int = 7, max_days: int = 30, campaign_info_dict: Dict = None) -> Optional[Dict]:
        """
        Рекомендация действия на основе UCB1 с ограничением изменения CPM на ±max_cpm_change относительно исходного CPM.

        Returns:
            Словарь с информацией о кампании, наградой, текущим и рекомендуемым CPM, и рекомендацией.
        """
        if not self.arms:
            logger.warning("Нет рычагов для рекомендации")
            return {"arm": None, "reward": 0.0, "metric": self.reward_metric, "current_cpm": 0.0, "recommended_cpm": 0.0, "recommendation": "Нет активных кампаний для анализа"}

        selected_arm = self.select_arm()

        if not hasattr(self, 'failed_attempts'):
            self.failed_attempts = {arm: 0 for arm in self.arms}

        if start_date is None or end_date is None:
            end_date_dt = datetime.now().date() - timedelta(days=1)
            start_date_dt = end_date_dt - timedelta(days=days - 1)
            start_date = start_date_dt.isoformat()
            end_date = end_date_dt.isoformat()

        if campaign_info_dict and str(selected_arm) in campaign_info_dict:
            campaign = campaign_info_dict[str(selected_arm)]
        else:
            campaign_info = wb_api.get_campaigns_info([int(selected_arm)])
            if campaign_info is None or not isinstance(campaign_info, list) or not campaign_info:
                logger.warning(f"Не удалось получить информацию о кампании {selected_arm}. Ответ API: {campaign_info}")
                self.failed_attempts[selected_arm] += 1
                self.update(selected_arm, 0.0)
                return {"arm": selected_arm, "reward": 0.0, "metric": self.reward_metric, "current_cpm": 0.0, "recommended_cpm": 0.0, "recommendation": f"Не удалось получить информацию о кампании {selected_arm}"}
            campaign = campaign_info[0]

        campaign_type = campaign.get("type")
        campaign_status = campaign.get("status")

        if campaign_status in [7, 11]:
            logger.warning(f"Кампания {selected_arm} неактивна (статус {campaign_status})")
            self.failed_attempts[selected_arm] += 1
            self.update(selected_arm, 0.0)
            return {"arm": selected_arm, "reward": 0.0, "metric": self.reward_metric, "current_cpm": 0.0, "recommended_cpm": 0.0, "recommendation": f"Кампания {selected_arm} неактивна, пропустите"}

        try:
            reward = 0.0
            current_cpm = campaign.get("autoParams", {}).get("cpm", 0.0)
            if not current_cpm:
                current_cpm = wb_api.get_campaign_bid(int(selected_arm)) or 0.0
                if not current_cpm:
                    logger.warning(f"CPM отсутствует в autoParams и bid для {selected_arm}, пытаемся рассчитать")

            if self.initial_cpms[selected_arm] is None and current_cpm > 0:
                self.initial_cpms[selected_arm] = current_cpm
                logger.info(f"Зафиксирован исходный CPM для {selected_arm}: {current_cpm:.2f}")

            stats = wb_api.get_stats_keywords(int(selected_arm), start_date=start_date, end_date=end_date)
            if not stats and days < max_days:
                logger.info(f"Попытка получить данные за {max_days} дней")
                max_end_date = datetime.now().date() - timedelta(days=1)
                max_start_date = max_end_date - timedelta(days=max_days - 1)
                stats = wb_api.get_stats_keywords(int(selected_arm), start_date=max_start_date.isoformat(), end_date=max_end_date.isoformat())

            total_clicks = 0
            total_views = 0
            total_sum = 0.0
            calculated_cpm = 0.0
            ctr = 0.0

            if stats:
                logger.info(f"Получены данные по ключевым словам для кампании {selected_arm}")
                total_reward = 0.0
                count = 0
                for stat in stats:
                    clicks = stat.get("clicks", 0)
                    views = stat.get("views", 1)
                    stat_sum = stat.get("sum", 0.0)
                    total_clicks += clicks
                    total_views += views
                    total_sum += stat_sum
                    if self.reward_metric == "ctr":
                        total_reward += stat.get("ctr", 0.0)
                    elif self.reward_metric == "clicks":
                        total_reward += clicks
                    elif self.reward_metric == "sum":
                        total_reward += stat_sum
                    elif self.reward_metric == "cpm":
                        calculated_cpm = (stat_sum / views * 1000) if views > 0 else 0.0
                        total_reward += 1000 / (calculated_cpm + 1)
                    elif self.reward_metric == "ctr/cpm":
                        ctr = (clicks / views * 100) if views > 0 else 0.0
                        calculated_cpm = (stat_sum / views * 1000) if views > 0 else 0.0
                        total_reward += ctr / (calculated_cpm + 1)
                    count += 1
                    logger.debug(f"Ключевое слово: {stat.get('keyword')}, {self.reward_metric}={stat.get(self.reward_metric, 0.0)}")
                
                reward = total_reward / count if count > 0 else 0.0
                if total_views > 0:
                    calculated_cpm = (total_sum / total_views * 1000)
                    ctr = (total_clicks / total_views * 100)
            else:
                logger.info(f"Кампания {selected_arm} (type={campaign_type}): используем get_campaign_stats")
                stats = wb_api.get_campaign_stats(int(selected_arm), start_date, end_date)
                if not stats:
                    logger.warning(f"Нет статистики для кампании {selected_arm}")
                    self.failed_attempts[selected_arm] += 1
                    if self.failed_attempts[selected_arm] >= 3:
                        logger.warning(f"Кампания {selected_arm} исключена из рычагов после {self.failed_attempts[selected_arm]} неудачных попыток")
                        self.arms.remove(selected_arm)
                    self.update(selected_arm, 0.0, current_cpm)
                    return {"arm": selected_arm, "reward": 0.0, "metric": self.reward_metric, "current_cpm": current_cpm, "recommended_cpm": 0.0, "recommendation": f"Нет данных для кампании {selected_arm}"}

                total_reward = 0.0
                count = 0
                for stat in stats:
                    clicks = stat.get("clicks", 0)
                    views = stat.get("views", 1)
                    stat_sum = stat.get("sum", 0.0)
                    total_clicks += clicks
                    total_views += views
                    total_sum += stat_sum
                    if self.reward_metric == "ctr":
                        total_reward += (clicks / views * 100) if views > 0 else 0.0
                    elif self.reward_metric == "clicks":
                        total_reward += clicks
                    elif self.reward_metric == "sum":
                        total_reward += stat_sum
                    elif self.reward_metric == "cpm":
                        calculated_cpm = (stat_sum / views * 1000) if views > 0 else 0.0
                        total_reward += 1000 / (calculated_cpm + 1)
                    elif self.reward_metric == "ctr/cpm":
                        ctr = (clicks / views * 100) if views > 0 else 0.0
                        calculated_cpm = (stat_sum / views * 1000) if views > 0 else 0.0
                        total_reward += ctr / (calculated_cpm + 1)
                    count += 1
                    logger.debug(f"Статистика кампании: {stat}")
                
                reward = total_reward / count if count > 0 else 0.0
                if total_views > 0:
                    calculated_cpm = (total_sum / total_views * 1000)
                    ctr = (total_clicks / total_views * 100)

            if not current_cpm and total_views > 0:
                current_cpm = calculated_cpm
                if self.initial_cpms[selected_arm] is None:
                    self.initial_cpms[selected_arm] = current_cpm
                    logger.info(f"Зафиксирован исходный CPM для {selected_arm}: {current_cpm:.2f}")

            recommended_cpm = current_cpm
            if total_views > 0 and self.daily_budget:
                recommended_cpm = min(calculated_cpm, self.daily_budget)
                if self.reward_metric in ["ctr", "ctr/cpm"] and ctr > 0:
                    efficiency = ctr / (calculated_cpm + 1)
                    if efficiency > 0.008:
                        recommended_cpm = min(calculated_cpm * 1.15, self.daily_budget) 
                    elif efficiency < 0.006:
                        recommended_cpm = max(calculated_cpm * 0.85, 50.0)  
                elif self.reward_metric == "ctr/cpm":
                    efficiency = ctr / (calculated_cpm + 1)
                    if efficiency > 0.008:
                        recommended_cpm = min(calculated_cpm * (1 + efficiency * 10), self.daily_budget)
                    elif efficiency < 0.006:
                        recommended_cpm = max(calculated_cpm * (1 - efficiency * 10), 50.0)

                if self.initial_cpms[selected_arm] is not None:
                    min_cpm = self.initial_cpms[selected_arm] * (1 - self.max_cpm_change)
                    max_cpm = self.initial_cpms[selected_arm] * (1 + self.max_cpm_change)
                    recommended_cpm = min(max(recommended_cpm, min_cpm), max_cpm)
                    logger.debug(f"Кампания {selected_arm}: Исходный CPM={self.initial_cpms[selected_arm]:.2f}, Ограничение CPM: min={min_cpm:.2f}, max={max_cpm:.2f}, recommended={recommended_cpm:.2f}")

            if self.daily_budget and calculated_cpm > self.daily_budget:
                penalty = max(0.5, self.daily_budget / calculated_cpm)
                reward *= penalty
                logger.warning(f"Кампания {selected_arm} превышает бюджет: CPM={calculated_cpm:.2f}, бюджет={self.daily_budget}, штраф={penalty:.2f}")

            self.update(selected_arm, reward, calculated_cpm)
            recommendation = f"Приоритет кампании {selected_arm} с {self.reward_metric}={reward:.4f}, CPM={calculated_cpm:.2f}"
            if reward == 0.0:
                recommendation = f"Кампания {selected_arm} не имеет активности, рассмотрите её оптимизацию"

            self.failed_attempts[selected_arm] = 0

            return {
                "arm": selected_arm,
                "reward": reward,
                "metric": self.reward_metric,
                "current_cpm": current_cpm,
                "recommended_cpm": recommended_cpm,
                "recommendation": recommendation
            }

        except Exception as e:
            logger.error(f"Ошибка при получении статистики для рычага {selected_arm}: {e}")
            self.failed_attempts[selected_arm] += 1
            if self.failed_attempts[selected_arm] >= 3:
                logger.warning(f"Кампания {selected_arm} исключена из рычагов после {self.failed_attempts[selected_arm]} неудачных попыток")
                self.arms.remove(selected_arm)
            self.update(selected_arm, 0.0, current_cpm)
            return {"arm": selected_arm, "reward": 0.0, "metric": self.reward_metric, "current_cpm": current_cpm, "recommended_cpm": 0.0, "recommendation": f"Ошибка данных для кампании {selected_arm}"}

    def get_arm_stats(self) -> Dict:
        """
        Возвращает статистику по всем рычагам.

        Returns:
            Словарь со статистикой (выборы, награда, средняя награда, CPM).
        """
        stats = {}
        for arm in self.arms:
            avg_reward = self.rewards[arm] / self.counts[arm] if self.counts[arm] > 0 else 0.0
            stats[arm] = {
                "pulls": self.counts[arm],
                "total_reward": self.rewards[arm],
                "avg_reward": avg_reward,
                "avg_cpm": self.cpms[arm]
            }
        return stats
    
