import math
import logging
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

class UCBBanditLogic:
    """Класс для управления логикой UCB-алгоритма в UCBBanditCPM."""

    def __init__(self, config):
        self.config = config
        self.counts = {arm: 0 for arm in self.config.arms}
        self.rewards = {arm: 0.0 for arm in self.config.arms}
        self.cpms = {arm: 0.0 for arm in self.config.arms}
        self.total_pulls = 0
        logger.info(f"Инициализирована логика UCB для advert_id {self.config.advert_id}")

    def select_arm(self, current_cpm: float, initial_cpm: Optional[float]) -> str:
        """Выбирает руку с использованием UCB-алгоритма, исключая руки, приводящие к ставкам ниже min_bid."""
        if not self.config.arms:
            raise ValueError("Нет доступных рук")
        self.total_pulls += 1
        available_arms = (
            self.config.initial_arms if self.total_pulls <= 5 else self.config.arms
        )

        # Фильтруем руки, которые приводят к CPM >= min_bid
        valid_arms = []
        for arm in available_arms:
            new_cpm = self.apply_cpm_change(arm, current_cpm, initial_cpm)
            if hasattr(self.config, 'min_bid') and self.config.min_bid is not None:
                if new_cpm >= self.config.min_bid:
                    valid_arms.append(arm)
                else:
                    logger.debug(f"Исключена рука {arm}: новый CPM {new_cpm} < min_bid {self.config.min_bid}")
            else:
                valid_arms.append(arm)

        if not valid_arms:
            logger.warning(f"Нет доступных рук, удовлетворяющих min_bid {self.config.min_bid}. Используется reset.")
            return "reset"  # Сбрасываем до начального CPM, если нет валидных рук

        # Проверяем, есть ли непротестированные руки
        for arm in valid_arms:
            if self.counts[arm] == 0:
                return arm

        # Фильтруем активные руки с учётом min_pulls_to_prune и порога награды
        active_arms = [
            arm for arm in valid_arms
            if self.counts[arm] < self.config.min_pulls_to_prune or
            (self.counts[arm] > 0 and self.rewards[arm] / self.counts[arm] > 0.1 * max(self.rewards.values()))
        ]
        if not active_arms:
            active_arms = valid_arms

        # Рассчитываем UCB-значения для активных рук
        ucb_values = {}
        current_exploration_factor = (
            self.config.exploration_factor * math.exp(-self.config.exploration_decay_rate * self.total_pulls)
            if self.config.adaptive_exploration else self.config.exploration_factor
        )
        for arm in active_arms:
            avg_reward = self.rewards[arm] / self.counts[arm]
            exploration_term = math.sqrt(current_exploration_factor * math.log(self.total_pulls) / self.counts[arm])
            ucb_values[arm] = avg_reward + exploration_term
            if self.config.daily_budget and self.cpms[arm] > self.config.daily_budget * 1.2:
                ucb_values[arm] *= 0.05
        return max(ucb_values, key=ucb_values.get)

    def apply_cpm_change(self, arm: str, current_cpm: float, initial_cpm: Optional[float]) -> float:
        """Применяет изменение CPM на основе выбранной руки."""
        if initial_cpm is None:
            logger.warning(f"initial_cpm не указан, используется current_cpm={current_cpm}")
            new_cpm = current_cpm
        else:
            if arm == "0%":
                new_cpm = current_cpm
            elif arm == "reset":
                new_cpm = float(initial_cpm)
            elif arm == "double":
                new_cpm = current_cpm * 2
            else:
                percentage = float(arm.strip("%")) / 100
                new_cpm = current_cpm * (1 + percentage)

            if self.total_pulls <= 5 and -10 <= float(arm.strip("%")) <= 10:
                new_cpm = round(new_cpm, 2)
            else:
                new_cpm = round(new_cpm / self.config.cpm_step_size) * self.config.cpm_step_size
            new_cpm = round(new_cpm, 2)

        return new_cpm

    def update(self, arm: str, reward: float, cpm: float = None):
        """Обновляет состояние бандита для указанной руки."""
        if arm not in self.config.arms:
            logger.warning(f"Рука {arm} не найдена в списке рук")
            return
        self.counts[arm] += 1
        self.rewards[arm] += reward
        if cpm is not None:
            self.cpms[arm] = (
                ((self.cpms[arm] * (self.counts[arm] - 1)) + cpm) / self.counts[arm]
                if self.counts[arm] > 1 else cpm
            )
            logger.debug(f"Обновлен CPM для руки {arm}: {self.cpms[arm]:.2f}")

    def get_arm_stats(self) -> Dict:
        """Возвращает статистику по всем рукам."""
        stats = {}
        for arm in self.config.arms:
            avg_reward = self.rewards[arm] / self.counts[arm] if self.counts[arm] > 0 else 0.0
            stats[arm] = {
                "pulls": self.counts[arm],
                "total_reward": self.rewards[arm],
                "avg_reward": avg_reward,
                "avg_cpm": self.cpms[arm]
            }
        return stats

    def update_arms(self, new_arms: Optional[List[str]] = None):
        """Обновляет список рук и синхронизирует состояния."""
        if new_arms:
            self.config.arms = new_arms
        else:
            self.config.arms = [f"{step}%" for step in range(-50, 51, 5)] + ["double", "reset"]
        self.counts = {arm: self.counts.get(arm, 0) for arm in self.config.arms}
        self.rewards = {arm: self.rewards.get(arm, 0.0) for arm in self.config.arms}
        self.cpms = {arm: self.cpms.get(arm, 0.0) for arm in self.config.arms}
        logger.info(f"Обновлены руки для advert_id {self.config.advert_id}: {self.config.arms}")