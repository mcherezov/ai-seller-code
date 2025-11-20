from api_clients.api_utils import send_get_request, send_post_request
from config_loader import load_all_configs


CONFIG = load_all_configs()


class WildberriesAPI:
    COMMON_API_URL = "https://common-api.wildberries.ru/api/v1"
    ANALYTICS_API_URL = "https://seller-analytics-api.wildberries.ru/api/v1"
    STATISTICS_API_URL = "https://statistics-api.wildberries.ru/api/v1"
    ADVERT_API_URL = "https://advert-api.wildberries.ru"


    def __init__(self, seller_legal: str):
        self.seller_legal = seller_legal

        # получаем все сконфигуренные «селлеры»
        all_cfg = load_all_configs()
        api_key = None
        for seller_cfg in all_cfg.values():
            keys = seller_cfg.get("wb_marketplace_keys", {})
            if seller_legal in keys:
                api_key = keys[seller_legal]
                break

        if not api_key:
            raise Exception(f"API ключ для юрлица '{seller_legal}' не найден ни в одном из конфигов.")

        self.headers = {"Authorization": api_key}


    def get_commission(self) -> dict:
        url = f"{self.COMMON_API_URL}/tariffs/commission"
        return send_get_request(url, headers=self.headers, seller=self.seller_legal)


    # Методы платного хранения
    def get_paid_storage_report(self, date_from: str, date_to: str):
        url = f"{self.ANALYTICS_API_URL}/paid_storage"
        params = {"dateFrom": date_from, "dateTo": date_to}
        return send_get_request(url, headers=self.headers, params=params, seller=self.seller_legal)


    def get_paid_storage_status(self, task_id: str):
        url = f"{self.ANALYTICS_API_URL}/paid_storage/tasks/{task_id}/status"
        return send_get_request(url, headers=self.headers, seller=self.seller_legal)


    def get_paid_storage_data(self, task_id: str):
        url = f"{self.ANALYTICS_API_URL}/paid_storage/tasks/{task_id}/download"
        return send_get_request(url, headers=self.headers, seller=self.seller_legal)


    # Методы платной приёмки
    def get_paid_acceptance_report(self, date_from: str, date_to: str):
        url = f"{self.ANALYTICS_API_URL}/acceptance_report"
        params = {"dateFrom": date_from, "dateTo": date_to}
        return send_get_request(url, headers=self.headers, params=params, seller=self.seller_legal)


    def get_paid_acceptance_status(self, task_id: str):
        url = f"{self.ANALYTICS_API_URL}/acceptance_report/tasks/{task_id}/status"
        return send_get_request(url, headers=self.headers, seller=self.seller_legal)


    def get_paid_acceptance_data(self, task_id: str):
        url = f"{self.ANALYTICS_API_URL}/acceptance_report/tasks/{task_id}/download"
        return send_get_request(url, headers=self.headers, seller=self.seller_legal)


    def get_keyword_stats(self, advert_id: int, date_from: str, date_to: str) -> dict:
        url = f"{self.ADVERT_API_URL}/adv/v0/stats/keywords"
        params = {"advert_id": advert_id, "from": date_from, "to": date_to}
        return send_get_request(url, headers=self.headers, params=params, seller=self.seller_legal)


    def get_clusters_by_ad_id(self, ad_id: int) -> dict:
        """
        Получить информацию по кластерам для рекламной кампании
        """
        url = f"{self.ADVERT_API_URL}/adv/v2/auto/stat-words"
        params = {"id": ad_id}
        return send_get_request(url, headers=self.headers, params=params, seller=self.seller_legal)

    def get_keyword_stats_by_date(self, advert_id: int, date_from: str, date_to: str) -> dict:
        """
        Получить статистику по ключевым словам
        """
        url = f"{self.ADVERT_API_URL}/adv/v0/stats/keywords"
        params = {
            "advert_id": advert_id,  # <-- изменено с "id"
            "from": date_from,
            "to": date_to
        }
        return send_get_request(url, headers=self.headers, params=params, seller=self.seller_legal)

    def get_custom(self, endpoint: str, params: dict | None = None) -> dict:
        """Универсальный GET для любых будущих ручек"""
        url = f"{self.COMMON_API_URL}/{endpoint.lstrip('/')}"
        return send_get_request(url, headers=self.headers, params=params, seller=self.seller_legal)


    def post_custom(self, endpoint: str, payload: dict | None = None) -> dict:
        """Универсальный POST для любых будущих ручек"""
        url = f"{self.COMMON_API_URL}/{endpoint.lstrip('/')}"
        return send_post_request(url, headers=self.headers, json=payload, seller=self.seller_legal)
