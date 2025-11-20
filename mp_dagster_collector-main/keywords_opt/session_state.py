from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import pandas as pd

class SessionState:
    """
    Управляет состоянием приложения, включая параметры оптимизации и логи.

    Attributes:
        product_id (str): Идентификатор продукта, загружаемый из базы данных.
        start_date (datetime.date): Начальная дата периода.
        end_date (datetime.date): Конечная дата периода.
        cost_price (float): Себестоимость продукта, загружаемая из базы данных.
        margin_rate (float): Ставка маржи.
        commission_rate (float): Ставка комиссии, загружаемая из базы данных.
        log_messages (list): Список сообщений логов.
        recommendations (pd.DataFrame): DataFrame с рекомендациями.
        max_cpc (float): Максимальная стоимость за клик.
    """
    def __init__(self, db_manager, product_id: str = None, start_date: datetime.date = None,
                 end_date: datetime.date = None, margin_rate: float = 0.001):
        """
        Инициализирует состояние сессии с заданными параметрами.

        Args:
            db_manager (DatabaseManager): Менеджер базы данных для загрузки product_id.
            product_id (str, optional): Идентификатор продукта. Если None, загружается из базы данных.
            start_date (datetime.date, optional): Начальная дата периода. По умолчанию 21 день назад.
            end_date (datetime.date, optional): Конечная дата периода. По умолчанию вчера.
            margin_rate (float): Ставка маржи.

        Raises:
            ValueError: Если product_id не указан и не удалось загрузить из базы данных.
        """
        self.product_id = None
        self.start_date = start_date or (datetime.now(ZoneInfo('Europe/Moscow')).date() - timedelta(days=21))
        self.end_date = end_date or (datetime.now(ZoneInfo('Europe/Moscow')).date() - timedelta(days=1))
        self.cost_price = None
        self.margin_rate = margin_rate
        self.commission_rate = None
        self.log_messages = []
        self.recommendations = pd.DataFrame()
        self.max_cpc = 0

        if product_id is None:
            self.set_product_id(db_manager)
        else:
            self.product_id = product_id

    def set_product_id(self, db_manager):
        """
        Устанавливает product_id, загружая его из базы данных через db_manager.
        Ищет product_id, где model = 'keyword_optimizator' и текущая дата в диапазоне valid_from и valid_to.

        Args:
            db_manager (DatabaseManager): Менеджер базы данных для получения product_id.

        Raises:
            ValueError: Если product_id не удалось загрузить из базы данных.
        """
        try:
            self.add_log_message("Начало извлечения product_id из базы данных")
            self.product_id = db_manager.get_active_product_id()
            self.add_log_message(f"Успешно извлечен product_id: {self.product_id}")
        except Exception as e:
            error_msg = f"Ошибка при извлечении product_id: {str(e)}"
            self.add_log_message(error_msg)
            raise ValueError(error_msg)

    def set_commission_rate(self, db_manager, product_id: str = None, date: str = None):
        """
        Устанавливает commission_rate, загружая его из базы данных.

        Args:
            db_manager (DatabaseManager): Менеджер базы данных для получения commission_rate.
            product_id (str, optional): Идентификатор продукта. Если None, используется self.product_id.
            date (str, optional): Дата в формате ISO. Если None, используется self.end_date.
        """
        product_id = product_id or self.product_id
        date = date or self.end_date.isoformat()
        self.commission_rate = db_manager.get_commission_rate(product_id=product_id, date=date)

    def set_cost_price(self, db_manager, product_id: str = None, date: str = None):
        """
        Устанавливает cost_price, загружая его из базы данных.

        Args:
            db_manager (DatabaseManager): Менеджер базы данных для получения cost_price.
            product_id (str, optional): Идентификатор продукта. Если None, используется self.product_id.
            date (str, optional): Дата в формате ISO. Если None, используется self.end_date.
        """
        product_id = product_id or self.product_id
        date = date or self.end_date.isoformat()
        self.cost_price = db_manager.get_cost_price(product_id=product_id, date=date)

    def get_parameters(self):
        """
        Возвращает параметры оптимизации в виде словаря.

        Returns:
            dict: Словарь с параметрами оптимизации.

        Raises:
            ValueError: Если cost_price, commission_rate или product_id не были установлены (остались None).
        """
        if self.cost_price is None:
            error_msg = "Cost_price не установлен. Необходимо вызвать set_cost_price для загрузки из базы данных."
            self.add_log_message(error_msg)
            raise ValueError(error_msg)

        if self.commission_rate is None:
            error_msg = "Commission_rate не установлен. Необходимо вызвать set_commission_rate для загрузки из базы данных."
            self.add_log_message(error_msg)
            raise ValueError(error_msg)

        if self.product_id is None:
            error_msg = "Product_id не установлен. Необходимо вызвать set_product_id для загрузки из базы данных."
            self.add_log_message(error_msg)
            raise ValueError(error_msg)

        return {
            'product_id': self.product_id,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'cost_price': self.cost_price,
            'margin_rate': self.margin_rate,
            'commission_rate': self.commission_rate
        }

    def add_log_message(self, message: str):
        """
        Добавляет сообщение в список логов.

        Args:
            message (str): Сообщение для добавления.
        """
        self.log_messages.append(message)

    def set_recommendations(self, recommendations: pd.DataFrame, max_cpc: float):
        """
        Устанавливает рекомендации и максимальную стоимость за клик.

        Args:
            recommendations (pd.DataFrame): DataFrame с рекомендациями.
            max_cpc (float): Максимальная стоимость за клик.
        """
        self.recommendations = recommendations
        self.max_cpc = max_cpc