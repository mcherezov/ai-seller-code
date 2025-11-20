import requests
import logging

class NotificationManager:
    """
    Управляет отправкой уведомлений в Telegram.

    Attributes:
        bot_token (str): Токен бота Telegram.
        chat_id (str): Идентификатор чата Telegram.
        logger (logging.Logger): Логгер для записи событий и ошибок.
        session_state (SessionState): Объект состояния сессии для логирования сообщений.
    """
    def __init__(self, bot_token: str, chat_id: str, session_state, logger: logging.Logger):
        """
        Инициализирует менеджер уведомлений с заданными параметрами.

        Args:
            bot_token (str): Токен бота Telegram.
            chat_id (str): Идентификатор чата Telegram.
            session_state (SessionState): Объект состояния сессии для логирования.
            logger (logging.Logger): Логгер для записи событий.
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.logger = logger
        self.session_state = session_state

    def send_message(self, message: str):
        """
        Отправляет сообщение в Telegram.

        Args:
            message (str): Текст сообщения для отправки.

        Returns:
            bool: True, если сообщение отправлено успешно, иначе False.
        """
        if not self.bot_token or not self.chat_id:
            error_msg = f"Не удалось отправить уведомление: bot_token={'*' * len(self.bot_token) if self.bot_token else None}, chat_id={self.chat_id}"
            self.logger.warning(error_msg)
            self.session_state.add_log_message(error_msg)
            return False

        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            }
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                self.logger.info("Уведомление успешно отправлено в Telegram")
                self.session_state.add_log_message("Уведомление успешно отправлено в Telegram")
                return True
            else:
                error_msg = f"Ошибка отправки в Telegram: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                self.session_state.add_log_message(error_msg)
                return False
        except Exception as e:
            error_msg = f"Ошибка при отправке уведомления в Telegram: {str(e)}"
            self.logger.error(error_msg)
            self.session_state.add_log_message(error_msg)
            return False