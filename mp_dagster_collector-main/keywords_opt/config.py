from pathlib import Path
import os
from dotenv import dotenv_values
import logging

class ConfigManager:
    """
    Управляет загрузкой конфигурации базы данных, Telegram и Wildberries API из файла .env.

    Attributes:
        logger (logging.Logger): Логгер для записи событий и ошибок.
        config (dict): Словарь с настройками конфигурации.
        error_message (str or None): Сообщение об ошибке, если загрузка не удалась.
    """
    def __init__(self, logger: logging.Logger):
        """
        Инициализирует ConfigManager с переданным логгером.

        Args:
            logger (logging.Logger): Логгер для записи событий.
        """
        self.logger = logger
        self.config = {}
        self.error_message = None
        self._load_config()

    def _load_config(self):
        """
        Загружает конфигурацию из файла .env и проверяет наличие необходимых параметров.
        """
        possible_env_paths = [
            Path(os.getenv("AKO_CONFIG_PATH", "")) / '.env' if os.getenv("AKO_CONFIG_PATH") else None,
            Path(__file__).parent / 'config' / '.env',
            Path.home() / 'Documents' / 'ako' / 'config' / '.env',
            Path(__file__).parent.parent / 'config' / '.env',
        ]

        valid_paths = [p for p in possible_env_paths if p is not None]
        self.logger.debug(f"Проверяемые пути для файла .env: {', '.join(str(p) for p in valid_paths)}")

        env_path = None
        for path in valid_paths:
            if path.exists():
                env_path = path
                self.logger.debug(f"Файл .env найден по пути: {path}")
                break
            else:
                self.logger.debug(f"Файл .env не найден по пути: {path}")

        if not env_path:
            self.error_message = f"Файл .env не найден в путях: {', '.join(str(p) for p in valid_paths)}"
            self.logger.error(self.error_message)
            return

        env_config = dotenv_values(env_path)
        self.logger.info(f"Файл .env успешно загружен из {env_path}")

        config_dir = env_path.parent
        cert_path = config_dir / 'CA.pem'
        self.logger.debug(f"Проверка наличия SSL-сертификата по пути: {cert_path}")
        if not cert_path.exists():
            self.logger.warning(f"SSL-сертификат не найден по пути {cert_path}. Продолжаем без SSL.")
            self.error_message = f"SSL-сертификат не найден по пути {cert_path}"
        else:
            self.logger.debug(f"SSL-сертификат найден по пути: {cert_path}")

        self.config = {
            'host': env_config.get('DEST_DB_HOST'),
            'port': env_config.get('DEST_DB_PORT'),
            'name': env_config.get('DEST_DB_NAME'),
            'user': env_config.get('DEST_DB_USER'),
            'password': env_config.get('DEST_DB_PASSWORD'),
            'sslmode': env_config.get('DEST_DB_SSLMODE'),
            'sslrootcert': str(cert_path) if cert_path.exists() else None,
            'telegram_bot_token': env_config.get('TELEGRAM_BOT_TOKEN'),
            'telegram_chat_id': env_config.get('TELEGRAM_CHAT_ID'),
            'WB_API_TOKEN': env_config.get('WB_API_TOKEN_YULIA') # @TODO: убрать костыль
        }

        required_params = [
            'host', 'port', 'name', 'user', 'password',
            'telegram_bot_token', 'telegram_chat_id', 'WB_API_TOKEN'
        ]
        missing_params = [key for key in required_params if not self.config.get(key)]
        if missing_params:
            self.error_message = f"Отсутствуют обязательные параметры в .env: {', '.join(missing_params)}"
            self.logger.error(self.error_message)
            self.config = {}
        else:
            self.logger.debug(
                f"Конфигурация загружена: { {k: v[:10] + '...' if k in ['password', 'WB_API_TOKEN', 'telegram_bot_token'] else v for k, v in self.config.items()} }"
            )

    def get_config(self):
        """
        Возвращает конфигурацию и сообщение об ошибке.

        Returns:
            tuple: Словарь с конфигурацией и сообщение об ошибке (None, если ошибок нет).
        """
        return self.config, self.error_message