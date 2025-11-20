from pathlib import Path
import logging

class LoggerManager:
    """
    Управляет настройкой логирования в консоль и файл. Реализует паттерн Singleton для предотвращения дублирования логов.

    Attributes:
        logger (logging.Logger): Настроенный логгер для записи событий и ошибок.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        Реализует паттерн Singleton, чтобы гарантировать создание только одного экземпляра LoggerManager.
        """
        if cls._instance is None:
            cls._instance = super(LoggerManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, log_file: str = 'optimization.log', log_level: int = logging.INFO):
        """
        Инициализирует логгер с выводом в консоль и файл.

        Args:
            log_file (str): Имя файла для записи логов. По умолчанию 'optimization.log'.
            log_level (int): Уровень логирования (по умолчанию logging.INFO).
        """
        if not hasattr(self, 'logger'):
            self.logger = logging.getLogger('app_logger')
            self._setup_logging(log_file, log_level)

    def _setup_logging(self, log_file: str, log_level: int):
        """
        Настраивает логирование с заданным форматом и обработчиками.

        Args:
            log_file (str): Имя файла для записи логов.
            log_level (int): Уровень логирования.
        """
        self.logger.handlers.clear()

        self.logger.setLevel(log_level)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)

        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(log_dir / log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def get_logger(self):
        """
        Возвращает настроенный логгер.

        Returns:
            logging.Logger: Настроенный логгер.
        """
        return self.logger