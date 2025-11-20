import logging
from dagster import resource

@resource
def logger_resource(_context) -> logging.Logger:
    """
    Ресурс для единой настройки Python‐логгера внутри Dagster‐опов.
    Возвращает настроенный экземпляр logger.
    """

    logger = logging.getLogger("wb")
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)

    logger.setLevel(logging.INFO)
    return logger
