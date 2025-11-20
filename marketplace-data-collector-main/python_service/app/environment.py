import logging
import os


def setup_logger():
    log_level = os.getenv("LOG_LEVEL").upper()
    logging.basicConfig(level=getattr(logging, log_level), format="%(asctime)s - %(levelname)s - %(message)s")
