import logging
import os
from logging.handlers import RotatingFileHandler

# Reusable logger factory
def make_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(name)s|%(asctime)s: %(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger

# Global logging setup
def setup_logging(log_dir=None, log_level=logging.INFO):
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return  # Already initialized

    formatter = logging.Formatter('%(name)s|%(asctime)s: %(levelname)s: %(message)s')

    # Console output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Optional file logging
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        logfile = os.path.join(log_dir, "daq.log")
        file_handler = RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    root_logger.setLevel(log_level)
