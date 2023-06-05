import logging

from src.settings import Config


def get_module_logger(mod_name):
    """
    To use this, do logging = get_module_logger(__name__)
    """
    logger = logging.getLogger(mod_name)
    handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "[%(asctime)s]-------[%(levelname)s]------- %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    file_handler = logging.FileHandler(Config.LOG_FILE_NAME)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.setLevel(Config.LOG_LEVEL)
    return logger
