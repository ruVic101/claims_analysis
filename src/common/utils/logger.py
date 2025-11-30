import logging


def create_logger(name, log_level=logging.INFO):
    logging.basicConfig(level=log_level, format="%(asctime)s : %(levelname)s [%(name)s : %(lineno)d]-  - %(message)s")
    logger = logging.getLogger(name)
    return logger
