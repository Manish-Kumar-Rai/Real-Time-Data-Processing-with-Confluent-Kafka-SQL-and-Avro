import logging

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # If logger already has handlers, avoid adding duplicates
    if logger.hasHandlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(name)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger