import logging

def log_config():
# Logging configurations are handled in this function.

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    #Log file config
    formatter = logging.Formatter('%(levelname)s    -   %(asctime)s -   %(process)s -   %(name)s    -   %(message)s')
    file_handler = logging.FileHandler('pipeline_log.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger