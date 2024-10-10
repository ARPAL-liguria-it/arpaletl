import logging


def get_logger(name: str = __name__) -> logging.Logger:
    """
    Get a logger with the specified name
    @param name: Name of the logger
    @returns: Logger object
    """
    logger = logging.getLogger(f'arpaletl.{name}')
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger
