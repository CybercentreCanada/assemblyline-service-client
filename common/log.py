import logging
import logging.handlers
import logging.config


from common.logformat import AL_LOG_FORMAT


def init_logging(log_level=logging.INFO):
    logging.root.setLevel(logging.CRITICAL)
    logger = logging.getLogger('assemblyline')
    logger.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setLevel(log_level)
    console.setFormatter(logging.Formatter(AL_LOG_FORMAT))
    logger.addHandler(console)
