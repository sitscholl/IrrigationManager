import yaml

import logging

logger = logging.getLogger(__name__)


def load_config(config_file: str):

    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        raise
