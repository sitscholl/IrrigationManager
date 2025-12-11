#Example for base app classes structure: https://github.com/kthorp/pyfao56/blob/main/tests/test01/cottondry2013.py
import logging
import logging.config
from datetime import datetime

from src.config import load_config
from src.database.db import IrrigDB
from src.workflow import WaterBalanceWorkflow


logger = logging.getLogger(__name__)

if __name__ == "__main__":
    
    config = load_config('config/config.yaml')
    logging.config.dictConfig(config['logging'])
    db = IrrigDB(**config.get('database', {}))

    logger.info("#"*50)
    logger.info('Starting water balance calculation')
    logger.info("#"*50)

    workflow = WaterBalanceWorkflow(config, db)
    
    try:
        workflow.run()
    except Exception as e:
        logger.error(f"Error running water balance workflow: {e}", exc_info = True)

    logger.info("#"*50)
    logger.info('Water balance calculation finished')
    logger.info("#"*50)
