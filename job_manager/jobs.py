
import os
import logging
from datetime import datetime

import requests

from cache import cache
from settings import config

logger = logging.getLogger(__name__)

def touch_router(path):
    logger.info("Fetching data from router / %s",path)
    mark = datetime.now()
    response = requests.get(os.path.join(config("ROUTER_URL"),path))
    if response.status_code == 200:
        logger.info("Retrieved data from router / %s after %s seconds",
                path, (datetime.now()-mark).seconds)
    else:
        logger.warning("router / %s returned %s",path,str(response.status_code))

