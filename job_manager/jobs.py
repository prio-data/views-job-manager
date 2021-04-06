
import os
import logging
from datetime import datetime

import requests

import settings
from cache import cache

logger = logging.getLogger(__name__)

def touch_router(path):
    logger.info("Fetching data from router / %s",path)
    mark = datetime.now()
    response = requests.get(os.path.join(settings.ROUTER_URL,path))
    if response.status_code == 200:
        logger.info("Retrieved data from router / %s after %s seconds",
                path, (datetime.now()-mark).seconds)
        cache.set(path,response.content)
    else:
        logger.warning("router / %s returned %s",path,str(response.status_code))

