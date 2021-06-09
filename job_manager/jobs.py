
import os
import logging
from datetime import datetime

from sqlalchemy.orm import Session

import requests
from . import models, caching

logger = logging.getLogger(__name__)

def touch(session: Session, base_url: str, job: models.Job):
    """
    Touch the path associated with a job at the base_url.
    """

    path = job.path()
    logger.info("Fetching data from router / %s", path)
    mark = datetime.now()
    response = requests.get(os.path.join(base_url, path)+"?touch=true")
    if response.status_code == 200:
        logger.info("Retrieved data from router / %s after %s seconds",
                path, (datetime.now()-mark).seconds)
    else:
        session.add(models.Error(content = response.content, status_code = response.status_code))
        session.commit()
        logger.warning("router / %s returned %s",path,str(response.status_code))

def check(cache: caching.BlobStorageCache, job: models.Job):
    """
    Checks to see if the job has been completed already
    """
    return cache.exists(job.path())
