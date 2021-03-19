
import os
import logging
import time
from contextlib import closing
from fastapi import FastAPI,BackgroundTasks,Response

from tasks import Job,ParsingError
from db import Session 
from cache import cache
import settings

try:
    logging.basicConfig(level=getattr(logging,settings.LOG_LEVEL))
except AttributeError:
    pass
logger = logging.getLogger(__name__)

app = FastAPI()

def handle_job(job):
    with closing(Session()) as sess:
        logger.info("Doing %s",str(job))
        sess.add(job)
        sess.commit()
        job_id = job.id_hash

        # Touch necessary paths through the router...
        tasks = job.tasks
        tasks.reverse()

        preceding = []
        for task in tasks:
            path = os.path.join(task.path(),*preceding)
            logger.info("requesting %s/%s",settings.ROUTER_URL,path)
            time.sleep(4)
            preceding.append(task.path())

    with closing(Session()) as sess:
        job = sess.query(Job).get(job_id)
        path = job.path()
        sess.delete(job)
        sess.commit()

    logger.info("Completed job %s",str(path))

    # Happens in the router (same cache)
    cache.set(path,"yooo")

@app.get("/{job:path}")
def dispatch(job:str,background_tasks:BackgroundTasks):
    #background_tasks.add_task(do_something_later,fname,msg)
    try:
        job = Job.parse_whole_path(job)
    except ParsingError:
        return Response(status_code=404)

    try:
        result = cache.get(job.path())
    except KeyError:
        pass
    else:
        logger.info("Returning %s from cache",job.path())
        return Response(str(result))

    with closing(Session()) as sess:
        if sess.query(Job).get(job.id_hash) is not None:
            logger.info("%s is already in progress",str(job))
            return Response(f"Already doing {job}",status_code=202)

    background_tasks.add_task(handle_job,job)
    return Response(f"Working on {job}",status_code=202)
