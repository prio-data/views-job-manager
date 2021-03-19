import os
import logging
import time
from contextlib import closing
from fastapi import FastAPI,BackgroundTasks,Response

from tasks import Job,ParsingError
from db import Session 
from cache import cache
import settings

from sqlalchemy.exc import IntegrityError

try:
    logging.basicConfig(level=getattr(logging,settings.LOG_LEVEL))
except AttributeError:
    pass
logger = logging.getLogger(__name__)

app = FastAPI()

# Should perhaps generalize the case of "check if job exists",
# delete it if stale. If required, poll until it disappears.

def retrieve_job(session,job_id):
    job = session.query(Job).get(job_id)
    if job is None:
        return job
    elif job.expired():
        session.delete(job)
        session.commit()
        logging.debug("Job %s expired",job_id)
        return None
    else:
        return job

def await_job(job_id,retries=None):
    retries = 0 if not retries else retries+1
    logging.debug("Checking if job %s exists",job_id)
    with closing(Session()) as sess:
        job = retrieve_job(sess,job_id) 
        if job is None:
            return True
        else:
            logging.debug("%s %s seconds old",str(job),job.age())

    logging.debug("Awaiting job with id %s",job_id)
    time.sleep(settings.JOB_RETRY)
    return await_job(job_id,retries=retries)

def complete_job(job):
    # Exists?
    await_job(job.id_hash)

    # Do job - touch router
    with closing(Session()) as sess:
        sess.add(job)
        sess.commit()

        url = os.path.join(settings.ROUTER_URL,job.path()+"?touch")
        logger.info("%s requesting %s",str(job),url) 

        try:
            cache.get(job.path())
        except KeyError:
            time.sleep(4)
            cache.set(job.path(),"yooo")

        logger.info("%s complete",str(job))

        sess.delete(job)
        sess.commit()

def handle_order(job):
    subjobs = job.subjobs()
    for subjob in subjobs:
        complete_job(subjob)
    complete_job(job)

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
        logger.info("Returning %s from cache",str(job))
        return Response(str(result))

    with closing(Session()) as sess:
        if retrieve_job(sess,job.id_hash) is not None:
            logger.info("%s is already in progress",str(job))
            return Response(f"Already doing {job}",status_code=202)

    background_tasks.add_task(handle_order,job)
    return Response(f"Working on {job}",status_code=202)
