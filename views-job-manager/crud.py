"""
Job CRUD.
"""
import logging
import time
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from models import Job
from cache import cache
from jobs import touch_router
import settings

logger = logging.getLogger(__name__)

def expire_job(session:Session,job_id):
    job = session.query(Job).get(job_id)
    if job is not None and job.expired():
        logger.info("Job %s expired",job_id)
        session.delete(job)
        session.commit()

def get_job(session:Session, job_id):
    expire_job(session,job_id)
    return session.query(Job).get(job_id)

def await_job(session:Session,job_id,retries=None):
    """
    Waits for a job to either expire, or be completed (removed).
    """
    retries = 0 if not retries else retries+1

    logging.debug("Checking if job %s exists",job_id)
    job = get_job(session,job_id) 

    if job is None:
        return True
    else:
        logging.debug("%s %s seconds old",str(job),job.age())

    logging.debug("Awaiting job with id %s",job_id)
    time.sleep(settings.JOB_RETRY)

    return await_job(session,job_id,retries=retries)

def do_job(session:Session, job):
    #job = Job.parse_whole_path(path)
    job_id = job.id_hash

    # Is in progress?
    try:
        assert get_job(session,job_id) is None
        session.add(job)
        session.commit()
    except (AssertionError,IntegrityError):
        logger.info("Job %s is already running")
        return

    # Do the job
    mark = datetime.now()

    #time.sleep(1)
    #cache.set(job.path(),"yoo")
    touch_router(job.path())

    delta = datetime.now()-mark
    logger.info("Job %s completed in %s seconds",job_id,delta.seconds)

    session.delete(job)
    session.commit()

def handle_order(session:Session,path:str):
    job = Job.parse_whole_path(path)
    try:
        assert get_job(session,job.id_hash) is None
    except AssertionError:
        logger.info("Job %s in progress",job.id_hash)
        return

    for subjob in job.subjobs():
        do_job(session,subjob)

    do_job(session,job)
