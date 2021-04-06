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
from settings import config

logger = logging.getLogger(__name__)

def expire_job(session:Session,job_id):
    session.expire_all() # Reflect remote
    job = session.query(Job).get(job_id)
    if job is not None and job.expired():
        logger.info("Job %s expired",job_id)
        session.delete(job)
        session.commit()

def get_job(session:Session, job_id):
    expire_job(session,job_id)
    return session.query(Job).get(job_id)

def await_job(session:Session,job,retries=None):
    """
    Waits for a job to either expire, or be completed (removed).
    """
    job_id = job.id_hash
    retries = 0 if not retries else retries+1

    logging.debug("Checking if job %s exists",job_id)
    job = get_job(session,job_id) 

    if job is None:
        return True
    else:
        logging.debug("%s %s seconds old",str(job),job.age())

    logging.debug("Awaiting job with id %s",job_id)
    time.sleep(config("JOB_RETRY"))

    return await_job(session,job,retries=retries)

def do_job(job):
    # Do the job
    mark = datetime.now()
    touch_router(job.path())
    delta = datetime.now()-mark
    logger.info("Job %s completed in %s seconds",
            job.id_hash,
            delta.seconds)

class JobIsLocked(Exception):
    pass

def create_job_lock(session,job):
    if get_job(session,job.id_hash) is not None:
        raise JobIsLocked
    try:
        session.add(job)
        session.commit()
    except IntegrityError as ie:
        logger.info("Job %s was locked",job.id_hash)
        session.rollback()
        raise JobIsLocked from ie
    else:
        logger.debug("Locked %s",job.id_hash)

def remove_job_lock(session,job):
    try:
        session.delete(job)
        session.commit()
    except:
        logging.critical("Something went wrong!!! %s",str(job))
        session.rollback()
        pass
    else:
        logging.debug("Removed lock for %s",str(job))

def handle_order(session:Session,path:str):
    job = Job.parse_whole_path(path)

    subjobs = job.subjobs()
    all_jobs = subjobs+[job]
    logger.debug("The raw job list is %s",str([job.path() for job in all_jobs]))

    is_locked = -1 
    all_jobs.reverse()
    for idx,job in enumerate(all_jobs):
        try:
            create_job_lock(session,job)
        except JobIsLocked:
            logger.debug("Job %s was locked",str(job))
            is_locked = idx
            break

    if is_locked == 0:
        logger.info("%s already requested",str(job))
        return
    if is_locked > 0:
        all_jobs = all_jobs[:is_locked+1]
        *_,dependent_on = all_jobs
        await_job(session,dependent_on)
        if not dependent_on.is_cached(cache):
            do_job(dependent_on)

    all_jobs.reverse()
    for job in all_jobs:
        try:
            logger.info("Doing job %s",job.id_hash)
            do_job(job)
        finally:
            remove_job_lock(session,job)
