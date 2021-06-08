"""
Job CRUD.
"""
import warnings
import logging
import time
from typing import List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SAWarning
import requests

from . import models, caching, remotes
logger = logging.getLogger(__name__)

class AlreadyRequested(Exception):
    pass

class JobHttpError(requests.HTTPError):
    """
    Raised when there is a remote HTTP error. These can't be proxied directly, since jobs are
    handled without hanging requests.
    """

class Retry(Exception):
    """
    Raised while handling jobs, prompting a retry of the control flow
    """
    def __init__(self,from_job):
        super().__init__()
        self.from_job = from_job

def get_job(job_lifetime: int, session: Session, job_path: str):
    session.expire_all()
    job = session.query(models.Job).get(job_path)
    if job is not None and job.age() > job_lifetime:
        logger.info("Job for %s expired", job_path)
        session.delete(job)
        session.commit()
        return None
    return job

def await_job(job_lifetime: int, retry_time: int, session:Session, job, retries = 0):
    """
    Waits for a job to either expire, or be completed (removed).
    """

    retries = retries + 1

    logging.debug("Checking if job %s exists", job.path)
    job = get_job(job_lifetime, session, job.path)

    if job is None:
        return True

    logging.debug("Awaiting job with id %s (%s seconds old): %s retries",
            job.path, job.age(), retries)

    time.sleep(retry_time)

    return await_job(job_lifetime, retry_time, session, job, retries=retries)

def lock_jobs(job_lifetime, session, subjobs)-> int:
    """
    Lock jobs
    """
    locked = []
    for job in subjobs:
        try:
            # The whole point is that this is supposed to break
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=SAWarning)
                session.add(job)
                session.commit()

            # The DB will only every permit one add, as such  
            # using the IntegrityError is the safest semaphore
            # to use when you only want the one Job.

            locked.append(job)
            logger.debug("Locked job %s", job.path)
        except IntegrityError:
            logger.info("Job %s already exists", job.path)
            session.rollback()
            existing = session.query(models.Job).get(job.path)

            if existing.age() > job_lifetime:
                logger.info("Job %s is expired, retrying...", job.path)
                session.delete(existing)
                session.commit()
                raise Retry(from_job = subjobs[-1])

            break

    return locked

def remove_cached_jobs(session: Session,cache: caching.BlobStorageCache,
        jobs: List[models.Job])-> List[models.Job]:
    cached = -1
    for idx,job in enumerate(jobs):
        if cache.exists(job.path):
            logger.debug("Job %s is already cached",job.path)
            session.delete(job)
            cached = idx
    session.commit()
    logging.debug("Job no %s was cached", cached)

    return jobs[cached+1:]

def do_jobs(job_lifetime: int, session: Session, api: remotes.Api,
        jobs: List[models.Job])-> None:

    for job in jobs:
        logger.debug("Checking previous errors for %s", job.path)
        error = session.query(models.Error).get(job.path)
        if error is not None and error.age() <= job_lifetime:
            error = None

        logger.debug("Doing job %s", job.path)
        try:
            api.touch(job.path)
        except requests.HTTPError as httpe:
            logger.critical("HTTP error from %s: %s - %s",
                    job.path, httpe.response.status_code, httpe.response.content.decode())
            error = post_error(session, job, httpe.response)
            raise JobHttpError(response = error)

def handle_job(
        job_lifetime: int,
        retry_time: int,
        session:Session,
        cache: caching.BlobStorageCache,
        api: remotes.Api,
        main_job: models.Job,
        )-> None:
    """
    Handles a 'main_job' by trying to handle its dependent jobs one by one.
    Can raise JobHttpError if there is a remote problem while doing the job and
    AlreadyRequested if all of the steps are already requested (locked).
    """

    subjobs = main_job.subjobs()

    locked_jobs = []
    try:
        locked_jobs = lock_jobs(job_lifetime, session, subjobs)
        logging.debug("Locked %s jobs", len(locked_jobs))

        if len(locked_jobs) == len(subjobs) - 1:
            """
            The last job is currently locked by someone else, main job is pending
            """
            raise AlreadyRequested

        if len(locked_jobs) < len(subjobs):
            """
            Need to wait for something else to complete.
            """
            await_job(job_lifetime, retry_time, session, subjobs[len(locked_jobs)])
            raise Retry(subjobs[len(locked_jobs) + 1])

    except Retry as retry:
        """
        Retry the control flow from the raised job
        """
        return handle_job(job_lifetime, retry_time, session, cache, api,
                retry.from_job)

    not_cached = remove_cached_jobs(session, cache, locked_jobs)
    logger.debug(f"Doing {len(not_cached)} jobs")
    try:
        do_jobs(job_lifetime, session, api, not_cached)
    finally:
        for job in not_cached:
            # Delete the rest of the jobs
            logger.debug("Unlocking %s", job.path)
            session.delete(job)
            session.commit()

    return None

def post_error(session: Session, job: models.Job, response: requests.Response):
    """
    Posts an error semaphore, which prevents subsequent tries for a job
    until it expires
    """

    error = models.Error(
            path = job.path,
            status_code = response.status_code,
            content = response.content,
        )

    session.add(error)
    session.commit()
    return error
