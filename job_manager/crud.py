"""
Job CRUD.
"""
from typing import Callable 
import asyncio
from collections import deque
import warnings
import logging
import time
from typing import List,Optional,Tuple
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SAWarning, InvalidRequestError
from sqlalchemy.orm.exc import ObjectDeletedError
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

def await_job(job_lifetime: int, retry_time: int, sessionmaker: Callable[[],Session], path: str, retries = 0):
    """
    Waits for a job to either expire, or be completed (removed).
    """

    retries = retries + 1

    logger.debug("Checking if job for %s exists", path)
    with sessionmaker() as session:
        job = get_job(job_lifetime, session, path)

    if job is None:
        return True

    logger.debug("Awaiting job with id %s (%s seconds old): %s retries",
            job.path, job.age(), retries)

    time.sleep(retry_time)
    return await_job(job_lifetime, retry_time, sessionmaker, path, retries=retries)

def lock_jobs(job_lifetime: int, session: Session,
        subjobs: List[models.Job])-> Tuple[List[models.Job], Optional[models.Job]]:
    """
    Lock jobs
    """
    locked = deque()
    pending_job = None
    for job in reversed(subjobs):
        try:
            # The whole point is that this is supposed to break
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=SAWarning)
                session.add(job)
                session.commit()

            # The DB will only every permit one add, as such
            # using the IntegrityError is the safest semaphore
            # to use when you only want the one Job.

        except IntegrityError:
            logger.info("Job %s already exists", job)
            session.rollback()
            existing = session.query(models.Job).get(job.path)

            if existing.age() > job_lifetime:
                logger.info("Job %s is expired", job)
                session.delete(existing)
                session.commit()
            else:
                pending_job = existing
                break
        locked.appendleft(job)

    return list(locked), pending_job

def remove_cached_jobs(session: Session,cache,
        jobs: List[models.Job])-> List[models.Job]:

    cached = -1
    for idx,job in enumerate(jobs):
        if cache.exists(job.path):
            logger.debug("Job %s is already cached",job.path)
            session.delete(job)
            cached = idx
    session.commit()

    if cached > -1:
        logger.debug("Job no %s was cached", cached)

    return jobs[cached+1:]

def do_jobs(
        cache: caching.RESTCache,
        job_lifetime: int,
        session: Session,
        api: remotes.Api,
        jobs: List[models.Job])-> int:

    for job in jobs:
        logger.debug("Checking previous errors for %s", job.path)
        error = session.query(models.Error).get(job.path)
        if error is not None and error.age() <= job_lifetime:
            error = None

        logger.debug("Doing job %s", job.path)
        try:
            api.touch(job.path, cache)
        except requests.HTTPError as httpe:
            logger.critical("HTTP error from %s: %s - %s",
                    job.path, httpe.response.status_code, httpe.response.content.decode())
            error = post_error(session, job, httpe.response)
            break

def handle_job(
        job_lifetime: int,
        retry_time: int,
        sessionmaker: Callable[[], Session],
        cache: caching.RESTCache,
        api: remotes.Api,
        main_job: models.Job,
        )-> int:
    """
    Handles a 'main_job' by trying to handle its dependent jobs one by one.
    Can raise JobHttpError if there is a remote problem while doing the job and
    AlreadyRequested if all of the steps are already requested (locked).
    """
    with sessionmaker() as session:
        subjobs = main_job.subjobs()

        locked_jobs, pending = lock_jobs(job_lifetime, session, subjobs)

        logger.debug("Locked %s jobs", len(locked_jobs))
        wait_for = pending.path if pending is not None else None

        job_paths = [job.path for job in locked_jobs]

    if wait_for is not None:
        if wait_for == main_job.path:
            return
        else:
            await_job(job_lifetime, retry_time, sessionmaker, wait_for)

    with sessionmaker() as session:
        locked_jobs = [job for job in [session.query(models.Job).get(p) for p in job_paths] if job is not None]
        jobs_todo = remove_cached_jobs(session, cache, locked_jobs)

        if len(jobs_todo) > 0:
            logger.debug(f"Doing {len(jobs_todo)} jobs")

            try:
                do_jobs(cache, job_lifetime, session, api, jobs_todo)
            finally:
                for job in jobs_todo:
                    logger.debug("Unlocking %s", job.path)
                    try:
                        session.delete(job)
                        session.commit()
                    except (ObjectDeletedError, InvalidRequestError):
                        logger.warning(f"Tried to delete job {job}, but it was not persisted")

def post_error(session: Session, job: models.Job, response: requests.Response):
    """
    Posts an error semaphore, which prevents subsequent tries for a job
    until it expires
    """

    error = models.Error(
            path = job.path,
            status_code = response.status_code,
            content = response.content.decode(),
        )

    session.add(error)
    session.commit()
    return error
