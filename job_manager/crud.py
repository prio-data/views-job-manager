"""
Job CRUD.
"""
import logging
import time
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
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

def handle_job(
        job_lifetime: int,
        retry_time: int,
        session:Session,
        cache: caching.BlobStorageCache,
        api: remotes.Api,
        main_job: models.Job,
        ):
    """
    Handles a 'main_job' by trying to handle its dependent jobs one by one.
    Can raise JobHttpError if there is a remote problem while doing the job and
    AlreadyRequested if all of the steps are already requested (locked).
    """

    subjobs = main_job.subjobs()

    """
    First check if any subjobs are already running, and if so, wait until they
    complete, then try again.
    """
    is_locked = -1
    for idx,job in enumerate(subjobs):
        try:
            session.add(job)
            session.commit()
            logger.debug("Added job %s", job.path)
        except IntegrityError:
            logger.info("Job %s already exists", job.path)
            session.rollback()
            existing = session.query(models.Job).get(job.path)

            if existing.age() > job_lifetime:
                logger.info("Job %s is expired, retrying...", job.path)
                session.delete(existing)
                session.commit()
                return handle_job(
                        job_lifetime, retry_time, session, cache, api,
                        main_job)

            is_locked = idx
            break
        else:
            logger.info("No jobs")


    if is_locked + 1 == len(subjobs):
        logger.info("All jobs for %s already requested",main_job.path)
        raise AlreadyRequested

    if is_locked >= 0:
        to_await,next_job,*_ = subjobs[is_locked:]

        logger.info("Waiting for job %s", to_await.path)
        await_job(job_lifetime, retry_time, session,
                to_await)

        logger.info("Handling next job %s", next_job.path)
        return handle_job(job_lifetime, retry_time, session, cache, api,
                next_job)

    """
    Second, check if there are any jobs that are already cached
    """

    is_cached = -1
    done = []

    for idx, job in enumerate(subjobs):
        if cache.exists(job.path):
            is_cached = idx
            done.append(job)
        else:
            break
    logger.debug(f"{is_cached+1} jobs were already cached")
    subjobs = subjobs[is_cached + 1:]

    """
    Third, do the remaining jobs
    """

    logger.debug(f"Doing {len(subjobs)} jobs")
    error = None
    for job in subjobs:
        """
        Were there any errors running the job previously?
        """
        logger.debug("Checking previous errors for %s", job.path)
        error = session.query(models.Error).get(job.path)
        if error is not None and error.age() <= job_lifetime:
            error = None

        """
        Try running job (touching remote path).
        """
        logger.debug("Doing job %s", job.path)
        try:
            api.touch(job.path)
        except requests.HTTPError as httpe:
            logger.critical("HTTP error from %s: %s - %s",
                    job.path, httpe.response.status_code, httpe.response.content.decode())
            error = post_error(session, job, httpe.response)

        done.append(job)
        if error:
            break

    # Cleanup
    for job in done:
        logger.debug("Unlocking %s", job.path)
        session.delete(job)
        session.commit()

    if error:
        raise JobHttpError(response = error)

    return True

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
