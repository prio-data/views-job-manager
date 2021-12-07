import asyncio
from typing import Deque, Tuple, Optional, List
from collections import deque
import logging
from . import remotes, caching, redis_locks

logger = logging.getLogger(__name__)

class JobHandler():
    """
    JobHandler
    ==========

    parameters:
        api_client (views_job_manager.remotes.Api)
        cache_client (views_job_manager.caching.RESTCache)
        locks_client (views_job_manager.redis_locks.RedisLocks)

        retry_cooldown (int):     How long to wait between each retry
        max_retries (int):        How many times to retry a dependent job before failing
        check_errors_every (int): How often to check for errors when retrying

    A class that handles the execution of chains of jobs via a locking system.
    """
    def __init__(self,
            api_client:         remotes.Api,
            cache_client:       caching.RESTCache,
            locks_client:       redis_locks.RedisLocks,
            retry_cooldown:     int = 5,
            max_retries:        int = 50,
            check_errors_every: int = 5):

        self._api_client: remotes.Api             = api_client
        self._cache_client: caching.RESTCache       = cache_client
        self._locks_client: redis_locks.RedisLocks = locks_client

        self._retry_cooldown     = retry_cooldown
        self._max_retries        = max_retries
        self._check_errors_every = check_errors_every

    async def close(self):
        """
        close
        =====

        Close the locks client.
        Remember to do this!

        """
        await self._locks_client.close()

    async def _do_job(self, path: str)-> Tuple[int, str]:
        """
        _do_job
        =======

        parameters:
            path (str): The path to touch on the API client

        This is what a job actually is: Currently the action of touching a
        remote path (with a HEAD) request, which triggers computation
        upstream.

        """
        logger.info(f"Doing job {path}")
        return await self._api_client.touch(path)

    async def lock_jobs(self, potential_jobs) -> Tuple[Optional[str], Deque[str]]:
        """
        lock_jobs
        =========

        parameters:
            potential_jobs (Deque[str]): Jobs to check if its possible to start doing

        returns:
            Tuple[Optional[str], Deque[str]]
        """

        pending = None
        todo = deque()

        for job in potential_jobs[::-1]:
            logger.info(f"Checking if {job} can be performed")
            is_cached = await self._cache_client.exists(job)
            lock = await self._locks_client.lock(job)
            in_progress = not lock

            if is_cached or in_progress:
                if is_cached:
                    await self._locks_client.unlock(job)
                    logger.info(f"{job} was cached")
                if in_progress:
                    logger.info(f"{job} was in progress")
                    pending = job
                break

            todo.appendleft(job)

        return pending, todo

    async def handle_jobs(self, jobs: List[str])-> None:
        """
        handle_jobs
        ===========

        parameters:
            jobs (List[str]): A list of jobs to do

        Handles a list of jobs via the following steps:
           1 Check which jobs are pending, and which subsequent jobs that can
             be locked
           2 If a pending job, wait for the pending job to complete, checking
             if errors occur, if not, go to 4
           3 Recurse when pending job completes, removing locks (go to 1)
           4 If there is no pending job, and locks can be acquired, do the
             locked jobs.
           5 Remove locks

        The purpose is to first figure out what jobs are done and what jobs are
        being done, building a list (todo) which can the be dispatched with the
        self._do_jobs method

        """

        pending, todo = await self.lock_jobs(jobs)

        if pending is not None and len(todo) > 0:
            logger.debug(f"Pending job: {pending}")
            logger.debug(f"Jobs todo: {len(todo)}")

            pending_was_finished = False
            pending_succeeding = True

            retries = 0

            while not pending_was_finished and pending_succeeding:
                retries += 1

                if (retries % self._check_errors_every) == 0:
                    error = await self._locks_client.get_error(pending)
                    if error is not None:
                        logger.critical(f"{pending} returned an error: {error}")
                        pending_succeeding = False

                if retries > self._max_retries:
                    logger.critical(f"Exceeded max retries while waiting for {pending}")
                    pending_succeeding = False

                logger.info(f"Waiting for {pending}")
                pending_was_finished = await self._cache_client.exists(pending)

                await asyncio.sleep(self._retry_cooldown)

            if pending_succeeding:
                # Recurse
                await self._locks_client.cleanup()
                return await self.handle_jobs(jobs)
            else:
                todo = deque()

        await self._do_jobs(todo)
        await self._locks_client.cleanup()
        await self._locks_client.close()

    async def _do_jobs(self, todo: Deque[str])-> None:
        """
        _do_jobs
        ========

        parameters:
            jobs (List[str])

        Applies do_job for each job, posting errors if they occur.
        """
        for job in todo:
            try:
                status, content = await self._do_job(job)

            except asyncio.exceptions.TimeoutError:
                await self._locks_client.set_error(job, 503, f"{job} timed out")
                break

            if status == 200:
                logger.info(f"Caching {job}")
                await self._cache_client.set(job, content)
            else:
                await self._locks_client.set_error(job, status, content)

