
from typing import List
import logging
from fastapi import Depends, Response, FastAPI, BackgroundTasks
from . import settings, parse, remotes, cache, job_handler, redis_locks

logging.basicConfig(level = getattr(logging, settings.LOG_LEVEL))
logger = logging.getLogger(__name__)
redis_logger = logging.getLogger("asyncio_redis")
redis_logger.setLevel(logging.WARNING)

app = FastAPI()

async def with_api_client():
    try:
        client = remotes.Api(settings.ROUTER_URL)
        yield client
    finally:
        pass

async def with_rest_cache():
    try:
        client = cache.RESTCache(settings.DATA_CACHE_URL)
        yield client
    finally:
        pass

async def with_locks_client():
    try:
        client = redis_locks.RedisLocks(settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_DB, settings.REDIS_ERROR_KEY_PREFIX, settings.REDIS_JOB_KEY_PREFIX)
        yield client
    finally:
        client.close()

async def with_job_handler(
        api_client: remotes.Api = Depends(with_api_client),
        cache_client: cache.RESTCache = Depends(with_rest_cache),
        locks_client: redis_locks.RedisLocks = Depends(with_locks_client)):
    try:
        client = job_handler.JobHandler(api_client, cache_client, locks_client,
                settings.RETRY_SLEEP, settings.MAX_RETRIES, settings.CHECK_ERRORS_EVERY)
        yield client
    finally:
        client.close()

async def dispatch_jobs(jobs: List[str], handler: job_handler.JobHandler = Depends(with_job_handler)):
    handler.handle_jobs(jobs)

@app.get("/job/")
async def list_jobs(locks: redis_locks.RedisLocks = Depends(with_locks_client)):
    jobs = await locks.jobs()
    return {"jobs": jobs}

@app.get("/job/{path:path}")
async def get_job(
        path: str,
        background_tasks: BackgroundTasks,
        locks_client: redis_locks.RedisLocks = Depends(with_locks_client),
        cache_client: cache.RESTCache = Depends(with_rest_cache)):

    requested_jobs = parse.subjobs(path)

    for job in requested_jobs:
        try:
            error = await locks_client.error_code_and_message(job)
            assert error is None
        except AssertionError:
            code, message = error
            return Response(f"{job} returned {message}", status_code = code)

    try:
        content = await cache_client.get(requested_jobs[-1])
    except cache.NotCached:
        pass
    else:
        return Response(content)

    background_tasks.add_task(dispatch_jobs, requested_jobs)

    return Response(status_code = 202)

@app.get("/errors/")
async def get_errors(locks_client: redis_locks.RedisLocks = Depends(with_locks_client)):
    errors = await locks_client.errors()
    return errors

@app.get("/errors/purge/")
async def delete_errors(locks_client: redis_locks.RedisLocks = Depends(with_locks_client)):
    locks_client.clear_errors()
    return Response(status_code = 204)
