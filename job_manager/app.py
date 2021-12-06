
from typing import List
import logging
from fastapi import Depends, Response, FastAPI, BackgroundTasks
from . import settings, parse, remotes, caching, job_handler, redis_locks

logging.basicConfig(level = getattr(logging, settings.LOG_LEVEL))
logger = logging.getLogger(__name__)
redis_logger = logging.getLogger("asyncio_redis")
redis_logger.setLevel(logging.WARNING)

app = FastAPI()

get_api = lambda: remotes.Api(settings.ROUTER_URL)
get_cache = lambda: caching.RESTCache(settings.DATA_CACHE_URL+"/files")
get_locks = lambda: redis_locks.RedisLocks(settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_DB, settings.REDIS_ERROR_KEY_PREFIX, settings.REDIS_JOB_KEY_PREFIX)

def with_rest_cache():
    try:
        client = get_cache()
        yield client
    finally:
        pass

async def with_locks_client():
    try:
        client = get_locks()
        yield client
    finally:
        await client.close()

async def dispatch_jobs(jobs: List[str]):
    try:
        api, cache, locks = get_api() ,get_cache(), get_locks()
        handler = job_handler.JobHandler(api, cache, locks,
                    settings.RETRY_SLEEP, settings.MAX_RETRIES, settings.CHECK_ERRORS_EVERY)

        await handler.handle_jobs(jobs)
    finally:
        await locks.cleanup()
        await locks.close()

@app.get("/job/")
async def list_jobs(locks: redis_locks.RedisLocks = Depends(with_locks_client)):
    jobs = await locks.jobs()
    return {"jobs": jobs}

@app.get("/job/{path:path}")
async def get_job(
        path: str,
        background_tasks: BackgroundTasks,
        locks_client: redis_locks.RedisLocks = Depends(with_locks_client),
        cache_client: caching.RESTCache = Depends(with_rest_cache)):

    try:
        requested_jobs = parse.subjobs(path)
    except parse.ParsingError:
        return Response(content = f"Could not parse as job path: {path}", status_code = 404)

    for job in requested_jobs:
        try:
            error = await locks_client.error_code_and_message(job)
            assert error is None
        except AssertionError:
            code, message = error
            return Response(f"{job} returned {message}", status_code = code)

    try:
        content = await cache_client.get(requested_jobs[-1])
    except caching.NotCached:
        pass
    else:
        return Response(content)

    background_tasks.add_task(dispatch_jobs, requested_jobs)

    return Response(status_code = 202)

@app.get("/errors/")
async def get_errors(locks_client: redis_locks.RedisLocks = Depends(with_locks_client)):
    errors = await locks_client.errors()
    return {"errors": errors}

@app.get("/errors/purge/")
async def delete_errors(locks_client: redis_locks.RedisLocks = Depends(with_locks_client)):
    await locks_client.clear_errors()
    return Response(status_code = 204)
