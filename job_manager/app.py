
import logging
from collections import deque
from datetime import datetime
import asyncio
import asyncio_redis
import redis
import fastapi
from . import settings, keys, parse, remotes, cache

logging.basicConfig(level = getattr(logging, settings.LOG_LEVEL))
logger = logging.getLogger(__name__)
redis_logger = logging.getLogger("asyncio_redis")
redis_logger.setLevel(logging.WARNING)

app = fastapi.FastAPI()

Client = lambda: asyncio_redis.Pool.create(host = settings.REDIS_HOST, port = settings.REDIS_PORT, db = settings.REDIS_DB)
cache_client = cache.RESTCache(settings.DATA_CACHE_URL+"/files")
api_client = remotes.Api(settings.ROUTER_URL)

async def request_job_computation(client_creator, subjobs):

    async def do_job(path):
        logger.info(f"Getting {path}")
        return await api_client.touch(path)

    client = await client_creator()
    todo = deque()

    for job in subjobs[::-1]:
        is_cached = await cache_client.exists(job)

        could_lock = await client.set(keys.job(job),str(datetime.now()), only_if_not_exists = True, expire = 400)
        in_progress = could_lock is None

        if is_cached or in_progress:
            if is_cached:
                client.delete([keys.job(job)])
                logger.info(f"{job} was cached")
            if in_progress:
                logger.info(f"{job} was in progress")
            break
        todo.appendleft(job)

    if len(todo) != len(subjobs) and len(todo) > 0:
        pending = subjobs[len(todo)]

        pending_was_finished = False
        retries = 0
        while not pending_was_finished:
            if retries > settings.MAX_RETRIES:
                logger.info(f"Exceeded max retries while waiting for {pending}")
                todo = []
                break

            logger.info(f"Waiting for {pending}")
            pending_was_finished = cache_client.exists(pending)
            await asyncio.sleep(1)

        return await request_job_computation(client_creator, subjobs)

    for job in todo:
        status, content = await do_job(job)
        if status == 200:
            logger.info(f"Caching {job}")
            await cache_client.set(job, content)
        else:
            await client.set(keys.error(job), f"{status}: {content}", expire = 400)

        await client.delete([keys.job(job)])

    client.close()

async def with_redis_client():
    try:
        connection = await Client()
        yield connection
    finally:
        connection.close()

@app.get("/job/")
async def list_jobs(redis_client: redis.Redis = fastapi.Depends(with_redis_client)):
    jobs = await redis_client.keys(keys.job("*"))
    return {"jobs": [*jobs]}

@app.get("/job/{path:path}")
async def get_job(
        path: str,
        background_tasks: fastapi.BackgroundTasks,
        redis_client: asyncio_redis.Pool = fastapi.Depends(with_redis_client)):

    requested_jobs = parse.subjobs(path)

    # Return error message
    for job in requested_jobs:
        try:
            error = await redis_client.get(keys.error(job))
            assert error is not None
        except AssertionError:
            pass
        else:
            try:
                code, message = error.split(":")
            except ValueError:
                code, message = "500", error
            return fastapi.Response(f"{job} returned {message}", status_code = int(code))

    # Return from cache
    try:
        content = await cache_client.get(requested_jobs[-1])
    except cache.NotCached:
        pass
    else:
        return fastapi.Response(content)

    background_tasks.add_task(request_job_computation, Client, requested_jobs)

    return fastapi.Response(status_code = 202)

@app.get("/errors/")
async def get_errors(redis_client: asyncio_redis.Pool = fastapi.Depends(with_redis_client)):
    async def get_error(client, k):
        return await client.get(keys.error(k))

    errors = await redis_client.smembers("jobman/errors")
    messages = await asyncio.gather(*[get_error(redis_client, e) for e in errors])

    return dict(zip(errors, messages))

@app.get("/errors/purge/")
async def delete_errors(redis_client: redis.Redis = fastapi.Depends(with_redis_client)):
    return fastapi.Response("")
