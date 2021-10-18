"""
Tests launching a job.
Assumes that completing a job takes around 1 second.
"""
import random
import uuid
import time
from datetime import datetime
import asyncio
import aiohttp

from environs import Env
import requests

import util
import settings

env = Env()
env.read_env()

NOISE_FACTOR = .1

timestamp = lambda: datetime.now().strftime("%H:%M:%S.%f")
msg = lambda msg: f"{timestamp()}: {msg}"
job_url = lambda steps: settings.JOB_MANAGER_URL + "/job" + util.steps_as_path(steps)

def sleep(t):
    print(f"Sleeping for {t} @ {timestamp()}")
    time.sleep(t)

async def request_job(steps, noise: int = 0):
    url = job_url(steps)
    print(msg(f"Getting {url}"))
    await asyncio.sleep(random.random()*(noise/4))
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            content = await response.text()
            print(msg(f"{url} returned {response.status}: {content}"))
            status = response.status
    return status

async def check_cache():
    async with aiohttp.ClientSession() as session:
        async with session.get(settings.CACHE_URL+"/files/") as response:
            print(msg(f"Cached jobs: {await response.text()}"))

async def check_jobs():
    async with aiohttp.ClientSession() as session:
        async with session.get(settings.JOB_MANAGER_URL + "/job") as response:
            print(msg(f"Current jobs: {await response.text()}"))

async def test():
    requests.delete(settings.SOURCE_URL+"/requests/")
    await util.clear_cache()

    responses = set()
    while not 200 in responses:
        responses = set(await asyncio.gather( *[request_job(["y","x"], random.randint(1,4)) for i in range(32)], check_cache(), check_jobs()))

    try:
        assert (n_requests := requests.get(settings.SOURCE_URL+"/requests/").json()["number_of_requests"]) == 2
    except AssertionError:
        print(f"ERROR: Got {n_requests} requests!")
    else:
        print(f"Got {n_requests} requests, as expected.")

if __name__ == "__main__":
    asyncio.run(test())
