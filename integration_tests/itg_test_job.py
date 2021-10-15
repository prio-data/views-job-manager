"""
Tests launching a job.
Assumes that completing a job takes around 1 second.
"""
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

async def request_job(steps):
    url = job_url(steps)
    print(msg(f"Getting {url}"))
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
    await util.clear_cache()
    status = await request_job(["y", "x"])
    while status != 200:
        status, *_ = await asyncio.gather(
                request_job(["y","x"]),
                check_cache(),
                check_jobs())

if __name__ == "__main__":
    asyncio.run(test())
