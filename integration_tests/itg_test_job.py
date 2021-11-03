"""
Tests launching a job.
Assumes that completing a job takes around 1 second.
"""
import string
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
    await asyncio.sleep((random.random()*(noise/8)))
    url = job_url(steps)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            content = await response.text()
            status = response.status
    return status

async def check_cache():
    async with aiohttp.ClientSession() as session:
        async with session.get(settings.CACHE_URL+"/files/") as response:
            print(msg(f"Cached jobs: {await response.text()}"))

async def check_jobs():
    async with aiohttp.ClientSession() as session:
        async with session.get(settings.JOB_MANAGER_URL + "/job") as response:
            data = await response.json()
            print(msg(f"Current jobs: {data['jobs']}"))
            return data


async def test():
    requests.delete(settings.SOURCE_URL+"/requests/")
    await util.clear_cache()

    responses = set()
    n_jobs = set()
    all_responses = set()
    while not 200 in responses:
        print("Checking jobs")
        jobs = await check_jobs()
        n_jobs = n_jobs.union({len(jobs["jobs"])})
        print("Getting jobs")
        jobs = [request_job(list(string.ascii_lowercase[:2]), random.randint(1,4)) for i in range(25)]
        jobs += [request_job(list(string.ascii_lowercase[:3]), random.randint(1,4)) for i in range(25)]
        jobs += [request_job(list(string.ascii_lowercase[:7]), random.randint(1,4)) for i in range(25)]
        responses = set(await asyncio.gather(*jobs))
        print(f"Got {responses}")
        all_responses |= responses
        print("Checking cache")
        await check_cache()

    try:
        assert len(bad_responses := all_responses.difference({202,200})) == 0
    except AssertionError:
        print(f"Got a bad http codes: {bad_responses}")
    else:
        print("Only got good responses!")
    
    try:
        assert all((n <= 2 for n in n_jobs))
    except AssertionError:
        print(f"There was, at some point, more than two jobs: {n_jobs}")
    else:
        print(f"Always lte two jobs: {n_jobs}")

    try:
        assert (n_requests := requests.get(settings.SOURCE_URL+"/requests/").json()["number_of_requests"]) == 2
    except AssertionError:
        print(f"ERROR: Got {n_requests} requests!")
    else:
        print(f"Got {n_requests} requests, as expected.")

if __name__ == "__main__":
    asyncio.run(test())
