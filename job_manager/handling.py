
from . import remotes, caching, models

def exists(cache, job)-> bool:
    return cache.exists(job.path)

def touch(api: remotes.Api, job: models.Job)-> None:
    api.touch(job.path)
