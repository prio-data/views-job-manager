import logging
import datetime
from typing import List

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa

from . import parsing, remotes, caching

logger = logging.getLogger(__name__)

Base = declarative_base()

class Job(Base):
    """
    A job is a list of tasks, that together comprise a remote path that must be available.
    """
    __tablename__ = "jobs"
    path = sa.Column(sa.String, primary_key = True)
    started_on = sa.Column(sa.DateTime, default = datetime.datetime.now)

    tasks: List[parsing.Task]
    loa: str

    def _parse_path(self,path):
        self.loa, self.tasks = parsing.parse_path(path)

    def __init__(self, path: str = None, tasks: List[parsing.Task] = None, loa: str = None):
        if path:
            self.path = path
            self._parse_path(path)
        elif tasks and loa:
            self.tasks = tasks
            self.loa = loa
            self.path = parsing.tasks_to_path(loa,tasks)
        else:
            TypeError("Job must be instantiated with either a path, or a list of tasks + a loa")

    def init_on_load(self):
        self._parse_path(self.path)

    def subjobs(self):
        """
        Returns a list of subjobs (dependent jobs) that must be completed
        before this job can be completed.
        """
        subjobs = [Job(loa = self.loa, tasks = self.tasks[i:]) for i in range(len(self.tasks))]
        subjobs.reverse()
        return subjobs

    def exists(self, cache: caching.BlobStorageCache)-> bool:
        return cache.exists(self.path)

    def get_result(self, cache: caching.BlobStorageCache) -> bytes:
        return cache.get(self.path)

    def touch(self, api: remotes.Api)-> None:
        """
        Syncronously touch a remote resource, waiting until it becomes available, or fails
        """
        api.touch(self.path)

    def age(self):
        return (datetime.datetime.now - self.started_on).seconds

class Error(Base):
    __tablename__ = "errors"
    path = sa.Column(sa.String, primary_key = True)
    status_code = sa.column(sa.Integer)
    content = sa.column(sa.String)
    posted_on = sa.Column(sa.DateTime, default = datetime.datetime.now)

    def age(self):
        return (datetime.datetime.now - self.posted_on).seconds
