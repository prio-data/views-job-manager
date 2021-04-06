import os
import logging
from hashlib import md5
import datetime
from typing import List,Tuple,Iterator,Any
from pathlib import PurePath
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
from sqlalchemy.orm import relationship,backref
from settings import config
logger = logging.getLogger(__name__)

Base = declarative_base()

digest = lambda p: md5(p.encode()).hexdigest()

class ParsingError(Exception):
    pass

class Task(Base):
    __tablename__ = "tasks"
    pk = sa.Column(sa.Integer,primary_key=True)
    order_place = sa.Column(sa.Integer,nullable=False)

    destination = sa.Column(sa.String,nullable=False)
    namespace = sa.Column(sa.String,nullable=False)
    args = sa.Column(sa.String,nullable=False)

    job_id = sa.Column(sa.String,sa.ForeignKey("jobs.id_hash"))
    job = relationship("Job",back_populates="tasks")

    def __repr__(self):
        return f"Task(no. {self.order_place} \"{self.path()}\")"

    def path(self):
        return os.path.join(self.destination,self.namespace,self.args)

class Job(Base):
    __tablename__ = "jobs"
    id_hash=sa.Column(sa.String,primary_key=True)
    level_of_analysis=sa.Column(sa.String)
    started_on=sa.Column(sa.DateTime,default=datetime.datetime.now)
    est_duration=sa.Column(sa.Integer,default=0)
    tasks = relationship("Task",cascade="all, delete")

    @classmethod
    def parse_whole_path(cls,path:str):
        level_of_analysis,*tail = PurePath(path).parts
        try:
            chunks = [[i] + ch for i,ch in enumerate(chunk(tail,3))]
        except AssertionError as ae:
            raise ParsingError from ae
        tasks = [Task(destination=d,namespace=p,args=a,order_place=i) for i,d,p,a in chunks]

        instance = cls(id_hash=path,#digest(path),
                level_of_analysis=level_of_analysis,
                tasks=tasks)
        return instance 

    def __str__(self):
        return f"Job(\"{self.id_hash[:5]}...\", Tasks:{self.tasks})"

    def path(self):
        return os.path.join(self.level_of_analysis,*(t.path() for t in self.tasks))

    def est_finished(self):
        return self.started_on + datetime.timedelta(seconds=self.est_duration)

    def age(self):
        return (datetime.datetime.now()-self.started_on).seconds

    def expired(self):
        return self.age() > config("JOB_TIMEOUT")

    def subjobs(self):
        todo = self.tasks[1:][::-1]
        jobs = []
        preceding = []
        for task in todo:
            path = os.path.join(self.level_of_analysis,task.path(),*preceding)
            jobs.append(Job.parse_whole_path(path))
            preceding.append(task.path())
        return jobs

    def is_cached(self,cache):
        return cache.exists(self.path())

def chunk(it:Iterator[Any],chunksize):
    assert len(it) % chunksize == 0
    return [it[i:i+3] for i in range(0,len(it),chunksize)]

if __name__ == "__main__":
    job = Job.parse_whole_path("foo/a/a/a/b/b/b")
    print(job)
    print(job.path())
    print(job.tasks[-1].path())
