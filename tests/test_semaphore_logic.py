
import logging
from unittest import TestCase
#from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import httpretty

from job_manager import models, crud, remotes

engine = create_engine("sqlite://")
Session = sessionmaker(bind=engine)
models.Base.metadata.create_all(engine)

class MockApi():
    def __init__(self):
        self.touched = []

    def touch(self,p):
        self.touched.append(p)

class MockCache():
    def __init__(self):
        self.dict = dict()
    def get(self,k):
        return self.dict[k]
    def set(self,k,v):
        self.dict[k] = v
    def exists(self,k):
        return k in self.dict.keys()

class TestSemaphoreLogic(TestCase):
    def setUp(self):
        self.sess = Session()
        self.api = MockApi()
        self.cache = MockCache()

    def handle_job(self,job, job_lifetime = 4000, retry_time = 1):
        crud.handle_job(job_lifetime, retry_time, self.sess, self.cache, self.api, job)

    def test_handling_job(self):
        """
        Just checking if the function actually works
        """
        job = models.Job("foo/a/b/c/1/2/3")
        self.handle_job(job)
        self.assertEqual(self.api.touched[0], "foo/1/2/3")
        self.assertEqual(self.api.touched[1], "foo/a/b/c/1/2/3")

    def test_is_cached(self):
        """
        Check the handler does not re-do already cached operations, but
        proceeds to the subsequent operations.
        """
        job = models.Job("foo/a/b/c/1/2/3/x/y/z")
        self.cache.dict["foo/x/y/z"] = "bar"
        self.handle_job(job)
        self.assertEqual(self.api.touched[0], "foo/1/2/3/x/y/z")
        self.assertEqual(self.api.touched[1], "foo/a/b/c/1/2/3/x/y/z")

    def test_existing(self):
        """
        Waits for an existing job
        """
        subjob = models.Job("foo/pre/existing/job")
        self.sess.add(subjob)
        self.sess.commit()
        self.cache.dict["foo/pre/existing/job"] = "bar"

        job = models.Job("foo/1/2/3/pre/existing/job")
        self.handle_job(job, job_lifetime = 10, retry_time = 0.05)

        self.assertEqual(self.api.touched[0],"foo/1/2/3/pre/existing/job")
        self.assertEqual(len(self.api.touched),1)

    def test_several_existing(self):
        self.cache.dict["foo/the/first/job"] = "bar"
        self.cache.dict["foo/the/second/job/the/first/job"] = "bar"
        self.sess.add(models.Job("foo/the/second/job/the/first/job"))
        self.sess.commit()
        final_url = "foo/the/third/job/the/second/job/the/first/job"
        self.handle_job(models.Job(final_url), job_lifetime = 10, retry_time = 0.05)
        self.assertEqual(self.api.touched[0],final_url)
        self.assertEqual(len(self.api.touched),1)

    def test_already_requested(self):
        self.sess.add(models.Job("foo/al/ready/requested"))
        self.sess.commit()
        self.assertRaises(
                crud.AlreadyRequested,
                self.handle_job,
                models.Job("foo/al/ready/requested"))

    @httpretty.activate()
    def test_remote_http_error(self):
        httpretty.register_uri(
                "GET",
                "http://bad-api/foo/i/will/break",
                "Something went wrong!",
                status = 500)

        def fails():
            crud.handle_job(10,0.05,self.sess,self.cache,remotes.Api("http://bad-api"),
                    models.Job("foo/i/will/break")
                    )
        self.assertRaises(crud.JobHttpError,fails)
