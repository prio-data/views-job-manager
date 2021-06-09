
from unittest import TestCase
from job_manager import models, parsing

class TestJobs(TestCase):
    def test_subjobs(self):
        job = models.Job("foo/1/2/3/a/b/c/x/y/z")
        subjobs = job.subjobs()
        self.assertEqual(len(subjobs),3)

        self.assertEqual(subjobs[0].tasks[0], parsing.Task("x","y","z"))
        self.assertEqual(len(subjobs[0].tasks), 1)

        self.assertEqual(subjobs[1].tasks[1], parsing.Task("x","y","z"))
        self.assertEqual(subjobs[1].tasks[0], parsing.Task("a","b","c"))

