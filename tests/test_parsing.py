
from unittest import TestCase
from job_manager import parse 

class TestParsing(TestCase):
    def test_basic_parse(self):
        success_cases = [
                ("foo/a/b/c",("foo",[parse.Task("a","b","c")])),
                ("bar/1/2/3/a/b/c",("bar",[parse.Task("1","2","3"),parse.Task("a","b","c")])),
            ]

        for case, outcome in success_cases:
            self.assertEqual(parse.parse_path(case), outcome)

        failure_cases = [
                "foo/a",
                "bar/a/b/c/d",
                "/baz/1/2/3",
                "foo/4/3/2/"
                ]

        for case in failure_cases:
            try:
                self.assertRaises(parse.ParsingError, parse.parse_path, case)
            except AssertionError:
                self.fail(f"Path {case} did not raise")

    def test_bidirectional(self):
        base = "foo/a/b/c/1/2/3/x/y/z"
        loa, tasks = parse.parse_path(base)
        result = parse.tasks_to_path(loa,tasks)
        self.assertEqual(base,result)

    def test_subjobs(self):
        base = "foo/a/b/c/1/2/2/x/y/z"
        jobs = parse.subjobs(base)
        self.assertListEqual(
                jobs,
                ["foo/x/y/z","foo/1/2/2/x/y/z", "foo/a/b/c/1/2/2/x/y/z"])
