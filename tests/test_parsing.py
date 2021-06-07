
from unittest import TestCase
from job_manager import parsing

class TestParsing(TestCase):
    def test_basic_parsing(self):
        success_cases = [
                ("foo/a/b/c",("foo",[parsing.Task("a","b","c")])),
                ("bar/1/2/3/a/b/c",("bar",[parsing.Task("1","2","3"),parsing.Task("a","b","c")])),
            ]

        for case, outcome in success_cases:
            self.assertEqual(parsing.parse_path(case), outcome)

        failure_cases = [
                "foo/a",
                "bar/a/b/c/d",
                "/baz/1/2/3",
                "foo/4/3/2/"
                ]

        for case in failure_cases:
            try:
                self.assertRaises(parsing.ParsingError, parsing.parse_path, case)
            except AssertionError:
                self.fail(f"Path {case} did not raise")

    def test_bidirectional(self):
        base = "foo/a/b/c/1/2/3/x/y/z"
        loa, tasks = parsing.parse_path(base)
        result = parsing.tasks_to_path(loa,tasks)
        self.assertEqual(base,result)
