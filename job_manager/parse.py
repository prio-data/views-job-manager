import os
import re
from typing import Iterator, Any, Tuple, List
from functools import reduce
from dataclasses import dataclass
from pathlib import PurePath

class ParsingError(Exception):
    pass

@dataclass
class Task:
    namespace: str
    name: str
    arguments: str

    def path(self):
        return os.path.join(self.namespace,self.name,self.arguments)

    def __str__(self):
        return f"Task({self.namespace}/{self.name}/{self.arguments})"

def chunk(it: Iterator[Any],chunksize) -> List[List[Any]]:
    assert len(it) % chunksize == 0
    return [it[i:i+3] for i in range(0,len(it),chunksize)]

def tasks_to_path(loa, tasks: List[Task]) -> str:
    """
    Returns a path from a list of tasks, and a level of analysis
    """
    return reduce(os.path.join, [loa, *[t.path() for t in tasks]])

def parse_path(path: str)-> Tuple[str, List[Task]]:
    """
    Returns a list of tasks from a raw path string
    """

    try:
        assert re.search(r"^[^/]+(?:(?:/[^/]+){3})+$", path)
        level_of_analysis,*tail = PurePath(path).parts
        tasks = [Task(namespace,name,args) for namespace,name,args in chunk(tail,3)]

    except (AssertionError,ValueError, AssertionError) as ae:
        raise ParsingError(f"Could not parse path: {path}") from ae

    return level_of_analysis, tasks

def subjobs(path: str) -> List[str]:
    loa, tasks = parse_path(path)
    return [tasks_to_path(loa, tasks[-i-1:]) for i in range(len(tasks))]
