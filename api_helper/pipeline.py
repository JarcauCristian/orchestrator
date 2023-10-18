from typing import Any, List, Tuple
from uuid import uuid4
from api_helper.pipeline_utils import check_steps_format


class Pipeline:

    def __init__(self, steps: List[Tuple[Any, Any]]):
        self.steps = check_steps_format(steps)
        self._n = 0
        self.__id = str(uuid4())

    def __iter__(self):
        return self

    def __len__(self):
        return len(self.steps)

    def get_id(self):
        return self.__id

    def __str__(self):
        return f"Piepline id: {self.__id}"

    def append(self, step: tuple[Any, Any]):
        self.steps.append(step)

    def execute_one(self):
        self.steps[self._n][1].execute()
        self._n += 1

    # def __next__(self):
    #     return self.next()
    #
    # def next(self):
    #     if self._n < self.__len__():
    #         cur, self._n = self._n, self._n + 1
    #         return self.steps[cur][1].execute()
    #     raise StopIteration()
