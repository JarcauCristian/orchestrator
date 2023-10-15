from typing import Any, List
from api_helper.pipeline_utils import check_steps_format


class Pipeline:

    def __init__(self, steps: List[tuple[Any, Any]]):
        self.steps = check_steps_format(steps)
        self._n = 0

    def __iter__(self):
        return self

    def __len__(self):
        return len(self.steps)

    def __next__(self):
        return self.next()

    def next(self):
        if self._n < self.__len__():
            cur, self._n = self._n, self._n + 1
            return self.steps[cur][1].execute()
        raise StopIteration()
