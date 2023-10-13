from typing import Any
import pandas as pd


class Pipeline:

    def __init__(self, steps: tuple[Any, Any]):
        self._loader = ["csv_loader"]
        self._transformers = ["remove_null_columns", "remove_null_rows", "remove_same_value_columns",
                              "replace_outliers_with_null"]
        self._exporters = ["csv_exporter"]
        self.steps = steps

    def __len__(self):
        return len(self.steps)

    def execute_workflow(self):
        if self.__len__() > 0:
            pass
