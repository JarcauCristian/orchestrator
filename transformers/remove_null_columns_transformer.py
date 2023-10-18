from typing import List
import pandas as pd
from transformers.utils_func import generate_case_combinations


class RemoveNullColumns:

    def __init__(self, columns: List[str] = None, threshold: float = 0.7):
        self.df = None
        self.columns = columns
        self.threshold = threshold
        self._null_values = [*generate_case_combinations("Null"), *generate_case_combinations("Nan"),
                             *generate_case_combinations("Na"), 0, 0.0]

    def set_params(self, df: pd.DataFrame) -> None:
        self.df = df

    def execute(self) -> pd.DataFrame:
        if self.threshold < 0 or self.threshold > 1:
            raise ValueError("The value of the threshold should be between 0 and 1.")

        if self.columns is not None:
            for column in self.columns:
                if (self.df[column].isin(self._null_values).sum() / len(self.df[column])) > self.threshold:
                    self.df = self.df.drop(column, axis=1)
        else:
            for column in list(self.df.columns):
                if (self.df[column].isin(self._null_values).sum() / len(self.df[column])) > self.threshold:
                    self.df = self.df.drop(column, axis=1)

        return self.df.copy()

    @property
    def init_params(self):
        return ["columns", "threshold"]
