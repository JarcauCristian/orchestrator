import pandas as pd
from transformers.utils_func import generate_case_combinations


class RemoveNullRows:

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self._null_values = [*generate_case_combinations("Null"), *generate_case_combinations("Nan"),
                             *generate_case_combinations("Na"), 0, 0.0]

    def execute(self, threshold: float = 0.7) -> pd.DataFrame:
        if threshold < 0 or threshold > 1:
            raise ValueError("The value of the threshold should be between 0 and 1.")

        mask = self.df.apply(lambda row: sum(val in self._null_values for val in row) /
                             len(self.df.columns) <= threshold, axis=1)

        return self.df[mask]
