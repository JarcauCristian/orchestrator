import pandas as pd
from transformers.utils_func import generate_case_combinations


class RemoveNullColumns:

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self._null_values = [*generate_case_combinations("Null"), *generate_case_combinations("Nan"),
                             *generate_case_combinations("Na"), 0, 0.0]

    def execute(self, columns: list[str] = list, threshold: float = 0.7) -> pd.DataFrame:
        if threshold < 0 or threshold > 1:
            raise ValueError("The value of the threshold should be between 0 and 1.")

        if len(columns) > 0:
            for column in columns:
                if (self.df[column].isin(self._null_values).sum() / len(self.df[column])) > threshold:
                    self.df = self.df.drop(column, axis=1)
        else:
            for column in list(self.df.columns):
                if (self.df[column].isin(self._null_values).sum() / len(self.df[column])) > threshold:
                    self.df = self.df.drop(column, axis=1)

        return self.df.copy()

