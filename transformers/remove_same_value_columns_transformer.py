import pandas as pd
from typing import List


class RemoveSameValueColumns:

    def __init__(self, columns: List[str] = None):
        self.df = None
        self.columns = columns

    def set_params(self, df: pd.DataFrame) -> None:
        self.df = df

    def execute(self, columns: list[str] = None):
        if columns is not None:
            for column in columns:
                if self.df[column].isin([self.df[column][0]]).sum() == len(self.df[column]):
                    self.df = self.df.drop(column, axis=1)
        else:
            for column in list(self.df.columns):
                if self.df[column].isin([self.df[column][0]]).sum() == len(self.df[column]):
                    self.df = self.df.drop(column, axis=1)

        return self.df.copy()

    @property
    def init_params(self):
        return ["columns"]
