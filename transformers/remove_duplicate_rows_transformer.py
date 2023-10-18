from typing import List

import pandas as pd


class RemoveDuplicateRows:

    def __init__(self, columns: List[str] = None):
        self.df = None
        self.columns = columns

    def set_params(self, df: pd.DataFrame) -> None:
        self.df = df

    def execute(self) -> pd.DataFrame:
        if self.columns is not None:
            return self.df.drop_duplicates(subset=self.columns)

        return self.df.drop_duplicates()

    @property
    def init_params(self):
        return ["columns"]
