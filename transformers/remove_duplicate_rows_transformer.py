from typing import List

import pandas as pd


class RemoveDuplicateRows:

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def execute(self, columns: List[str] = None) -> pd.DataFrame:
        if columns is not None:
            return self.df.drop_duplicates(subset=['column1', 'column2'])

        return self.df.drop_duplicates()
