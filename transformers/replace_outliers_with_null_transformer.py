import pandas as pd
import numpy as np
from typing import List


class ReplaceOutliersWithNull:

    def __init__(self, columns: List[str] = None, threshold: float = 3):
        self.df = None
        self.columns = columns
        self.threshold = threshold

    def set_params(self, df: pd.DataFrame) -> None:
        self.df = df

    def execute(self) -> pd.DataFrame:
        if self.threshold < 0 or self.threshold > 5:
            raise ValueError("The value of the threshold should be between 0 and 5.")

        if self.columns is not None:
            for column in self.columns:
                if pd.api.types.is_numeric_dtype(self.df[column]):
                    mean = np.mean(self.df[column])
                    std = np.std(self.df[column])
                    self.df[column] = self.df[column].apply(
                        lambda x: np.nan if (np.abs((x - mean) / std) >= self.threshold) else x)
        else:
            for column in list(self.df.columns):
                if pd.api.types.is_numeric_dtype(self.df[column]):
                    mean = np.mean(self.df[column])
                    std = np.std(self.df[column])
                    self.df[column] = self.df[column].apply(
                        lambda x: np.nan if (np.abs((x - mean) / std) >= self.threshold) else x)

        return self.df.copy()

    @property
    def init_params(self):
        return ["columns", "threshold"]
