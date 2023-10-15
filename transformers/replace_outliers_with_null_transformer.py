import pandas as pd
import numpy as np


class ReplaceOutliersWithNull:

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def execute(self, columns: list[str] = None, threshold: float = 3) -> pd.DataFrame:
        if threshold < 0 or threshold > 5:
            raise ValueError("The value of the threshold should be between 0 and 5.")

        if columns is not None:
            for column in columns:
                if pd.api.types.is_numeric_dtype(self.df[column]):
                    mean = np.mean(self.df[column])
                    std = np.std(self.df[column])
                    self.df[column] = self.df[column].apply(
                        lambda x: np.nan if (np.abs((x - mean) / std) >= threshold) else x)
        else:
            for column in list(self.df.columns):
                if pd.api.types.is_numeric_dtype(self.df[column]):
                    mean = np.mean(self.df[column])
                    std = np.std(self.df[column])
                    self.df[column] = self.df[column].apply(
                        lambda x: np.nan if (np.abs((x - mean) / std) >= threshold) else x)

        return self.df.copy()
