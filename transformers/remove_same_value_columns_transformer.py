import pandas as pd


class RemoveSameValueColumns:

    def __init__(self, df: pd.self.dfFrame):
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
