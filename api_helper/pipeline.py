import pandas as pd

from loaders.csv_loader import CSVLoader


class Pipeline:

    def __init__(self):
        self._loader = ["csv_loader"]
        self._transformers = ["remove_null_columns", "remove_null_rows", "remove_same_value_columns",
                              "replace_outliers_with_null"]
        self._exporters = ["csv_exporter"]
        self.block = None

    def execute_block(self, block_type: str, block_data: pd.DataFrame = pd.DataFrame()):
        pass
