import io
import json
from typing import Any, Dict
import pandas as pd

from minio_storage.client import Client


class CSVExporter:

    def __init__(self, dataset_name: str = "", tags: Dict[Any, Any] = None):
        self.df = None
        self.dataset_name = dataset_name
        self.tags = tags
        self._file = io.BytesIO()
        self._endpoint = 'put_object'
        self._ctx = Client()

    def set_params(self, df: pd.DataFrame) -> None:
        self.df = df

    def execute(self) -> str | None:
        self.df.to_csv(self._file)
        body = {
            "file": self._file.getvalue(),
            "file_name": self.dataset_name,
            "tags": json.dumps(self.tags).encode('utf-8')
        }

        try:
            response = self._ctx.send_urllib3_request(endpoint=self._endpoint, method="PUT", fields=body)
            return response
        except TypeError as e:
            print(f"An error occurred: {e}")
            return None

    @property
    def init_params(self):
        return ["dataset_name", "tags"]
