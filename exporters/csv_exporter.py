import io
import json
from typing import Any
import pandas as pd

from minio_storage.client import Client


class CSVExporter:

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self._file = io.BytesIO()
        self._endpoint = 'put_object'
        self._ctx = Client()

    def export(self, dataset_name: str, tags: dict[Any, Any] = dict) -> str | None:
        self.df.to_csv(self._file)
        body = {
            "file": self._file.getvalue(),
            "file_name": dataset_name,
            "tags": json.dumps(tags).encode('utf-8')
        }

        try:
            response = self._ctx.send_urllib3_request(endpoint=self._endpoint, method="PUT", fields=body)
            return response
        except TypeError as e:
            print(f"An error occurred: {e}")
            return None
