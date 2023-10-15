import json
import requests
import io
from minio_storage.client import Client
import pandas as pd


class CSVLoader:

    def __init__(self, path: str):
        self.path = path
        self._endpoint = 'get_object'
        self._ctx = Client()
        self._df = pd.DataFrame()

    def execute(self) -> None:
        try:
            response = self._ctx.send_request(endpoint=self._endpoint,
                                              method="GET",
                                              query_params={'dataset_path': self.path})
            response_url = json.loads(response.content.decode('utf-8'))["url"]
            try:
                response = requests.get(response_url)

                if response.status_code != 200:
                    print(f"Error fetching data from the server, status_code: {response.status_code}")

                self._df = pd.read_csv(io.StringIO(response.text))
            except Exception as e:
                print(f"Error fetching data from the server: {str(e)}")

        except TypeError as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()

    def get_statist
