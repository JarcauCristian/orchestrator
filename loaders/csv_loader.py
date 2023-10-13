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

    def load(self) -> pd.DataFrame:
        try:
            response = self._ctx.send_request(endpoint=self._endpoint,
                                              method="GET",
                                              query_params={'dataset_path': self.path})

            response_url = json.loads(response)["url"]
            try:
                response = requests.get(response_url)

                if response.status_code != 200:
                    return pd.DataFrame()

                return pd.read_csv(io.StringIO(response.text))
            except Exception as e:
                print(f"Error fetching data from the server: {str(e)}")
                return pd.DataFrame()
        except TypeError as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()



