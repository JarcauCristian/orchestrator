import psycopg2
import pandas as pd
from typing import Union
from urllib.parse import quote_plus
import sqlalchemy.exc
from sqlalchemy import create_engine
from minio_storage.client import Client


class PostgresLoader:

    def __init__(self, db_name: str,
                 user: str, password: str,
                 host: str, port: Union[int, None] = 5432):
        self._ctx = Client()
        self._engine = create_engine(f'postgresql+psycopg2://{user}:{quote_plus(password)}@'
                                     f'{host}:{port}/'
                                     f'{db_name}')
        self._file = None

    def load(self, table_name: str, description: str) -> bool:
        conn = None
        try:
            conn = self._engine.connect()

            df = pd.read_sql_table(table_name, conn)

            self._file = {"data": df,
                          "name": f"{table_name}.csv",
                          "description": description}

        except (Exception, psycopg2.DatabaseError, sqlalchemy.exc.ObjectNotExecutableError):
            return False
        finally:
            if conn is not None:
                conn.close()

            if self._file is None:
                return False

            return True

    def save_data(self) -> str:
        if self._file is None:
            return "Load the data before!"

        result = self._ctx.upload_data(self._file["data"], self._file["name"])

        if result is None:
            return "An error occurred when uploading the object!"

        return result
