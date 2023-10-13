from api_helper.pipeline import Pipeline
from loaders.csv_loader import CSVLoader

if __name__ == '__main__':
    pipeline = Pipeline(("csv_loader", CSVLoader()))
