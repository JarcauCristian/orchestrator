from typing import Tuple, List, Any, Dict
from pathlib import Path
import importlib
from exporters.csv_exporter import CSVExporter
from loaders.csv_loader import CSVLoader
from transformers.remove_duplicate_rows_transformer import RemoveDuplicateRows
from transformers.remove_null_columns_transformer import RemoveNullColumns
from transformers.remove_null_rows_transformer import RemoveNullRows
from transformers.remove_same_value_columns_transformer import RemoveSameValueColumns
from transformers.replace_outliers_with_null_transformer import ReplaceOutliersWithNull

loaders_params = {
    "csv_loader": {
        "params": CSVLoader().init_params,
        "name": "CSVLoader"
    }
}

transformers_params = {
    "remove_duplicate_rows_transformer": RemoveDuplicateRows().init_params,
    "remove_null_columns_transformer": RemoveNullColumns().init_params,
    "remove_null_rows_transformer": RemoveNullRows().init_params,
    "remove_same_value_columns_transformer": RemoveSameValueColumns().init_params,
    "replace_outliers_with_null_transformer": ReplaceOutliersWithNull().init_params
}

exporters_params = {
    "csv_exporter": {
        "params": CSVExporter().init_params
    }
}


def check_steps_format(steps: List[Tuple[Any, Any]]):
    if len(steps) == 1:
        if steps[0][0] not in get_loaders():
            raise TypeError("The first step should be a loader.")
    elif len(steps) == 2:
        if steps[0][0] not in get_loaders():
            raise TypeError("The first step should be a loader.")
        if steps[-1][0] not in get_exporters():
            raise TypeError("The last step should be an exporter.")
    else:
        if steps[0][0] not in get_loaders():
            raise TypeError("The first step should be a loader.")
        if steps[-1][0] not in get_exporters():
            raise TypeError("The last step should be an exporter.")
        for step in steps[1:-1]:
            if step not in get_transformers():
                raise TypeError("The steps in the middle should be transformers.")

    return steps


def get_loaders():
    loaders = []
    cwd = Path.cwd() / "loaders"
    for path in cwd.glob("*.py"):
        last_part = path.parts[-1]
        if 'loader' in last_part:
            loaders.append(last_part.split('.')[0])

    return loaders


def get_loaders_with_params():
    params = {}
    cwd = Path.cwd() / "loaders"
    for path in cwd.glob("*.py"):
        last_part = path.parts[-1]
        if 'loader' in last_part:
            params[last_part.split(".")[0]] = loaders_params[last_part.split(".")[0]]['params']

    return params


def get_transformers():
    transformers = []
    cwd = Path.cwd() / "transformers"
    for path in cwd.glob("*.py"):
        last_part = path.parts[-1]
        if 'transformer' in last_part:
            transformers.append(last_part.split('.')[0])

    return transformers


def get_transformers_with_params():
    params = {}
    cwd = Path.cwd() / "transformers"
    for path in cwd.glob("*.py"):
        last_part = path.parts[-1]
        if 'transformer' in last_part:
            params[last_part.split(".")[0]] = transformers_params[last_part.split(".")[0]]['params']

    return params


def get_exporters():
    exporters = []
    cwd = Path.cwd() / "exporters"
    for path in cwd.glob("*.py"):
        last_part = path.parts[-1]
        if 'exporter' in last_part:
            exporters.append(last_part.split('.')[0])

    return exporters


def get_exporters_with_params():
    params = {}
    cwd = Path.cwd() / "exporters"
    for path in cwd.glob("*.py"):
        last_part = path.parts[-1]
        if 'exporters' in last_part:
            params[last_part.split(".")[0]] = exporters_params[last_part.split(".")[0]]['params']

    return params


def create_pipeline_object(params: Dict[str, Any]):
    pipeline_type = params['name']

    if pipeline_type in get_loaders():
        module = importlib.import_module(f'loaders.{pipeline_type}')
        if hasattr(module, loaders_params[pipeline_type]['name']):
            obj = getattr(module, loaders_params[pipeline_type]['name'])(**params['params'])
            return obj

    if pipeline_type in get_transformers():
        module = importlib.import_module(f'transformers.{pipeline_type}')
        if hasattr(module, loaders_params[pipeline_type]['name']):
            obj = getattr(module, loaders_params[pipeline_type]['name'])(**params['params'])
            return obj

    if pipeline_type in get_exporters():
        module = importlib.import_module(f'exporters.{pipeline_type}')
        if hasattr(module, loaders_params[pipeline_type]['name']):
            obj = getattr(module, loaders_params[pipeline_type]['name'])(**params['params'])
            return obj

    raise ImportError("Could not import the specified object!")
