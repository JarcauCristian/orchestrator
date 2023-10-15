from typing import Tuple, List, Any
from pathlib import Path


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
    cwd = Path("../loaders/")
    for path in cwd.glob("*.py"):
        last_part = path.parts[-1]
        if 'loader' in last_part:
            loaders.append(last_part.split('.')[0])

    return loaders


def get_transformers():
    transformers = []
    cwd = Path("../transformers/")
    for path in cwd.glob("*.py"):
        last_part = path.parts[-1]
        if 'transformer' in last_part:
            transformers.append(last_part.split('.')[0])

    return transformers


def get_exporters():
    exporters = []
    cwd = Path("../exporters/")
    for path in cwd.glob("*.py"):
        last_part = path.parts[-1]
        if 'exporter' in last_part:
            exporters.append(last_part.split('.')[0])

    return exporters
