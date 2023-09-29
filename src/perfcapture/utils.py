"""Simple utility functions."""

import importlib.util
import pathlib
import sys

def path_not_empty(path: pathlib.Path) -> bool:
    """Returns True if `path` is not empty."""
    # If `path` contains just a single entry then return True.
    # To save time, don't bother iterating past the first entry.
    for _ in path.iterdir():
        return True
    return False


def load_module_from_filename(py_filename: pathlib.Path):
    module_name = py_filename.stem
    spec = importlib.util.spec_from_file_location(module_name, py_filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
