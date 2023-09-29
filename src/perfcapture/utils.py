import pathlib


def path_not_empty(path: pathlib.Path) -> bool:
    "Returns True if `path` is not empty."
    # If `path` contains just a single entry then return True.
    # To save time, don't bother iterating past the first entry.
    for _ in path.iterdir():
        return True
    return False