import abc
import pathlib

from perfcapture.utils import path_not_empty


class Dataset(abc.ABC):
    """Inherit from `Dataset` to implement a new benchmark dataset.
    
    Datasets are read by `Workload`s.
    """
    def __init__(self, base_data_path: pathlib.Path):
        self.path_to_dataset = base_data_path / self.name

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """The name of this dataset. Must be unique amongst all the datasets used in the benchmark suite."""
        pass

    @abc.abstractmethod
    def prepare(self) -> None:
        """Override this method if your workload needs to prepare a local dataset.
        
        Store your dataset at `self.path_to_dataset`.
        
        Every time the workload runner executes, it runs this pseudocode:

            if not dataset.already_exists():
                dataset.prepare()
        """
        pass

    def already_exists(self) -> bool:
        """Returns True if the dataset is already on disk."""
        path_is_dir_which_is_not_empty = (
            self.path_to_dataset.exists() and
            self.path_to_dataset.is_dir() and
            path_not_empty(self.path_to_dataset)
        )
        path_is_single_file = (
            self.path_to_dataset.exists() and
            not self.path_to_dataset.is_dir()
        )
        return path_is_dir_which_is_not_empty or path_is_single_file