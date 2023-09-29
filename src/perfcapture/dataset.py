import abc
import pathlib

from perfcapture.utils import path_not_empty


class Dataset(abc.ABC):
    """Inherit from `Dataset` to implement a new benchmark dataset.
    
    Datasets are read by `Workload`s.
    """
    def set_path(self, base_data_path: pathlib.Path):
        self.path = base_data_path / self.name

    @property
    def name(self) -> str:
        """The name of this dataset.
        
        Must be unique amongst all the datasets used in the benchmark suite.
        """
        return self.__class__.__name__

    @abc.abstractmethod
    def prepare(self) -> None:
        """Override this method if your workload needs to prepare a local dataset.
        
        Store your dataset at `self.path`.
        
        Every time the workload runner executes, it runs this pseudocode:

            if not dataset.already_exists():
                dataset.prepare()
        """
        pass

    def already_exists(self) -> bool:
        """Returns True if the dataset is already on disk."""
        path_is_dir_which_is_not_empty = (
            self.path.exists() and
            self.path.is_dir() and
            path_not_empty(self.path)
        )
        path_is_single_file = (
            self.path.exists() and
            not self.path.is_dir()
        )
        return path_is_dir_which_is_not_empty or path_is_single_file