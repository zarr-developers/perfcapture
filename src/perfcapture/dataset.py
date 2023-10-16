import abc
import pathlib

from perfcapture.utils import path_not_empty


class Dataset(abc.ABC):
    """Inherit from `Dataset` to implement a new benchmark dataset.
    
    Datasets are read by `Workload`s.
    """
    @abc.abstractmethod
    def create(self) -> None:
        """Override this method to define a new local dataset.
        
        Store your dataset at `self.path`.
        
        Every time the workload runner executes, it runs this pseudocode:

            if not dataset.already_exists():
                dataset.create()
        """

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
    
    @property
    def name(self) -> str:
        """The name of this dataset.
        
        Must be unique amongst all the datasets used in the benchmark suite.
        
        The default implementation will use the name of the class. Override
        this method if you wish to set a custom name.
        """
        return self.__class__.__name__

    def set_path(self, base_data_path: pathlib.Path) -> None:
        self._path = base_data_path / self.name
        
    @property
    def path(self) -> pathlib.Path:
        try:
            return self._path
        except Exception as e:
            e.add_note("Run `Dataset.set_path()` before attempting to access `Dataset.path`!")
            raise


def create_datasets_if_necessary(workloads: list, data_path: pathlib.Path) -> None:
    all_datasets = set()
    for workload in workloads:
        all_datasets.update(workload.datasets)
    print(f"Found {len(all_datasets)} Dataset object(s).")
    for dataset in all_datasets:
        dataset.set_path(data_path)
        if dataset.already_exists():
            print(f"{dataset.name} already exists.")
        else:
            print(f"Creating dataset for {dataset.name}")
            dataset.create()
