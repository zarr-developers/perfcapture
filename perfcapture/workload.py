import abc
import pathlib

from perfcapture.utils import path_not_empty


class Workload(abc.ABC):
    def __init__(self, path_to_dataset: pathlib.Path):
        self.path_to_dataset = path_to_dataset
    
    def prepare_dataset(self) -> None:
        """Override this method if your workload needs to prepare a local dataset.
        
        The workload runner will call this method every time the workload runner runs.
        If your dataset takes a long time to prepare then leave your disk,
        and start your `prepare_dataset` method with a test to see if the dataset
        already exists on disk.
        
        Store your dataset at `self.path_to_dataset`.
        """
        pass
    
    @abc.abstractmethod
    def run_workload(self) -> dict[str, object]:
        """Must be overridden. This method implements the workload.
        """

    def dataset_already_exists(self) -> bool:
        """Returns True if the dataset is already on disk.
        """
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
