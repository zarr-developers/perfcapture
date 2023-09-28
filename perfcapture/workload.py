import abc
import pathlib

from perfcapture.utils import path_not_empty


class Workload(abc.ABC):
    """To implement a new benchmark workload, inherit from `Workload`.
    
    Most folks will want to override just two methods:
    
    - prepare_dataset
    - run_workload
    """
    
    def __init__(self, path_to_dataset: pathlib.Path):
        self.path_to_dataset = path_to_dataset
    
    def prepare_dataset(self) -> None:
        """Override this method if your workload needs to prepare a local dataset.
        
        Every time the workload runner executes, it runs this pseudocode
        before calling `run_workload`:

            if not workload.dataset_already_exists():
                workload.prepare_dataset()

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
