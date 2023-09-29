import abc
import pathlib
from perfcapture.dataset import Dataset

from perfcapture.utils import path_not_empty


class Workload(abc.ABC):
    """Inherit from `Workload` to implement a new benchmark workload."""
    
    def __init__(self, dataset: Dataset):
        self.dataset = dataset

    @abc.abstractmethod
    def run(self) -> dict[str, object]:
        """Must be overridden to implement the workload."""
