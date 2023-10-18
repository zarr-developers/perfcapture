from pathlib import Path

import numpy as np
from perfcapture.dataset import Dataset
from perfcapture.metrics import MetricsForRun
from perfcapture.workload import Workload


class NumpyDataset(Dataset):
    def create(self) -> None:
        """Create simple numpy file."""
        array = _create_numpy_array()
        with open(self.path, mode="wb") as fh:
            np.save(fh, array)
    

class ReadNumpyFile(Workload):
    def init_datasets(self) -> tuple[Dataset, ...]:
        return (NumpyDataset(), )
    
    def run(self, dataset_path: Path) -> MetricsForRun:
        """Load numpy file into RAM."""
        arr = np.load(dataset_path)
        return MetricsForRun(
            nbytes_in_final_array=arr.nbytes,
        )
        
    @property
    def n_runs(self) -> int:
        return 10


def _create_numpy_array() -> np.ndarray:
    """Generate an array of random numbers."""
    DTYPE = np.uint8
    low, high = np.iinfo(DTYPE).min, np.iinfo(DTYPE).max
    rng = np.random.default_rng()
    return rng.integers(low=low, high=high, size=(100, 100, 100, 100), dtype=DTYPE)