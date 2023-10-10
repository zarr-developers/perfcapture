import numpy as np
from perfcapture.dataset import Dataset
from perfcapture.workload import Workload


class NumpyDataset(Dataset):
    def create(self) -> None:
        """Create simple numpy file."""
        array = _create_numpy_array()
        with open(self.path, mode="wb") as fh:
            np.save(fh, array)
    

class ReadNumpyFile(Workload):
    def init_datasets(self) -> Dataset:
        return NumpyDataset()
    
    def run(self):
        """Load numpy file into RAM."""
        np.load(self.dataset.path)
        
    @property
    def n_repeats(self) -> int:
        return 10


def _create_numpy_array() -> np.ndarray:
    """Generate an array of random numbers."""
    DTYPE = np.uint8
    low, high = np.iinfo(DTYPE).min, np.iinfo(DTYPE).max
    rng = np.random.default_rng()
    return rng.integers(low=low, high=high, size=(100, 100, 100, 100), dtype=DTYPE)