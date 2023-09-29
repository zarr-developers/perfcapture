import numpy as np
from perfcapture.dataset import Dataset

from perfcapture.workload import Workload


class NumpyDataset(Dataset):
    def prepare(self) -> None:
        """Create simple numpy file."""
        # Generate an array of random numbers
        rng = np.random.default_rng()
        DTYPE = np.uint8
        low, high = np.iinfo(DTYPE).min, np.iinfo(DTYPE).max
        array = rng.integers(
            low=low, 
            high=high, 
            size=(100, 100, 100, 100),
            dtype=DTYPE,
            )
        print("Created array", flush=True)
        
        # Save array to temporary file
        with open(self.path_to_dataset, mode="wb") as fh:
            np.save(fh, array)    
    

class ReadNumpyFile(Workload):
    def run(self):
        """Load numpy file into RAM."""
        for _ in range(100):
            np.load(self.dataset.path_to_dataset)
