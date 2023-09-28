import os
import tempfile

import numpy as np


class ReadNumpyFile:  # TODO: Should inherit from Workload ABC.
    def before_each_run(self):
        """Create simple numpy file in a temp directory."""
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
        fh, self.filename = tempfile.mkstemp()
        os.close(fh)
        with open(self.filename, mode="wb") as fh:
            np.save(fh, array)
    
    def run(self):
        """Load numpy file into RAM."""
        for _ in range(100):
            np.load(self.filename)
    
    def after_each_run(self):
        """Remove numpy file from temp directory."""
        if os.path.isfile(self.filename):
            os.remove(self.filename)