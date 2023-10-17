from datetime import datetime

import numpy as np

from perfcapture.metrics import MetricsForRun


class Timer:
    """Simple timer."""
    def __init__(self):
        self.seconds_per_run: list[float] = []
        self.gigabytes_per_second_per_run: list[float] = []
    
    def start_timing_run(self) -> None:
        self.time_at_start_of_run = datetime.now()
        
    def stop_timing_run(self, metrics_for_run: MetricsForRun) -> None:
        now = datetime.now()
        duration = now - self.time_at_start_of_run
        total_seconds = duration.total_seconds()
        self.seconds_per_run.append(total_seconds)
        
        bytes_per_sec = metrics_for_run.nbytes_in_final_array / total_seconds
        gigabytes_per_sec = bytes_per_sec / 1E9
        self.gigabytes_per_second_per_run.append(gigabytes_per_sec)
    
    def __str__(self) -> str:
        return "  Runtime: mean = {:.3f} seconds; std = {:.3f}".format(
            np.mean(self.seconds_per_run),
            np.std(self.seconds_per_run)
        ) + "\n  Bandwidth to numpy array: mean = {:.3f} GB/s; std = {:.3f}".format(
            np.mean(self.gigabytes_per_second_per_run),
            np.std(self.gigabytes_per_second_per_run),
        )