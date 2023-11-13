import abc
import pathlib
from collections import namedtuple
from dataclasses import InitVar, dataclass, field
from datetime import datetime

import numpy as np
import pandas as pd
import psutil

from perfcapture.metrics import MetricsForRun

_NAME_MEAN_STD_STRING = "{:>18}: mean = {:>9.3f}; std = {:>9.3f}\n"


class _PerfCounterABC(abc.ABC):
    """PerfCounterABC subclasses define a single performance counter.
    
    Usage:
    1. Init PerfCounter subclass when we start benchmarking a specific
       combination of <Workload> and <Dataset>.
    2. Set `PerfCounter.dataset_path` if this perf counter needs to know the dataset path.
    3. Call `start_timing_iteration()` at the start of each iteration.
    4. Call `stop_timing_iteration()` at the end of each iteration.
    5. Call `get_results()` at the end of the `run`, to get a `pd.DataFrame` of results.
    """
    def __init__(self) -> None:
        self._data_per_run = pd.DataFrame(columns=[self.name])
        self._data_per_run.index.name = "run_ID"
        self._dataset_path: pathlib.Path | None = None
        
    def start_timing_run(self) -> None:
        pass

    @abc.abstractmethod
    def stop_timing_run(self, metrics_for_iteration: MetricsForRun) -> None:
        pass
    
    def get_results(self) -> pd.DataFrame:
        """Return a DataFrame where columns are counters, and rows are runs."""
        return self._data_per_run

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def dataset_path(self) -> pathlib.Path:
        return self._dataset_path

    @dataset_path.setter
    def dataset_path(self, dataset_path: pathlib.Path) -> None:
        self._dataset_path = dataset_path

@dataclass
class PerfCounterManager:
    """Simple manager for multiple performance counters."""
    dataset_path: InitVar[pathlib.Path]
    counters: list[_PerfCounterABC] = field(
        default_factory=lambda: [Runtime(), BandwidthToNumpy(), DiskIO()]
        )
    
    def __post_init__(self, dataset_path: pathlib.Path) -> None:
        self._run_id: int = 0
        for counter in self.counters:
            counter.dataset_path = dataset_path

    def start_timing_run(self) -> None:
        self._run_id += 1
        [counter.start_timing_run() for counter in self.counters]
        self._timer = _BasicTimer()
        
    def stop_timing_run(self, metrics_for_run: MetricsForRun) -> None:
        # Many counters require a runtime. So compute the runtime once, here.
        metrics_for_run.total_secs = self._timer.total_secs_elapsed()
        metrics_for_run.run_id = self._run_id
        [counter.stop_timing_run(metrics_for_run) for counter in self.counters]
    
    def get_results(self) -> pd.DataFrame:
        return pd.concat(
            [counter.get_results() for counter in self.counters],
            axis="columns",
            )
        
    def get_summary_of_results(self) -> pd.DataFrame:
        results = self.get_results()
        return pd.DataFrame({'mean': results.mean(), 'std': results.std()})
    
    def __str__(self) -> str:
        return str(self.get_summary_of_results())


class Runtime(_PerfCounterABC):
    def stop_timing_run(self, metrics_for_run: MetricsForRun) -> None:
        self._data_per_run.loc[metrics_for_run.run_id] = metrics_for_run.total_secs
        
    @property
    def name(self) -> str:
        return "Runtime in secs"


class BandwidthToNumpy(_PerfCounterABC):
    def stop_timing_run(self, metrics_for_run: MetricsForRun) -> None:
        bytes_per_sec = metrics_for_run.nbytes_in_final_array / metrics_for_run.total_secs
        gigabytes_per_sec = bytes_per_sec / 1E9
        self._data_per_run.loc[metrics_for_run.run_id] = gigabytes_per_sec
        
    @property
    def name(self) -> str:
        return "GB/sec to numpy"


class DiskIO(_PerfCounterABC):
    """Record performance of disks.
    
    Note that this records performance of the specific disk used
    to store the benchmark datasets, but we record the activity of
    all processes. So, if other processes are using this disk then
    you'll get misleading results!
    
    For more information on the fields recorded, please see:
    - https://psutil.readthedocs.io/en/latest/#psutil.disk_io_counters
    - https://www.kernel.org/doc/Documentation/iostats.txt
    """
    def __init__(self) -> None:
        columns = (
            # See:
            # https://psutil.readthedocs.io/en/latest/#psutil.disk_io_counters
            # https://www.kernel.org/doc/Documentation/iostats.txt
            psutil._pslinux.sdiskio._fields + # superset of _common.sdiskio._fields
            ("read_IOPS", "write_IOPS", "avg read GB/sec", "avg write GB/sec",
             "read GB / read_time_secs", "write GB / write_time_secs",
             "read GB", "write GB",
             "read_time_secs", "write_time_secs", "busy_time_secs")
        )
        columns = tuple(
            col for col in columns if col not in 
            ("read_bytes", "write_bytes", "read_time", "write_time", "busy_time"))
        self._data_per_run = pd.DataFrame(dtype=np.int64, columns=columns)
        self._data_per_run.index.name = "run_ID"

    @property
    def dataset_path(self) -> pathlib.Path:
        return super().dataset_path()

    @dataset_path.setter
    def dataset_path(self, dataset_path: pathlib.Path) -> None:
        self._dataset_path = dataset_path
        self._dataset_partition_name = _get_partition_name_from_path(dataset_path)
        print("dataset_partition_name =", self._dataset_partition_name)

    def start_timing_run(self) -> None:
        self._disk_counters_at_start_of_run = self._get_disk_io_counters_as_series()
        
    def stop_timing_run(self, metrics_for_run: MetricsForRun) -> None:
        disk_io_counters_at_end_of_run = self._get_disk_io_counters_as_series()
        count_diff = disk_io_counters_at_end_of_run - self._disk_counters_at_start_of_run

        # Convert bytes to gigabytes, and milliseconds to secs:
        for direction in ("read", "write"):
            count_diff[f"{direction} GB"] = count_diff[f"{direction}_bytes"] / 1E9
            count_diff[f"{direction}_time_secs"] = count_diff[f"{direction}_time"] / 1E3
            del count_diff[f"{direction}_bytes"]
            del count_diff[f"{direction}_time"]
        if "busy_time" in count_diff:
            count_diff[f"busy_time_secs"] = count_diff[f"busy_time"] / 1E3
            del count_diff["busy_time"]

        # Compute {read,write} GB / {read,write}_time(secs)
        for direction in ("read", "write"):
            # Protect against divide-by-zero (if <direction>_time is zero):
            new_key = f"{direction} GB / {direction}_time_secs"
            if count_diff[f"{direction}_time_secs"] > 0:
                count_diff[new_key] = (
                    count_diff[f"{direction} GB"] / count_diff[f"{direction}_time_secs"])
            else:
                count_diff[new_key] = 0

        # Compute counters which depend on runtime:
        total_secs = metrics_for_run.total_secs
        count_diff["read_IOPS"] = count_diff["read_count"] / total_secs
        count_diff["write_IOPS"] = count_diff["write_count"] / total_secs
        count_diff["avg read GB/sec"] = count_diff["read GB"] / total_secs
        count_diff["avg write GB/sec"] = count_diff["write GB"] / total_secs

        self._data_per_run.loc[metrics_for_run.run_id] = count_diff
    
    def _get_disk_io_counters_as_series(self) -> pd.Series:
        counters: dict[str, namedtuple] = psutil.disk_io_counters(perdisk=True)
        counters: namedtuple = counters[self._dataset_partition_name]
        return pd.Series(counters._asdict())
    
    @property
    def name(self) -> str:
        return "Disk IO"
    
    def get_results(self) -> pd.DataFrame:
        return self._data_per_run


class _BasicTimer:
    def __init__(self) -> None:
        self._time_at_start = datetime.now()
        
    def total_secs_elapsed(self) -> float:
        duration = datetime.now() - self._time_at_start
        return duration.total_seconds()


def _get_partition_name_from_path(dataset_path: pathlib.Path) -> str:
    dataset_mount_point = _get_mount_point_from_path(dataset_path)
    partitions: list[psutil._common.sdiskpart] = psutil.disk_partitions()
    for partition in partitions:
        if pathlib.Path(partition.mountpoint) == dataset_mount_point:
            return pathlib.Path(partition.device).resolve().parts[-1]
    raise RuntimeError(f"Could not find partition for {dataset_path}")
            
        
def _get_mount_point_from_path(p: pathlib.Path) -> pathlib.Path:
    if len(p.parts) == 0:
        raise RuntimeError(f"Path '{p}' should not be empty!")
    elif p.is_mount():
        return p
    else:
        return _get_mount_point_from_path(p.parent)