import abc
from collections import namedtuple
from dataclasses import dataclass, field
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
    2. Call `start_timing_iteration()` at the start of each iteration.
    3. Call `stop_timing_iteration()` at the end of each iteration.
    4. Call `get_results()` at the end of the `run`, to get a `pd.DataFrame` of results.
    """
    def __init__(self) -> None:
        self._data_per_run = pd.DataFrame(columns=[self.name])
        self._data_per_run.index.name = "run_ID"
        
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


@dataclass
class PerfCounterManager:
    """Simple manager for multiple performance counters."""
    counters: list[_PerfCounterABC] = field(
        default_factory=lambda: [Runtime(), BandwidthToNumpy(), DiskIO()]
        )
    
    def __post_init__(self) -> None:
        self._run_id: int = 0

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
    def __init__(self) -> None:
        columns = (
            psutil._pslinux.sdiskio._fields + # superset of _common.sdiskio._fields
            ('read_IOPS', 'write_IOPS', 'read GB/sec from disk', 'write GB/sec to disk',
             'read GB', 'write GB')
        )
        columns = tuple(col for col in columns if col not in ('read_bytes', 'write_bytes'))
        self._data_per_run = pd.DataFrame(dtype=np.int64, columns=columns)
        self._data_per_run.index.name = "run_ID"
    
    def start_timing_run(self) -> None:
        self._disk_counters_at_start_of_run = self._get_disk_io_counters_as_series()
        
    def stop_timing_run(self, metrics_for_run: MetricsForRun) -> None:
        disk_io_counters_at_end_of_run = self._get_disk_io_counters_as_series()
        disk_io_counters_diff = (
            disk_io_counters_at_end_of_run - self._disk_counters_at_start_of_run)
        
        # Convert bytes to GB:
        disk_io_counters_diff['read GB'] = disk_io_counters_diff['read_bytes'] / 1E9
        disk_io_counters_diff['write GB'] = disk_io_counters_diff['write_bytes'] / 1E9
        del disk_io_counters_diff['read_bytes']
        del disk_io_counters_diff['write_bytes']

        # Compute counters which depend on runtime:
        total_secs = metrics_for_run.total_secs
        disk_io_counters_diff['read_IOPS'] = disk_io_counters_diff['read_count'] / total_secs
        disk_io_counters_diff['write_IOPS'] = disk_io_counters_diff['write_count'] / total_secs
        disk_io_counters_diff['read GB/sec from disk'] = (
            disk_io_counters_diff['read GB'] / total_secs)
        disk_io_counters_diff['write GB/sec to disk'] = (
            disk_io_counters_diff['write GB'] / total_secs)

        self._data_per_run.loc[metrics_for_run.run_id] = disk_io_counters_diff
    
    @classmethod
    def _get_disk_io_counters_as_series(cls) -> pd.Series:
        counters: namedtuple = psutil.disk_io_counters()
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