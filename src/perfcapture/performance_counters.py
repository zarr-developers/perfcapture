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
    def __init__(self) -> None:
        self._data_per_run = pd.Series(name=self.name)
        
    def start_timing_run(self) -> None:
        pass
        
    @abc.abstractmethod
    def stop_timing_run(self, metrics_for_run: MetricsForRun) -> None:
        pass

    @property
    def name(self) -> str:
        return self.__class__.__name__
    
    def __str__(self) -> str:
        return _NAME_MEAN_STD_STRING.format(
            self.name,
            self._data_per_run.mean(),
            self._data_per_run.std(),
        )


@dataclass
class CounterManager:
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
    
    def __str__(self) -> str:
        return "".join(["{}".format(counter) for counter in self.counters])


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
            ('read_iops', 'write_iops', 'read_bytes_per_sec', 'write_bytes_per_sec')
        )
        self._data_per_run = pd.DataFrame(dtype=np.int64, columns=columns)
    
    def start_timing_run(self) -> None:
        self._disk_counters_at_start_of_run = self._get_disk_io_counters_as_series()
        
    def stop_timing_run(self, metrics_for_run: MetricsForRun) -> None:
        disk_io_counters_at_end_of_run = self._get_disk_io_counters_as_series()
        disk_io_counters_diff = (
            disk_io_counters_at_end_of_run - self._disk_counters_at_start_of_run)
        
        # Compute counters which depend on runtime:
        total_secs = metrics_for_run.total_secs
        disk_io_counters_diff['read_iops'] = disk_io_counters_diff['read_count'] / total_secs
        disk_io_counters_diff['write_iops'] = disk_io_counters_diff['write_count'] / total_secs
        disk_io_counters_diff['read_bytes_per_sec'] = (
            disk_io_counters_diff['read_bytes'] / total_secs)
        disk_io_counters_diff['write_bytes_per_sec'] = (
            disk_io_counters_diff['write_bytes'] / total_secs)
        
        self._data_per_run.loc[metrics_for_run.run_id] = disk_io_counters_diff
    
    @classmethod
    def _get_disk_io_counters_as_series(self) -> pd.Series:
        counters: namedtuple = psutil.disk_io_counters()
        return pd.Series(counters._asdict())
    
    @property
    def name(self) -> str:
        return "Disk IO"
    
    def __str__(self) -> str:
        string = ""
        for col_name in ["read_iops", "write_iops", "read_bytes_per_sec", "write_bytes_per_sec"]:
            data = self._data_per_run[col_name]
            
            if "bytes" in col_name:
                col_name = col_name.replace("bytes", "GB")
                data = data / 1E9
            
            string += _NAME_MEAN_STD_STRING.format(col_name, data.mean(), data.std())
        
        return string


class _BasicTimer:
    def __init__(self) -> None:
        self._time_at_start = datetime.now()
        
    def total_secs_elapsed(self) -> float:
        duration = datetime.now() - self._time_at_start
        return duration.total_seconds()