import abc
import inspect
import pathlib
import subprocess

import pandas as pd

from perfcapture.dataset import Dataset
from perfcapture.metrics import MetricsForRun
from perfcapture.performance_counters import PerfCounterManager
from perfcapture.utils import load_module_from_filename, path_not_empty


class Workload(abc.ABC):
    """Inherit from `Workload` to implement a new benchmark workload."""
    
    def __init__(self):
        self.datasets = self.init_datasets()
        
    @abc.abstractmethod
    def init_datasets(self) -> tuple[Dataset, ...]:
        """Initialises and returns a tuple of concrete Dataset objects.
        
        Use this method to assign Dataset objects to this Workload.
        """

    @abc.abstractmethod
    def run(self, dataset_path: pathlib.Path) -> MetricsForRun:
        """Run this workload once against a specific dataset.
        
        Must be overridden to implement the workload.
        """

    @property
    def name(self) -> str:
        """Return the name of this workload.
        
        Must be unique amongst all the workloads used in this benchmark suite.
        """
        return self.__class__.__name__
    
    @property
    def n_runs(self) -> int:
        """The number of times to repeat this workload."""
        return 3


def load_workloads_from_filename(py_filename: pathlib.Path) -> list[Workload]:
    workloads = []
    module = load_module_from_filename(py_filename)
    for member_name in dir(module):
        module_attr = getattr(module, member_name)
        if (module_attr 
            and inspect.isclass(module_attr) 
            and issubclass(module_attr, Workload) 
            and module_attr is not Workload
            ):
            print(f"Instantiating {member_name}")
            workload_obj = module_attr()
            workloads.append(workload_obj)
    return workloads

    
def discover_workloads(recipe_path: pathlib.Path) -> list[Workload]:    
    # First, load any dataset modules, so they're available for the workload modules:
    for py_filename in recipe_path.glob("*dataset*.py"):
        workloads_from_py_file = load_workloads_from_filename(py_filename)

    workloads = []
    for py_filename in recipe_path.glob("*.py"):
        workloads_from_py_file = load_workloads_from_filename(py_filename)
        workloads.extend(workloads_from_py_file)
    return workloads
        

def run_workloads(
    workloads: list[Workload],
    keep_cache: bool
    ) -> pd.DataFrame:
    all_results = []
    for workload in workloads:
        for dataset in workload.datasets:
            print(f"Running {workload.name} {workload.n_runs} times on {dataset.name}!")
            perf_counter = PerfCounterManager(dataset.path)
            for i in range(workload.n_runs):
                print(f"Run {i+1} of {workload.n_runs}...")
                if not keep_cache:
                    p = subprocess.run(
                        ["vmtouch", "-e", dataset.path], capture_output=True, check=True)
                perf_counter.start_timing_run()
                metrics_for_run = workload.run(dataset_path=dataset.path)
                perf_counter.stop_timing_run(metrics_for_run)
            print(f"Finished!\n{perf_counter}\n")

            # Store results
            results = perf_counter.get_results().reset_index()
            results['workload'] = workload.name
            results['dataset'] = dataset.name
            all_results.append(results)
    return pd.concat(all_results).set_index(["workload", "dataset", "run_ID"])
