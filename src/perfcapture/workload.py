import abc
import inspect
import pathlib
import subprocess

from perfcapture.dataset import Dataset
from perfcapture.metrics import MetricsForRun
from perfcapture.performance_counters import CounterManager
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
        """Must be overridden to implement the workload."""

    @property
    def name(self) -> str:
        """The name of this workload.
        
        Must be unique amongst all the workloads used in this benchmark suite.
        """
        return self.__class__.__name__
    
    @property
    def n_repeats(self) -> int:
        """The number of times to repeat this workload."""
        return 1


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
    workloads = []
    for py_filename in recipe_path.glob("*.py"):
        workloads_from_py_file = load_workloads_from_filename(py_filename)
        workloads.extend(workloads_from_py_file)
    return workloads
        

def run_workloads(
    workloads: list[Workload],
    keep_cache: bool
    ) -> dict[tuple[str, str], CounterManager]:
    all_timers: dict[tuple[str, str], CounterManager] = {}
    for workload in workloads:
        for dataset in workload.datasets:
            print(f"Running {workload.name} {workload.n_repeats} times on {dataset.name}!")
            perf_counters = CounterManager()
            for _ in range(workload.n_repeats):
                if not keep_cache:
                    p = subprocess.run(
                        ["vmtouch", "-e", dataset.path], capture_output=True, check=True)
                perf_counters.start_timing_run()
                metrics_for_run = workload.run(dataset_path=dataset.path)
                perf_counters.stop_timing_run(metrics_for_run)
            print(f"  Finished!\n{perf_counters}\n")
            all_timers[(workload.name, dataset.name)] = perf_counters
    return all_timers