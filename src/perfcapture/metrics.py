from dataclasses import dataclass


@dataclass
class MetricsForRun:
    """Record metrics for each run."""
    nbytes_in_final_array: int