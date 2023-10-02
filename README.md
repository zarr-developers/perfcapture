# perfcapture
Capture the performance of a computer system whilst running a set of benchmark workloads.
`perfcapture` is especially focused on benchmarking workloads which require a lot of IO.

Please note that `perfcapture` is a general-purpose benchmarking tool. Please see
[`zarr-benchmark`](https://github.com/zarr-developers/zarr-benchmark) for Zarr-specific benchmarks.
`zarr-benchmark` is built on top of `perfcapture`.
# Features

* Easily to define new workloads and datasets.
* Orchestrate the creation of on-disk datasets (for benchmarking against).
* (TODO) Parameterize workloads and dataset creation using a similar syntax to `pytest`.
* (TODO) For each workload, measure a range of performance metrics including:
  - total runtime
  - total bytes of IO
  - total IO operations
* (TODO) Save performance metrics to disk as JSON.

# Installation

1. Clone the repository.
2. Optionally create a virtual Python environment (e.g. with `python -m venv </path/to/venv/>`) and 
   activate that venv (`source </path/to/venv/>bin/activate`).
3. `pip install -e .`

# Usage

To run the examples:

```
~/dev/perfcapture$ mkdir -p ~/temp/perfcapture_data_path
~/dev/perfcapture$ python scripts/cli.py --data-path ~/temp/perfcapture_data_path --recipe-path examples
```