# perfcapture
Capture the performance of a computer system whilst running a set of benchmark workloads.

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