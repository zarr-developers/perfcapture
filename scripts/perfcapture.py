#!/usr/bin/env python
import pathlib
import typer
from typing_extensions import Annotated
from typing import Optional

app = typer.Typer()


@app.command()
def bench(
    data_path: Annotated[
        pathlib.Path,
        typer.Argument(help="The directory for storing the data which the benchmarks read from.")
    ],
    recipe_dir: Annotated[
        pathlib.Path,
        typer.Argument(help=(
            "The directory containing the code which defines the Workloads and Datasets."))
    ] = pathlib.Path("."),
    workloads: Annotated[
        Optional[str], 
        typer.Argument(help=(
            "Space-separated list of workload classes to run. If not set, all workloads found in"
            " recipe_dir will be run."))
        ] = None,
    keep_cache: Annotated[
        bool, 
        typer.Option(
            "--keep-cache",
            help="Set this flag to prevent `vmtouch -e` being called before each benchmark.",
            )
        ] = False,
    ) -> None:
    
    all_workloads = descover_workloads(recipe_dir)
    all_datasets = set([workload.dataset for workload in all_workloads])
    for dataset in all_datasets:
        if not dataset.already_exists():
            dataset.prepare(base_data_path=data_path)

if __name__ == "__main__":
    app()
