#!/usr/bin/env python
import typer
from typing_extensions import Annotated
from typing import Optional

app = typer.Typer()


@app.command()
def bench(
    workloads: Annotated[Optional[str], typer.Argument()] = None,
    do_not_clear_cache: Annotated[
        bool, 
        typer.Option(
            "--do-not-clear-cache",
            help="Set this flag to prevent `vmtouch -e` being called before each benchmark.",
            )
        ] = False,
    ):
    pass

if __name__ == "__main__":
    app()
