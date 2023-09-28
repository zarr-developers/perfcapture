#!/usr/bin/env python
import typer
from typing_extensions import Annotated
from typing import Optional

app = typer.Typer()


@app.command()
def bench(
    workloads: Annotated[Optional[str], typer.Argument()] = None,
    ):
    pass

if __name__ == "__main__":
    app()
