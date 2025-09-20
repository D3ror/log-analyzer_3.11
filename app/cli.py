# app/cli.py
import typer
from app.parse import parse_file_to_parquet

cli = typer.Typer()


@cli.command()
def preproc(in_file: str, out_file: str = "logs.parquet"):
    """Parse raw log file into a Parquet file."""
    parse_file_to_parquet(in_file, out_file)
    typer.echo(f"✅ Parsed {in_file} -> {out_file}")


if __name__ == "__main__":
    cli()
