import click
from .ingest import run_command


@click.command()
@click.option("--book_string", help="Book string", required=True)
@click.option("--uuid", help="Existing book UUID", default=None, required=False)
@click.option("--printer", help="Printer name", default=None, required=False)
def main(book_string, uuid, printer):
    run_command(book_string, uuid, printer)


if __name__ == "__main__":
    main()
