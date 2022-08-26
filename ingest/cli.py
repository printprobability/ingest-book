import click
from .ingest import run_command


@click.command()
@click.option("--book_string", prompt="book_string", help="Book string", required=True)
@click.option("--uuid", prompt="uuid", help="Existing book UUID", default=None, required=False)
@click.option("--printer", prompt="printer", help="Printer name", required=False)
def main(book_string, uuid, printer):
    run_command(book_string, uuid, printer)


if __name__ == "__main__":
    main()
