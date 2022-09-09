import click
from .ingest import run_command


@click.command()
@click.option("--book_string", help="Book string", required=True)
@click.option("--uuid", help="Existing book UUID", default=None, required=False)
@click.option("--printer", help="Printer name", default=None, required=False)
@click.option('--update', '-u', is_flag=True, help="Update existing book")
@click.option('--overwrite', '-w', is_flag=True, help="Overwrite existing book")
def main(book_string, uuid, printer, update, overwrite):
    run_command(book_string, uuid, printer, update, overwrite)


if __name__ == "__main__":
    main()
