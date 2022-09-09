import click
from .ingest import run_command


@click.command()
@click.option("--book_string", help="Book string", required=True)
@click.option("--uuid", help="Existing book UUID", default=None, required=False)
@click.option("--printer", help="Printer name", default=None, required=False)
@click.option('--update', '-u', is_flag=True, help="Update/overwrite existing book run")
def main(book_string, uuid, printer, update):
    run_command(book_string, uuid, printer, update)


if __name__ == "__main__":
    main()
