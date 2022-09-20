# ingest-book

## Pre-requisite

Make sure you have `poetry` installed - 

```shell
curl -sSL https://install.python-poetry.org | python3 -
```

## Step 1. Prepare environment

```shell
source init_env.sh
```

Verif that you are using the right Python version (`3.9.12`) and your conda env is created by the name of `env` - 

```shell
conda info
```

c)

```shell
poetry install
```

## Step 2. Run ingest script
```shell
./ingest_book.sh --book_string <book-string-value> --printer <optional-printer-full-name> --uuid <optional-existing-uuid>
```

Additionally, if you know that the book already exists and that you want to update the `existing run` for the book, you can add a `-u` or `--update` option - 

```shell
./ingest_book.sh --update --book_string <book-string-value> --printer <optional-printer-full-name> --uuid <optional-existing-uuid>
```
or
```shell
./ingest_book.sh -u --book_string <book-string-value> --printer <optional-printer-full-name> --uuid <optional-existing-uuid>
```

If the `-u` or `--update` option is not specified, we always assume that this is a `new run` - both in the case of a new book creation and 
in the case where there is an existing book. 