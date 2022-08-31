# ingest-book

## Pre-requisite

Make sure you have `poetry` installed - 

```shell
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 -
```

## Step 1. Prepare environment

```shell
source init_env.sh
```

## Step 2. Run ingest script
```shell
./ingest_book.sh --book_string <book-string-value> --printer <optional-printer-full-name> --uuid <optional-existing-uuid>
```
