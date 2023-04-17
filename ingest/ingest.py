"""
Original Authors: Chris/Nikolai
Hookup a book to the api by looking up its VID in database using ESTC no.
Called from the workflow.
"""
from csv import DictReader
import re
import json
import datetime
import requests
import subprocess
from .sheets.sheet import get_full_printer_name_for_short_name, \
    update_uuid_in_sheet_for_book_string, get_uuid_for_book_string_from_sheet
from .estc_search.estc import est_info_for_number
from .util import confirm

API_TOKEN_FILE_PATH = '/ocean/projects/hum160002p/shared/api/api_token.txt'
JSON_OUTPUT_PATH = '/ocean/projects/hum160002p/shared/ocr_results/json_output'
BOOKS_API_URL = 'https://printprobdb.psc.edu/api/books/'
BOOKS_URL = 'https://printprobdb.psc.edu/books'
CERT_PATH = '/ocean/projects/hum160002p/shared/api/incommonrsaserverca-bundle.crt'
BULK_LOAD_JSON_SCRIPT = '/ocean/projects/hum160002p/shared/books/code/ingest-book/ingest/bulk_load_json.py'
ESTC_LOOKUP_CSV = '/ocean/projects/hum160002p/shared/api/estc_vid_lookup.csv'
INIT_ENV_SCRIPT = '/ocean/projects/hum160002p/shared/books/code/ingest-book/init_env.sh'
ESTC_VALUES_WITH_MULTIPLE_BOOKS = ['S111228']


def _year_from_imprint_value(imprint_value):
    # match 4 digit year in the input string
    pattern_format_string = r'\d{4}'

    # create the re.Pattern object use re.compile function.
    reg_pattern = re.compile(pattern_format_string)

    result = reg_pattern.search(imprint_value)

    if result is None:
        print("Error: cannot identify 'year' from ESTC Imprint value", imprint_value)
        exit(-1)

    return result.group()  # return first match


def _load_token(path_to_token):
    with open(path_to_token) as f:
        token = f.read()
        token = token.rstrip()
    return token


def _build_headers(token):
    return {'Authorization': 'Token {}'.format(token)}


def _api_headers():
    token = _load_token(API_TOKEN_FILE_PATH)
    headers = _build_headers(token)
    return headers


def _get_vid_for_estc_number(estc_number):
    with open(ESTC_LOOKUP_CSV) as csvfile:
        reader = DictReader(csvfile)
        for row in reader:
            # check the arguments against the row
            if row['estcNO'] == estc_number:
                return dict(row).get('VID')


def _get_vid(estc_number_as_string) -> str:
    try:
        vid = _get_vid_for_estc_number(estc_number=estc_number_as_string)
        return vid
    except IndexError:
        print("It looks like that ESTC number may not be in our file?")


# For EEBO metadata case
def _update_dates(book):
    year_early = book['pq_year_early']
    if year_early is not None:
        book['date_early'] = datetime.datetime(int(year_early), 1, 1).strftime('%Y-%m-%d')
    year_late = book['pq_year_late']
    if year_late is not None:
        book['date_late'] = datetime.datetime(int(year_late), 12, 31).strftime('%Y-%m-%d')


def _retrieve_metadata(vid):
    headers = _api_headers()
    payload = {'vid': vid}
    r = requests.get(BOOKS_API_URL, headers=headers, params=payload, verify=CERT_PATH)
    result = r.json().get('results')
    if result is None or len(result) == 0:
        print('Error fetching metadata for VID -', vid)
        return None
    book = result[0]  # we assume that the first book that matches is the metadata we want, TODO: this may not be true.
    _update_dates(book)
    return book


def _existing_book_for_uuid(uuid):
    headers = _api_headers()
    try:
        r = requests.get(f'{BOOKS_API_URL}{uuid}/', headers=headers, verify=CERT_PATH)
        if r.status_code == 200 and r.headers['Content-Type'] == 'application/json':
            result = r.json()
            return result
        return None
    except requests.exceptions.HTTPError as err:
        print('Error fetching existing book for UUID: ', uuid, err)
        exit(0)


def _existing_books_for_estc(estc):
    payload = {'estc': estc}
    r = requests.get(BOOKS_API_URL, headers=_api_headers(), params=payload, verify=CERT_PATH)
    result = r.json().get('results')
    if result is None or len(result) == 0:
        return None
    return result


def _is_not_eebo_book(book):
    return bool(book.get('is_eebo_book') == False)


def _exactly_one_non_eebo_book(books):
    non_eebo_books = []
    for book in books:
        if _is_not_eebo_book(book):
            non_eebo_books.append(book)
    if len(non_eebo_books) > 1:
        print("We have multiple non-EEBO books, exiting. Please specify a UUID.")
        exit(0)
    return None if len(non_eebo_books) == 0 else non_eebo_books[0]


def _existing_book_has_no_characters(book):
    all_runs = book['all_runs']
    return len(all_runs['pages']) == 0 and len(all_runs['lines']) == 0 and len(all_runs['characters']) == 0


def _create_book(book, printer):
    payload = {
        # "id": None,
        "eebo": book.get('eebo'),
        "vid": book.get('vid'),
        "tcp": book.get('tcp', ''),  # Default to empty if not available
        "estc": book.get('estc'),
        "zipfile": "",
        "pp_publisher": book.get('pp_publisher'),
        "pp_author": book.get('pp_author'),
        "pq_publisher": book.get('pq_publisher'),
        "pq_title": book.get('pq_title'),
        "pq_url": book.get('pq_url', ''),  # Default to empty if not available
        "pq_author": book.get('pq_author'),
        "pq_year_verbatim": book.get('pq_year_verbatim'),
        "pq_year_early": book.get('pq_year_early'),
        "pq_year_late": book.get('pq_year_late'),
        "tx_year_early": book.get('tx_year_early'),
        "tx_year_late": book.get('tx_year_late'),
        "date_early": book.get('date_early'),
        "date_late": book.get('date_late'),
        "pdf": "",
        "starred": False,
        "ignored": False,
        "is_eebo_book": False,
        "prefix": None,
        "repository": "",
        "pp_printer": printer,
        "colloq_printer": "",
        "pp_notes": ""
    }
    # print(payload)
    r = requests.post(BOOKS_API_URL, headers=_api_headers(), json=payload, verify=CERT_PATH)
    return r.json()


def _get_book_data_from_estc(estc_number):
    estc_info = est_info_for_number(estc_number=estc_number)

    # Make sure we get the right data from ESTC, otherwise fail here
    assert estc_info.get("ESTC No.") == estc_number

    publisher_info = estc_info.get('Publisher Info')
    year = _year_from_imprint_value(publisher_info)
    print("Using year from ESTC as - ", year)

    first_day_of_year = datetime.datetime(int(year), 1, 1).strftime('%Y-%m-%d')
    last_day_of_year = datetime.datetime(int(year), 12, 31).strftime('%Y-%m-%d')

    book_metadata = {
        'estc': estc_number,
        'pp_publisher': publisher_info,
        'pp_author': estc_info.get('Author'),
        'pq_publisher': '',
        'pq_title': estc_info.get('Title'),
        'pq_author': estc_info.get('Author'),
        'pq_year_verbatim': year,
        'pq_year_early': year,
        'pq_year_late': year,
        'tx_year_early': year,
        'tx_year_late': year,
        'date_early': first_day_of_year,
        'date_late': last_day_of_year,
    }
    print('Using the following metadata to create the book - ', json.dumps(book_metadata, indent=4))
    # if not confirm('Continue with these details ?'):
    #     exit(0)
    return book_metadata


def _create_new_book_with_data(book_metadata, printer=None):
    response = _create_book(book_metadata, printer)
    print('Create book response from backend - ', response)
    return response['id']  # UUID of the book


# Create the batch command to ingest the book
def _create_bash_command(book_uuid, folder_name, update=False):
    batch_command_prefix = 'sbatch --dependency=singleton --job-name=IngestBookJob -c 10 --mem-per-cpu=1999mb ' \
                           '-p "RM-shared" -t 48:00:00'
    activate_virtual_env = 'source ~/.bashrc; source {init_env_script}'.format(init_env_script=INIT_ENV_SCRIPT)
    update_option = '-u' if update else ''
    command_to_run = 'python3 {BULK_LOAD_JSON_SCRIPT} {update_option} -b {book_uuid} ' \
                     '-j {JSON_OUTPUT_PATH}/{folder_name}_color'.format(BULK_LOAD_JSON_SCRIPT=BULK_LOAD_JSON_SCRIPT,
                                                                        book_uuid=book_uuid,
                                                                        JSON_OUTPUT_PATH=JSON_OUTPUT_PATH,
                                                                        folder_name=folder_name,
                                                                        update_option=update_option)
    return '{batch_command_prefix} --wrap="module load anaconda3; {activate_virtual_env}; {command_to_run}"' \
        .format(batch_command_prefix=batch_command_prefix,
                activate_virtual_env=activate_virtual_env,
                command_to_run=command_to_run)


# Lookup printer full name from the Google sheet 'Printers' worksheet
def _get_printer_name_from_sheet(printer_short_name):
    return get_full_printer_name_for_short_name(printer_short_name)


def run_command(book_string, preexisting_uuid, printer, update):
    # Folder name is same as the book string
    folder_name = book_string

    split_book_string = book_string.split('_')

    # ESTC number is the second element in the split book string
    estc_no = split_book_string[1]
    print("ESTC number - ", estc_no)

    if preexisting_uuid is None:
        # UUID of existing book for the ESTC that we are trying to update or overwrite
        # Lookup UUID in our sheet
        preexisting_uuid = get_uuid_for_book_string_from_sheet(book_string)
        if preexisting_uuid is not None and not preexisting_uuid.strip():
            preexisting_uuid = None  # No existing UUID
        else:
            print("Existing UUID from Google sheet - ", preexisting_uuid)

    # Specific UUID from commandline
    if preexisting_uuid is not None:
        book_uuid = preexisting_uuid
        existing_book = _existing_book_for_uuid(preexisting_uuid)
        if existing_book is None:
            print('No book found for given pre-existing UUID: ', preexisting_uuid)
            exit(0)
        print('We have an existing book with UUID: ', preexisting_uuid)
        # check if the book has an existing run or not
        no_characters_in_book = _existing_book_has_no_characters(existing_book)
        if no_characters_in_book:
            update = False  # we have nothing to update, we'll have to create a new run
            print(f'Existing book for UUID - {preexisting_uuid} has no runs yet.')
    else:
        target_book = None
        if estc_no not in ESTC_VALUES_WITH_MULTIPLE_BOOKS:
            existing_books = _existing_books_for_estc(estc_no)
            if existing_books is not None:
                non_eebo_existing_book = _exactly_one_non_eebo_book(existing_books)
                if non_eebo_existing_book is not None:
                    target_book = non_eebo_existing_book

        if target_book is not None:
            book_uuid = target_book['id']
            print('Found non-EEBO target book with id : ', book_uuid)
        else:
            update = False
            # VID lookup in the ESTC CSV
            vid = _get_vid(estc_no)

            # Try to retrieve metadata based on VID
            book_metadata = _retrieve_metadata(vid) if vid is not None else None

            if book_metadata is None:  # we do not have this book from EEBO
                print("We do not have this book's metadata from EEBO.")
                print("Getting book metadata using ESTC info lookup...")
                book_metadata = _get_book_data_from_estc(estc_number=estc_no)
                if book_metadata is None:
                    print("Failed to fetch book data from ESTC, maybe ESTC website is down? Check - http://estc.bl.uk/")
                    exit(0)

            # Use printer passed as argument, default to the fullname from
            # Google sheet or the short-name as last default
            book_printer = printer if printer is not None else _get_printer_name_from_sheet(split_book_string[0])
            # Create book in our backend
            book_uuid = _create_new_book_with_data(book_metadata, book_printer)
            # Update the book UUID in the Google sheet
            print("Book created with UUID: ", book_uuid)

    print("Updating UUID in Google sheet for book string", book_string, book_uuid)
    update_uuid_in_sheet_for_book_string(book_string, book_uuid)
    if update:
        print('Updating/overwriting an existing run for book with UUID: ', book_uuid)
    else:
        print('Creating a new run for the book with UUID: ', book_uuid)
    command = _create_bash_command(book_uuid, folder_name, update)
    print("ONCE COMPLETED, THIS BOOK WILL BE LOADED AT {BOOKS_URL}/{book_uuid}"
          .format(BOOKS_URL=BOOKS_URL, book_uuid=book_uuid))

    # subprocess.run(input=command)
    subprocess.run(command, shell=True)
    print("Job Launched")
