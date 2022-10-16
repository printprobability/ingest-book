"""
Script to load JSON-formatted outputs from Ocular into the P&P REST API.
"""

import requests
import json
import logging
from glob import glob
import optparse
import re
import concurrent.futures


AUTH_TOKEN = open("/ocean/projects/hum160002p/shared/api/api_token.txt", "r").read().strip()
AUTH_HEADER = {"Authorization": f"Token {AUTH_TOKEN}"}
PP_URL = "https://printprobdb.psc.edu/api"
CERT_PATH = "/ocean/projects/hum160002p/shared/api/incommonrsaserverca-bundle.crt"



class CharacterClasses:
    """
    Utility to create a hashmap of Ocular character codes to database IDs, so that we don't need to check every single time
    """

    data = {}

    def load_character_classes(self):
        """
        Create a dict of all currently-loaded character classes
        """
        cc_res = requests.get(
            f"{PP_URL}/character_classes/",
            params={"limit": 500},
            headers=AUTH_HEADER,
            verify=CERT_PATH,
        )
        if cc_res.status_code == 200:
            for cc in cc_res.json()["results"]:
                self.data[cc["classname"]] = cc["classname"]
            logging.info(self.data)
        else:
            raise Exception(cc_res.content)

    def get_or_create(self, ocular_code):
        if ocular_code == "":
            ocular_code = "space"
        elif ocular_code == ".":
            ocular_code = "period"
        elif ocular_code == ";":
            ocular_code = "semicolon"
        elif ocular_code == "/":
            ocular_code = "slash"
        elif ocular_code == "\\":
            ocular_code = "backslash"
        try:
            return self.data[ocular_code]
        except:
            cc_res = requests.post(
                f"{PP_URL}/character_classes/",
                json={"classname": ocular_code, "label": ocular_code},
                headers=AUTH_HEADER,
                verify=CERT_PATH,
            )
            if cc_res.status_code == 201:
                logging.info(f"{ocular_code} created")
                self.data[ocular_code] = ocular_code
                return ocular_code
            else:
                raise Exception(cc_res.content)


class BookLoader:
    def __init__(self, book_id, json_directory, update=False):
        self.book_id = book_id
        self.json_directory = json_directory
        self.update = update
        self.cc = CharacterClasses()
        self.cc.load_character_classes()

    def load_db(self):
        self.confirm_book()
        self.load_json()
        if self.update:
            self.update_pages()
            self.update_lines()
            self.update_characters()
        else:
            self.create_pages()
            self.create_lines()
            self.create_characters()

    def confirm_book(self):
        """
        Confirm that the book actually exists on Bridges
        """
        res = requests.get(
            f"{PP_URL}/books/{self.book_id}/", headers=AUTH_HEADER, verify=CERT_PATH
        )
        if res.status_code != 200:
            logging.info(res.content)
            raise Exception(
                f"The book {self.book_id} is not yet registered in the database. Please confirm you have used the correct UUID."
            )

    def divide_into_chunks(self, data, chunk_size=20):
        # looping till length l
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    def load_json(self):
        self.pages = json.load(open(f"{self.json_directory}/pages.json", "r"))["pages"]
        # Add a "side" to every page
        for page in self.pages:
            page["side"] = "s"
        logging.info(f"{len(self.pages)} pages loaded")
        self.lines = json.load(open(f"{self.json_directory}/lines.json", "r"))["lines"]
        logging.info(f"{len(self.lines)} lines loaded")
        self.characters = json.load(open(f"{self.json_directory}/chars.json", "r"))[
            "chars"
        ]
        # Normalize characters
        for character in self.characters:
            character["character_class"] = self.cc.get_or_create(
                character["character_class"]
            )
        logging.info(f"{len(self.characters)} characters loaded")

    def create_character_run(self):
        character_run_create_response = requests.post(
            f"{PP_URL}/runs/characters/",
            json={"book": self.book_id},
            headers=AUTH_HEADER,
            verify=CERT_PATH,
        )
        json_response = character_run_create_response.json()
        logging.info("Character run created with id: " + json_response['id'])
        return json_response

    def get_character_run(self, id):
        character_run_get_response = requests.get(
            f"{PP_URL}/runs/characters/${id}",
            headers=AUTH_HEADER,
            verify=CERT_PATH,
        )
        json_response = character_run_get_response.json()
        logging.info("Got character run ith id: " + json_response['id'])
        return json_response

    def create_pages(self):
        bulk_page_response = requests.post(
            f"{PP_URL}/books/{self.book_id}/bulk_pages/",
            json={"pages": self.pages, "tif_root": "/ocean/projects/hum160002p/shared"},
            headers=AUTH_HEADER,
            verify=CERT_PATH,
        )
        logging.info(bulk_page_response.content)

    def create_lines(self):
        bulk_line_response = requests.post(
            f"{PP_URL}/books/{self.book_id}/bulk_lines/",
            json={"lines": self.lines},
            headers=AUTH_HEADER,
            verify=CERT_PATH,
        )
        logging.info(bulk_line_response.content)

    def create_characters(self):
        character_run = self.create_character_run()
        character_run_id = character_run['id']
        worker_size = 20
        book_id = self.book_id
        character_list = self.characters
        chunks = list(self.divide_into_chunks(character_list, int(round(len(character_list) / worker_size))))
        logging.info({"Total number of characters to be added": len(character_list)})
        logging.info({"Number of chunks for characters": len(chunks)})
        try:
            # Run these threads in an atomic transaction
            with concurrent.futures.ThreadPoolExecutor(max_workers=worker_size) as executor:
                logging.info("Bulk creating characters using a threadpool executor")

                def db_bulk_create(characters, run_id):
                    logging.info({"Characters creating": len(characters)})
                    bulk_character_response = requests.post(
                        f"{PP_URL}/books/{book_id}/bulk_characters/",
                        json={"characters": characters, "character_run_id": run_id},
                        headers=AUTH_HEADER,
                        verify=CERT_PATH,
                    )
                    logging.info({"Character create response": bulk_character_response})
                    return bulk_character_response.json()

                result_futures = list(map(lambda characters:
                                          executor.submit(db_bulk_create, characters, character_run_id), chunks))
                for future in concurrent.futures.as_completed(result_futures):
                    try:
                        logging.info({"Characters chunk created", future.result()})
                    except Exception as e:
                        logging.error(f'Error in creating characters - {str(e)}')
        except Exception as ex:
            logging.error(f"Error saving characters - {str(ex)}")
            raise

    def update_pages(self):
        logging.info("Updating Pages...")
        bulk_page_response = requests.post(
            f"{PP_URL}/books/{self.book_id}/bulk_pages_update/",
            json={"pages": self.pages, "tif_root": "/ocean/projects/hum160002p/shared"},
            headers=AUTH_HEADER,
            verify=CERT_PATH,
        )
        logging.info(bulk_page_response.content)

    def update_lines(self):
        logging.info("Updating Lines...")
        bulk_line_response = requests.post(
            f"{PP_URL}/books/{self.book_id}/bulk_lines_update/",
            json={"lines": self.lines},
            headers=AUTH_HEADER,
            verify=CERT_PATH,
        )
        logging.info(bulk_line_response.content)

    def update_characters(self):
        logging.info("Updating Characters...")
        character_run = self.get_character_run(self.characters[0]['id'])
        character_run_id = character_run['id']
        worker_size = 20
        character_list = self.characters
        chunks = list(self.divide_into_chunks(character_list, int(round(len(character_list) / worker_size))))
        logging.info({"Total number of characters to be added": len(character_list)})
        logging.info({"Number of chunks for characters": len(chunks)})
        try:
            # Run these threads in an atomic transaction
            with concurrent.futures.ThreadPoolExecutor(max_workers=worker_size) as executor:
                logging.info("Bulk creating characters using a threadpool executor")

                def db_bulk_update(characters, run_id):
                    bulk_character_response = requests.post(
                        f"{PP_URL}/books/{self.book_id}/bulk_characters_update/",
                        json={"characters": characters, "character_run_id": run_id},
                        headers=AUTH_HEADER,
                        verify=CERT_PATH,
                    )
                    return bulk_character_response

                result_futures = list(map(lambda characters:
                                          executor.submit(db_bulk_update, characters, character_run_id), chunks))
                for future in concurrent.futures.as_completed(result_futures):
                    try:
                        logging.info({"Characters chunk updated", len(future.result())})
                    except Exception as e:
                        logging.error(f'Error in updating characters - {str(e)}')
        except DatabaseError as ex:
            logging.error(f"Error saving characters - {str(ex)}")
            raise


def main():

    logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

    # Options and arguments
    p = optparse.OptionParser(
        description="Load a directory containing Ocular JSON outputs",
        usage="usage: %prog [options] (-h for help)",
    )
    p.add_option(
        "-b",
        "--book_id",
        dest="book_id",
        help="UUID of the book from printprobability.psc.edu",
    )
    p.add_option(
        "-j",
        "--json",
        dest="json",
        help="Absolute directory path (starting with /ocean) where the Ocular JSON output is stored.",
    )
    p.add_option(
        "-u",
        "--update",
        dest="update",
        action="store_true",
        help="Whether this is an update or not i.e. create",
        default=False,
    )

    (opt, sources) = p.parse_args()

    logging.info(f"Using {CERT_PATH} for SSL verification")
    logging.info(f"Book id {opt.book_id}")
    logging.info(f"JSON dir {opt.json}")
    logging.info(f"Update? - {opt.update}")

    pp_loader = BookLoader(
        book_id=opt.book_id,
        json_directory=opt.json,
        update=opt.update
    )
    pp_loader.load_db()


if __name__ == "__main__":
    main()