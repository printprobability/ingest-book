from bs4 import BeautifulSoup
from functools import partial
import pandas as pd
import urllib.request
from urllib.parse import parse_qs, urlencode, urlsplit

columns = ["ESTC No.", "Title", "Author", "Publisher Info", "Description", "Locations", "General Notes", "Citation/Ref. Notes", "Electronic Location"]
text_queries = ["ESTC Citation No.", "Main Title", "ME-Personal Name", "Imprint", "Phys.Description", "Location",
                "General Note", "Citation/Ref. Note", "Electronic Location"]


def _query_html(soup: BeautifulSoup, field: str) -> str:
    """
    Finds all instances of single query field e.g. Author, Title etc.
    (becomes column in DF)

    Parameters
    ----------
    soup : BeautifulSoup
        BeautifulSoup html parsed object
    field: str
        Desired search field text e.g. Publisher_year

    Returns
    -------
    field_values : pd.Series
        pd.Series of all desired field values consisting of future DF column
    """
    # find all values containing query field
    all_text = soup.find_all('td', string=field)
    values = []
    for text in all_text:
        value = text.find_next_sibling("td").text.strip()
        if value is not None and value != 'nan':
            value = value.replace('\xa0', ' ')
            values.append(value)
        else:
            values.append('')
    return '\t---\t'.join(values)


def _read_url(url: str):
    fp = urllib.request.urlopen(url)
    url_bytes = fp.read()

    html_string = url_bytes.decode("utf8")
    fp.close()
    return html_string


def _estc_url(estc_number: str):
    print("Fetching information for ESTC number:", estc_number)
    url = "http://estc.bl.uk/{}".format(estc_number)
    html = _read_url(url)
    soup = BeautifulSoup(html, 'html.parser')
    el = soup.select_one("a[href*=set_entry]")
    href = el.get('href')
    parsed = urlsplit(href)
    query_dict = parse_qs(parsed.query)
    query_dict['format'][0] = '002'
    query_new = urlencode(query_dict, doseq=True)
    parsed = parsed._replace(query=query_new)
    url_new = (parsed.geturl())
    return url_new


def est_info_for_number(estc_number: str) -> dict:
    url = _estc_url(estc_number)
    html = _read_url(url)
    # text_file = open("test.html", "r")
    # html = text_file.read()
    soup = BeautifulSoup(html, "html.parser")
    _do_query = partial(_query_html, soup)

    df_values = {}
    for column, query in zip(columns, text_queries):
        df_values[column] = _do_query(query)
    return df_values
