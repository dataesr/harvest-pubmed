import datetime
import math
import time

import requests
from bs4 import BeautifulSoup

from pubmed.server.main.logger import get_logger
from pubmed.server.main.utils_swift import conn, get_inventory_json, get_objects, remove_all_elt_with_field, \
    set_inventory_json, set_objects

RETMAX = 25000
API_KEY = '37773bb127a1587f715b8c26d28834b2f308'
logger = get_logger()


def download_pmid(pmid: str) -> dict:
    page_url = f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}' \
               f'&rettype=xml&api_key={API_KEY}'
    try:
        r = requests.get(page_url, timeout=2)
    except:
        time.sleep(5)
        try:
            r = requests.get(page_url, timeout=10)
        except:
            logger.error('Page not responding ' + page_url)
            return None
    if '<ERROR>' in r.text or 'API rate limit exceeded' in r.text:
        return None
    return {
        'id': f'pmid{pmid}',
        'pmid': f'{pmid}',
        'notice': r.text
    }


def download_one_entrez_date(date: str, fr_only: bool = False, early_stop: bool = False) -> None:
    container = 'pubmed'
    if fr_only:
        container = 'pubmedfr'
    existing_pmid = get_inventory_json(conn, date, container, 'notices_inventory')
    last_download_date = existing_pmid.get(date, {}).get('last_download_date')
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=('
    if fr_only:
        url += '(%22france%22[Affiliation]%20OR%20%22inserm%22[Affiliation])'
        url += '%20AND%20'
    url += '(%22' + date + '%22[EDat]%20:%20%22' + date + '%22[EDat])'
    if last_download_date is not None:
        url += '%20AND%20'
        url += '(%22' + last_download_date + '%22[LR]%20:%20%22' + '3000' + '%22[LR])'
    url += ')&retmax={}&retstart={}&api_key={}'
    r = requests.get(url.format(1, 0, API_KEY))
    soup = BeautifulSoup(r.text, 'html5lib')
    nb_res = int(soup.find('count').text)
    nb_pages = math.ceil(nb_res / RETMAX)
    logger.debug(f'{date} modified since {last_download_date} Pubmed results = {nb_res}, so {nb_pages} pages')
    all_notices = get_objects(conn, date, container, 'notices')
    current_download_date = datetime.datetime.today().strftime('%Y/%m/%d')
    is_modified = False
    id_to_download = []
    for page in range(0, nb_pages):
        start = page * RETMAX
        r = requests.get(url.format(RETMAX, start, API_KEY))
        soup = BeautifulSoup(r.text, 'html5lib')
        id_to_download += list(set(soup.find('idlist').find_all('id')))
    id_to_download = list(set(id_to_download))
    logger.debug(f'{len(id_to_download)} ids to download')
    for ix, e in enumerate(id_to_download):
        pmid = e.text
        is_modified = True
        if ix % 50 == 0:
            logger.debug(ix)
        download = download_pmid(pmid)
        if download:
            existing_pmid[date] = {'last_download_date': current_download_date}
            remove_all_elt_with_field(all_notices, f'{pmid}', 'pmid')
            all_notices.append(download)
            existing_pmid[pmid] = {'last_download_date': current_download_date}
        time.sleep(0.5)
        if early_stop and ix > 5:
            break
    if len(all_notices) > len(existing_pmid) - 1:
        all_notices_no_duplicates = []
        for n in reversed(all_notices):
            if n.get('id') not in [p.get('id') for p in all_notices_no_duplicates]:
                all_notices_no_duplicates.append(n)
        all_notices = all_notices_no_duplicates
        is_modified = True
        logger.debug(f'Removed duplicates, now {len(all_notices)} in obj vs {len(existing_pmid)} in invent')
    if is_modified:
        set_objects(conn, date, all_notices, container, 'notices')
        set_inventory_json(conn, date, existing_pmid, container, 'notices_inventory')
        logger.debug(f'Update ! {date}')
