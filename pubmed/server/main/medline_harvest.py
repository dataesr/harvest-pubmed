import gzip
import io
import json
import os
import pandas as pd
import requests
import time
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
from jsonschema import exceptions, validate

from pubmed.server.main.logger import get_logger
from pubmed.server.main.pubmed_parse import parse_pubmed
from pubmed.server.main.utils import FRENCH_ALPHA2, chunks
from pubmed.server.main.utils_swift import conn, get_objects_raw, set_objects_raw

AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')
logger = get_logger(__name__)
matcher_endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/enrich_filter'
PV_MOUNT = '/upw_data/'
schema = json.load(open('/src/pubmed/server/main/schema.json', 'r'))


def get_all_files():
    medline_files = []
    for update in ['baseline', 'updatefiles']:
        medline_url = f'https://ftp.ncbi.nlm.nih.gov/pubmed/{update}'
        soup = BeautifulSoup(requests.get(medline_url).text, 'lxml')
        medline_files += [f'{medline_url}/{a.text}' for a in soup.find_all('a') if a.text.startswith('pubmed') and a.text.endswith('xml.gz')]
    medline_files.reverse()
    return medline_files


def validate_json_schema(data: list, _schema: dict) -> bool:
    is_valid = True
    try:
        for datum in data:
            validate(instance=datum, schema=_schema)
    except exceptions.ValidationError as error:
        is_valid = False
        logger.debug(error)
    return is_valid


def get_matcher_results(publications: list, countries_to_keep: list) -> list:
    r = requests.post(matcher_endpoint_url, json={'publications': publications, 'countries_to_keep': countries_to_keep})
    task_id = r.json()['data']['task_id']
    logger.debug(f'New task {task_id} for matcher')
    for i in range(0, 10000):
        r_task = requests.get(f'{AFFILIATION_MATCHER_SERVICE}/tasks/{task_id}').json()
        try:
            status = r_task['data']['task_status']
        except:
            logger.error(f'Error in getting task {task_id} status : {r_task}')
            status = 'error'
        if status == 'finished':
            return r_task['data']['task_result']
        elif status in ['started', 'queued']:
            time.sleep(2)
            continue
        else:
            logger.error(f'Error with task {task_id} : status {status}')
            return []


def download_medline(url: str) -> None:
    logger.debug(f'Dowloading Medline {url}')
    filename = url.split('/')[-1].split('.')[0]
    s = requests.get(url).content
    input_content = gzip.open(io.BytesIO(s))
    logger.debug(f'Reading xml {filename}')
    tree = ET.parse(input_content)
    root = tree.getroot()
    removed_pmids = []
    all_notices = []
    for child in root:
        citation = child.find('MedlineCitation')
        if citation:
            current_notice = {'notice': ET.tostring(child)}
            all_notices.append(current_notice)
        elif child.tag == 'DeleteCitation':
            removed_pmids = [{'pmid': c.text} for c in child.findall('PMID')]
        else:
            logger.debug('Unexpected error')
            logger.debug(ET.tostring(child))
    if removed_pmids:
        set_objects_raw(conn=conn, path='removed/'+filename,
                        all_objects=removed_pmids, container='medline')
    chunk_index = 0
    for all_notices_chunk in chunks(all_notices, 1000):
        set_objects_raw(conn=conn, path=f'notices/{filename}_{chunk_index}',
                        all_objects=all_notices_chunk, container='medline')
        chunk_index += 1


def parse_medline(filename: str) -> None:
    logger.debug(f'Matching {filename}')
    container = 'medline'
    notices = get_objects_raw(conn=conn, path=filename, container='medline')
    publications = []
    for notice in notices:
        publication = parse_pubmed(notice)
        publications.append(publication)
    publications_with_countries = get_matcher_results(
        publications=publications, countries_to_keep=FRENCH_ALPHA2)
    all_parsed_publications = publications_with_countries['publications']
    all_parsed_filtered = publications_with_countries['filtered_publications']
    is_valid = validate_json_schema(data=all_parsed_publications,
                                    _schema=schema)
    df_publis = pd.DataFrame(all_parsed_publications)
    set_objects_raw(conn=conn, path=f'parsed/{filename}',
                    all_objects=df_publis, container=container)
    logger.debug('Parsed notices saved into Object Storage.')
    df_publis_filtered = pd.DataFrame(all_parsed_filtered)
    set_objects_raw(conn=conn, path=f'parsed/fr/{filename}',
                    all_objects=df_publis_filtered, container=container)
    logger.debug('Filtered notices saved into Object Storage.')
    if is_valid is False:
        logger.debug(f'BEWARE !! Some notices are not schema-valid in file \
            {filename}. See previous logs.')
