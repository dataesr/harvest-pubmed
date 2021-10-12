import time
import datetime
import os
import json
import gzip
import requests
import io
import pandas as pd
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
from jsonschema import exceptions, validate
from typing import Union

from pubmed.server.main.logger import get_logger
from pubmed.server.main.utils_swift import conn, get_objects_raw, set_objects_raw
from pubmed.server.main.pubmed_parse import parse_pubmed
from pubmed.server.main.utils import FRENCH_ALPHA2, chunks

AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')
logger = get_logger()
matcher_endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/enrich_filter'
PV_MOUNT = '/upw_data/'
schema = json.load(open('/src/pubmed/server/main/schema.json', 'r'))

def get_all_files():
    medline_files = [] 
    for update in ['baseline', 'updatefiles']:
        medline_url = f'https://ftp.ncbi.nlm.nih.gov/pubmed/{update}'
        soup = BeautifulSoup(requests.get(medline_url).text, 'lxml')
        medline_files +=[f'{medline_url}/{a.text}' for a in soup.find_all('a') if a.text.startswith('pubmed') and a.text.endswith('xml.gz')]
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

def parse_medline(url):
    logger.debug(f'dowloading {url}')
    filename = url.split('/')[-1].split('.')[0]
    s=requests.get(url).content
    input_content = gzip.open(io.BytesIO(s))
    logger.debug(f'parsing xml {filename}')
    tree = ET.parse(input_content)
    root = tree.getroot()
    all_parsed = []
    removed_pmids = []
    for child in root:
        citation = child.find('MedlineCitation')
        if citation:
            new_pmid = citation.find('PMID').text
            current_notice = {'notice': ET.tostring(child)}
            current_parsed = parse_pubmed(current_notice)
            all_parsed.append(current_parsed)
        elif child.tag == 'DeleteCitation':
            removed_pmids = [{'pmid': c.text} for c in child.findall('PMID')]
        else:
            logger.debug("unexpected error")
            logger.debug(ET.tostring(child))
    if removed_pmids:
        set_objects_raw(conn=conn, path='removed/'+filename, all_objects=removed_pmids, container='medline')

    chunk_index = 0
    for all_parsed_chunk in chunks(all_parsed, 10):
        logger.debug(f'matching chunk {chunk_index} for {filename}')
        publications_with_countries = get_matcher_results(publications=all_parsed_chunk, countries_to_keep=FRENCH_ALPHA2)
        all_parsed_publications = publications_with_countries['publications']
        all_parsed_filtered = publications_with_countries['filtered_publications']
        is_valid = validate_json_schema(data=all_parsed_publications, _schema=schema)
        df_publis = pd.DataFrame(all_parsed_publications)
        set_objects_raw(conn=conn, path=f'{filename}_{chunk_index}', all_objects=df_publis, container='medline')
        logger.debug('Parsed notices saved into Object Storage.')
        df_publis_filtered = pd.DataFrame(all_parsed_filtered)
        set_objects_raw(conn=conn, path=f'fr/{filename}_{chunk_index}', all_objects=df_publis_filtered, container='medline')
        logger.debug('Filtered notices saved into Object Storage.')
        if is_valid is False:
            logger.debug('BEWARE !! Some notices are not schema-valid. See previous logs.')
        chunk_index += 1
        if chunk_index == 1:
            break
