import datetime
import json
import os

import pandas as pd
import pymongo

from bs4 import BeautifulSoup
from jsonschema import exceptions, validate

from pubmed.server.main.logger import get_logger
from pubmed.server.main.utils_mongo import drop_collection
from pubmed.server.main.utils_swift import conn, get_objects, set_objects
from pubmed.server.main.pubmed_harvest import download_one_entrez_date

PV_MOUNT = '/upw_data/'
logger = get_logger()

schema = json.load(open('/src/pubmed/server/main/schema.json', 'r'))

def get_orcid(x: str) -> str:
    for s in x.split('/'):
        v = s.strip()
        if len(v) == 19:
            return v
        if len(v) == 16:
            return '-'.join([v[0:4], v[4:8], v[8:12], v[12:16]])
    return None


def get_date(elt: str) -> str:
    year, month, day = 'XXXX', 'XX', 'XX'
    if elt:
        articledate = elt
        if articledate.find('year'):
            year = articledate.find('year').text
        if articledate.find('month'):
            month = articledate.find('month').text.zfill(2)
        if articledate.find('day'):
            day = articledate.find('day').text.zfill(2)
    return f'{year}-{month}-{day}'


def parse_pubmed(notice: dict) -> dict:
    res = {}
    res['sources'] = ['pubmed']
    res['domains'] = ['health']
    x = notice['notice']
    soup = BeautifulSoup(x, 'lxml')
    # DOI
    doi = None
    doi_elt = soup.find('articleid', {'idtype': 'doi'})
    if doi_elt is None:
        doi_elt = soup.find('elocationid', {'eidtype': 'doi'})
    if doi_elt is not None:
        doi = doi_elt.text.lower()
    is_comment_or_letter = False
    for pubtype in soup.find_all('publicationtype'):
        if pubtype.text.lower() in ['letter', 'comment']:
            is_comment_or_letter = True
    if doi and '10' in doi and not is_comment_or_letter:
        res['doi'] = doi.lower()
    title = ''
    if soup.find('articletitle'):
        title = soup.find('articletitle').text
    res['title'] = title
    #lang
    lang = ''
    if soup.find('language'):
        lang = soup.find('language').text[0:2]
    res['language'] = lang
    #abstract
    abstract = ''
    if soup.find('abstracttext'):
        abstract = {'abstract': soup.find('abstracttext').text}
        if lang:
            abstract['lang'] = lang
    if abstract:
        res['abstract'] = [abstract]

    publication_date = get_date(soup.find('articledate'))
    if publication_date[0:4] == 'XXXX':
        publication_date = get_date(soup.find('pubmedpubdate', {'pubstatus': 'entrez'}))
    if publication_date[0:4] == 'XXXX':
        publication_date = get_date(soup.find('datecompleted'))
    res['publication_date'] = publication_date
    #res['publication_year_month'] = res['publication_date'][0:7]
    res['publication_year'] = res['publication_date'][0:4]
    publication_types = []
    for e in soup.find_all('publicationtype'):
        publication_types.append(e.text)
    res['publication_types'] = publication_types
    authors = []
    affiliations = []
    for ix, aut in enumerate(soup.find_all('author')):
        author = {}
        author['author_position'] = ix + 1
        last_name = aut.find('lastname')
        first_name = aut.find('forename')
        if last_name:
            author['last_name'] = last_name.text
        if first_name:
            author['first_name'] = first_name.text
        identifiers = aut.find_all('identifier')
        for identifier in identifiers:
            source_id = identifier.attrs['source']
            if source_id.lower() == 'orcid':
                orcid = get_orcid(identifier.text)
                if orcid:
                    author['orcid'] = orcid
            if 'external_ids' not in author:
                author['external_ids'] = []
            author['external_ids'].append({'id_type': source_id.lower(), 'id_value': identifier.text})

        author['full_name'] = author.get('first_name', '') + ' ' + author.get('last_name', '')
        author['full_name'] = author['full_name'].strip()
        if len(author) > 0 and author not in authors:
            authors.append(author)
        author_aff = []
        for aff in aut.find_all('affiliationinfo'):
            affiliation = {}
            identifiers = aff.find_all('identifier')
            for identifier in identifiers:
                if 'external_ids' not in affiliation:
                    affiliation['external_ids'] = []
                source_id = identifier.attrs['source']
                affiliation['external_ids'].append({'id_type': source_id, 'id_value': identifier.text})
            aff_name = aff.find('affiliation')
            if aff_name:
                affiliation['name'] = aff_name.text
            if affiliation not in affiliations:
                affiliations.append(affiliation)
            author_aff.append(affiliation)
        if len(author_aff) > 0:
            author['affiliations'] = author_aff
    res['authors'] = authors
    res['affiliations'] = affiliations
    # KEYWORDS
    keywords = [k.text for k in soup.find_all('keyword')]
    res['keywords'] = [{"keyword": k} for k in keywords]
    # URL
    pubmed_id_elt = soup.find('articleid', {'idtype': 'pubmed'})
    if pubmed_id_elt:
        pubmed_id = pubmed_id_elt.text
    else:
        pmid = notice.get('pmid')
        logger.warning(f'No pubmed element for pmid {pmid}?')
        logger.warning(x)
        return False
    res['url'] = f'https://www.ncbi.nlm.nih.gov/pubmed/{pubmed_id}'
    res['pmid'] = f'{pubmed_id}'
    # MESH
    mesh_headings = []
    for mesh_elt in soup.find_all('meshheading'):
        mesh = ''
        if mesh_elt.find('descriptorname'):
            mesh = mesh_elt.find('descriptorname').text
        if mesh_elt.find('qualifiername'):
            mesh += '__' + mesh_elt.find('qualifiername').text
        if len(mesh) > 0:
            mesh_headings.append(mesh)
    for mesh_elt in soup.find_all('supplmeshname'):
        mesh = mesh_elt.text
        if len(mesh) > 0:
            mesh_headings.append(mesh)
    res['mesh_headings'] = mesh_headings
    databank = []
    for databank_elt in soup.find_all('databank'):
        elt = {}
        if databank_elt.find('databankname'):
            elt['name'] = databank_elt.find('databankname').text
            elt['accession_numbers'] = []
        for access in databank_elt.find_all('accessionnumber'):
            elt['accession_numbers'].append({'accession_number': access.text})
        databank.append(elt)
    res['databank'] = databank
    grants = []
    for grant_elt in soup.find_all('grant'):
        elt = {}
        for f in ['grantid', 'agency', 'country']:
            if grant_elt.find(f):
                elt[f] = grant_elt.find(f).text
        grants.append(elt)
    if grants:
        res['grants'] = grants
        res['has_grant'] = True
    else:
        res['has_grant'] = False
    # COI
    coi_elt = soup.find('coistatement')
    if coi_elt:
        coi = coi_elt.get_text(' ')
        res['coi'] = coi
    # Journal
    journal_elt = soup.find('journal')
    if journal_elt:
        issn_print_elt = journal_elt.find('issn', {'issntype': 'Print'})
        if issn_print_elt:
            issn_print = issn_print_elt.get_text(' ')
            res['issn_print'] = issn_print
        issn_electronic_elt = journal_elt.find('issn', {'issntype': 'Electronic'})
        if issn_electronic_elt:
            issn_electronic = issn_electronic_elt.get_text(' ')
            res['issn_electronic'] = issn_electronic
        journal_title_elt = journal_elt.find('title')
        if journal_title_elt:
            journal_title = journal_title_elt.get_text(' ')
            res['journal_title'] = journal_title
    return res


def validate_json_schema(data: list, schema: dict) -> bool:
    is_valid = True
    try:
        for datum in data:
            validate(instance=datum, schema=schema)
    except exceptions.ValidationError as error:
        is_valid = False
        logger.debug(error)
    return is_valid


def parse_pubmed_one_date(date: str) -> pd.DataFrame:
    logger.debug(f'Parse pubmed one date {date}')
    all_notices = get_objects(conn=conn, date=date, container='pubmed', path='notices')
    logger.debug(f'Len notices = {len(all_notices)}')
    all_parsed = []
    has_done_full_reshesh = False
    for notice in all_notices:
        if 'pmid' not in notice:
            continue
        try:
            parsed = parse_pubmed(notice)
        except:
            parsed = None

        if parsed:
            all_parsed.append(parsed)
        elif has_done_full_reshesh is False:
            logger.debug(f"refresh full download for date {date}")
            download_one_entrez_date(date=date, refresh_all=True)
            has_done_full_reshesh = True
        else:
            continue
    is_valid = validate_json_schema(data=all_parsed, schema=schema)
    df_publis = pd.DataFrame(all_parsed)
        
    set_objects(conn=conn, date=date, all_objects=df_publis, container='pubmed', path='parsed')
    logger.debug('Parsed notices saved into Object Storage.')
    
    if is_valid is False:
        logger.debug('BEWARE !! Some notices are not schema-valid. See previous logs.')
    return df_publis


def pubmed_to_json(date: str) -> None:
    logger.debug(f'Pubmed to json: {date}')
    os.system(f'mkdir -p {PV_MOUNT}pubmed')
    dt_str = date.replace('/', '')
    output_json = f'{PV_MOUNT}pubmed/pubmed_mongo_{dt_str}.jsonl'
    start = datetime.datetime.now()
    df_publis = parse_pubmed_one_date(date=date)
    df_publis.to_json(output_json, orient='records', lines=True)
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'Json written in {delta}')


def pubmed_to_mongo() -> None:
    logger.debug('Concatenate all daily pubmed files in one big')
    global_pubmed_file = f'{PV_MOUNT}pubmed_global.jsonl'
    os.system(f'rm -rf {global_pubmed_file}')
    os.system(f'cat {PV_MOUNT}pubmed/pubmed_mongo_*.jsonl >> {global_pubmed_file}')
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['unpaywall']
    collection_name = 'pubmed'
    drop_collection(collection_name)
    start = datetime.datetime.now()
    mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/unpaywall --file ' \
                  f'{global_pubmed_file} --collection {collection_name}'
    logger.debug(f'Mongoimport start at {start}')
    logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'Mongoimport done in {delta}')
    logger.debug(f'Checking doi index on collection {collection_name}')
    mycol = mydb[collection_name]
    mycol.create_index('doi')
    resp = mycol.create_index('pmid')
    logger.debug(resp)
