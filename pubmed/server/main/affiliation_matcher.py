import os
import requests
from concurrent.futures import ThreadPoolExecutor
from pubmed.server.main.utils import chunks
from pubmed.server.main.elastic import client, load_in_es, reset_index

from pubmed.server.main.logger import get_logger

AFFILIATION_MATCHER_SERVICE = os.getenv('AFFILIATION_MATCHER_SERVICE')
NB_AFFILIATION_MATCHER = 3
matcher_endpoint_url = f'{AFFILIATION_MATCHER_SERVICE}/match_api'

logger = get_logger(__name__)

def check_matcher_health():
    res = requests.post(matcher_endpoint_url, json={'query': 'france', 'type': 'country'})
    if 'results' in res:
        logger.debug("matcher seems healthy")
        return True
    else:
        logger.debug("matcher does not seem loaded, lets load it")
        load_res = requests.get(f'{AFFILIATION_MATCHER_SERVICE}/load', timeout=1000)
        logger.debug(load_res.json())
        return True

def get_country(affiliation):
    in_cache = False

    params={
        "size": 1,
        "query": {
            "term": {
                "affiliation.keyword": affiliation
            }
        }
    }
    r = client.search(index='bso-cache-country', body=params)
    hits = r['hits']['hits']
    if len(hits)>=1:
        in_cache = True
        countries = hits[0]['_source']['countries']
    else:
        strategies = [
                ['grid_city', 'grid_name', 'country_all_names'],
                ['grid_city', 'country_all_names'],
                ['grid_city', 'country_alpha3'],
                ['country_all_names'],
                ['country_subdivisions', 'country_alpha3']
        ]

        countries = requests.post(matcher_endpoint_url, json={'query': affiliation, 'type': 'country', 'strategies': strategies}).json()['results']
    return {'countries': countries, 'in_cache': in_cache}

def is_na(x):
    return not(not x)

def filter_publications_by_country(publications: list, countries_to_keep: list = None) -> list:
    logger.debug(f'Filter {len(publications)} publication against {",".join(countries_to_keep)} countries.')
    if countries_to_keep is None:
        countries_to_keep = []
    field_name = 'detected_countries'
    # Retrieve all affiliations
    all_affiliations = []
    for publication in publications:
        affiliations = publication.get('affiliations', [])
        affiliations = [] if affiliations is None else affiliations
        all_affiliations += [affiliation.get('name') for affiliation in affiliations]
        authors = publication.get('authors', [])
        for author in authors:
            affiliations = author.get('affiliations', [])
            affiliations = [] if affiliations is None else affiliations
            all_affiliations += [affiliation.get('name') for affiliation in affiliations]
    logger.debug(f'Found {len(all_affiliations)} affiliations in total.')
    # Deduplicate affiliations
    all_affiliations_list = list(filter(is_na, list(set(all_affiliations))))
    logger.debug(f'Found {len(all_affiliations_list)} different affiliations in total.')
    # Transform list into dict
    all_affiliations_dict = {}
    # Retrieve countries for all publications
    #for affiliation in all_affiliations:
    #    countries = requests.post(endpoint_url, json={'query': affiliation, 'type': 'country'}).json()['results']
    #    all_affiliations[affiliation] = countries
    check_matcher_health()
    use_parallel = False
    for all_affiliations_list_chunk in chunks(all_affiliations_list, 1000):
        if use_parallel:
            with ThreadPoolExecutor(max_workers=NB_AFFILIATION_MATCHER) as pool:
                countries_list = list(pool.map(get_country,all_affiliations_list_chunk))
            for ix, affiliation in enumerate(all_affiliations_list_chunk):
                all_affiliations_dict[affiliation] = countries_list[ix]
        else:
            for affiliation in all_affiliations_list_chunk:
                all_affiliations_dict[affiliation] = get_country(affiliation)
        logger.debug(f'{len(all_affiliations_dict)} / {len(all_affiliations_list)} treated in country_matcher')
        logger.debug(f'loading in cache')
        cache = []
        for ix, affiliation in enumerate(all_affiliations_list_chunk):
            if affiliation in all_affiliations_dict and all_affiliations_dict[affiliation]['in_cache'] is False:
                cache.append({'affiliation': affiliation, 'countries': all_affiliations_dict[affiliation]['countries']})
        load_in_es(data=cache, index='bso-cache-country')

    logger.debug('All countries of all affiliations have been retrieved.')
    # Map countries with affiliations
    for publication in publications:
        countries_by_publication = []
        affiliations = publication.get('affiliations', [])
        affiliations = [] if affiliations is None else affiliations
        for affiliation in affiliations:
            query = affiliation.get('name')
            if query in all_affiliations_dict:
                countries = all_affiliations_dict[query]['countries']
                affiliation[field_name] = countries
                countries_by_publication += countries
        authors = publication.get('authors', [])
        for author in authors:
            affiliations = author.get('affiliations', [])
            for affiliation in affiliations:
                query = affiliation.get('name')
                if query in all_affiliations_dict:
                    countries = all_affiliations_dict[query]['countries']
                    affiliation[field_name] = countries
                    countries_by_publication += countries
        publication[field_name] = list(set(countries_by_publication))
    if not countries_to_keep:
        filtered_publications = publications
    else:
        countries_to_keep_set = set(countries_to_keep)
        filtered_publications = [publication for publication in publications
                                 if len(set(publication[field_name]).intersection(countries_to_keep_set)) > 0]
    logger.debug(f'After filtering by countries, {len(filtered_publications)} publications have been kept.')
    return {'publications': publications, 'filtered_publications': filtered_publications}
