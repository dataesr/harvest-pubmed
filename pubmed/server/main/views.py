import datetime
import dateutil.parser
import redis

from bs4 import BeautifulSoup
from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue

from pubmed.server.main.logger import get_logger
from pubmed.server.main.tasks import create_task_pubmed
from pubmed.server.main.medline_harvest import get_all_files, download_medline, parse_medline
from pubmed.server.main.utils_swift import clean_container, conn, get_filenames_by_page, get_objects_raw, exists_in_storage

DATE_FORMAT = '%Y/%m/%d'
DEFAULT_TIMEOUT = 36000
logger = get_logger(__name__)
main_blueprint = Blueprint('main', __name__, )


@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('home.html')


@main_blueprint.route('/pubmed', methods=['POST'])
def run_task_harvest():
    """
    Harvest data from pubmed
    Expected args:
    - task: str ["harvest", "parse", "load", "all"]
    - date: str 2021/04/26
    """
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('pubmed', default_timeout=DEFAULT_TIMEOUT)
        task = q.enqueue(create_task_pubmed, args)
    response_object = {
        'status': 'success',
        'data': {
            'task_id': task.get_id()
        }
    }
    return jsonify(response_object), 202


def get_nb_chunks(filename):
    for i in range(0, 99999):
        path = 'notices/{filename}_{i}'
        if exists_in_storage('medline', path) is False:
            return i
            break

@main_blueprint.route('/medline', methods=['POST'])
def run_task_medline():
    """
    Harvest data from medline
    """
    container = 'medline'
    removed = []
    #clean_container(container)
    logger.debug('getting files from FTP medline')
    all_notices_filenames = get_all_files()
    for ix, url in enumerate(all_notices_filenames):
        if ix % 100 == 0:
            logger.debug(f'{ix} / {len(all_notices_filenames)} notice filenames')
        filename = url.split('/')[-1].split('.')[0]

        # check if data in chunk _1
        sample_notices = get_objects_raw(conn=conn, path=f'notices/{filename}_1', container=container)
        nb_chunks = get_nb_chunks(filename)
        if len(sample_notices) == 0 or nb_chunks < 1:
            nb_chunks = download_medline(url)
        
        sample_parsed = get_objects_raw(conn=conn, path=f'parsed/{filename}_{nb_chunks - 1}', container=container)
        sample_notices = get_objects_raw(conn=conn, path=f'notices/{filename}_{nb_chunks - 1}', container=container)
        if len(sample_notices) != len(sample_parsed):
            logger.debug(f'nb of notices {len(sample_notices)} != nb of existing parsed {len(sample_parsed)} => re-parse all {nb_chunks} notices for {filename}')
            for k in range(0, nb_chunks):
                logger.debug(f'sending parsing task for {filename}_{k}')
                with Connection(redis.from_url(current_app.config['REDIS_URL'])):
                    q = Queue('harvest-pubmed', default_timeout=DEFAULT_TIMEOUT)
                    task = q.enqueue(parse_medline, f'{filename}_{k}')
                    response_object = {
                            'status': 'success',
                            'data': {
                            'task_id': task.get_id()
                            }
                    }
    return jsonify(response_object), 202

@main_blueprint.route('/medline_old', methods=['POST'])
def run_task_medline_old():
    """
    Harvest data from medline
    """
    container = 'medline'
    removed = []
    #clean_container(container)
    logger.debug('getting files from FTP medline')
    all_files = get_all_files()
    for ix, url in enumerate(all_files):
        if ix % 100 == 0:
            logger.debug(f'{ix} / {len(all_files)}')
        filename = url.split('/')[-1].split('.')[0]
        sample_notices = get_objects_raw(conn=conn, path=f'notices/{filename}_0', container=container)
        if len(sample_notices) == 0:
            download_medline(url)
        removed += get_objects_raw(conn=conn, path=f'removed/{filename}', container=container)
    removed_ids = set([str(k.get('pmid')) for k in removed])
    logger.debug(f'{len(removed_ids)} removed pmids')
    previous_ids = set([])
    for ix, url in enumerate(all_files):
        filename = url.split('/')[-1].split('.')[0]
        for k in range(0, 1000):
            pmids_to_parse = []
            current_notices = get_objects_raw(conn=conn, path=f'notices/{filename}_{k}', container=container)
            # if no more notices, stop
            if len(current_notices) == 0:
                break
            for n in current_notices:
                n['pmid'] = BeautifulSoup(n['notice'], 'lxml').find('pmid').text
                if (n['pmid'] not in previous_ids) and (n['pmid'] not in removed_ids):
                    pmids_to_parse.append(n['pmid'])
            logger.debug(f'{len(current_notices)} current_notices, and {len(pmids_to_parse)} pmids_to_parse')
            previous_ids.update(pmids_to_parse)
            if len(pmids_to_parse) == 0:
                continue
            # there are notices, continue only if parsed not here yet !
            current_parsed = get_objects_raw(conn=conn, path=f'parsed/{filename}_{k}', container=container)
            logger.debug(f'{len(current_parsed)} current_parsed')
            if len(current_parsed) == len(pmids_to_parse):
                logger.debug(f'parsed/{filename}_{k} already parsed')
                continue
            logger.debug(f'sending task for {filename}_{k} with {len(pmids_to_parse)} pmids to parse')
            with Connection(redis.from_url(current_app.config['REDIS_URL'])):
                q = Queue('harvest-pubmed', default_timeout=DEFAULT_TIMEOUT)
                task = q.enqueue(parse_medline, f'{filename}_{k}', pmids_to_parse)
                response_object = {
                        'status': 'success',
                        'data': {
                        'task_id': task.get_id()
                        }
                }

    return jsonify(response_object), 202


@main_blueprint.route('/pubmed_interval', methods=['POST'])
def run_task_pubmed_interval():
    """
    Harvest data from pubmed for an interval of time
    Expected args:
    - task: str ["harvest", "parse", "load", "all"]
    - start: str 2021/04/25
    - end: str 2021/04/26
    """
    args = request.get_json(force=True)
    task = args.get('task')
    start_string = args.get('start', '2013/01/01')
    end_string = args.get('end', datetime.date.today().isoformat())
    if 'start' in args:
        del args['start']
    if 'end' in args:
        del args['end']
    start_date = dateutil.parser.parse(start_string).date()
    end_date = dateutil.parser.parse(end_string).date()
    nb_days = (end_date - start_date).days
    logger.debug(f'Starting tasks inbetween {start_date} and {end_date}')
    for delta in range(nb_days):
        current_date = start_date + datetime.timedelta(days=delta)
        local_args = args.copy()
        local_args['date'] = current_date.strftime(DATE_FORMAT)
        with Connection(redis.from_url(current_app.config['REDIS_URL'])):
            q = Queue('pubmed', default_timeout=DEFAULT_TIMEOUT)
            task = q.enqueue(create_task_pubmed, local_args)
    response_object = {
        'status': 'success',
        'data': {
            'task_id': task.get_id()
        }
    }
    return jsonify(response_object), 202


@main_blueprint.route('/tasks/<task_id>', methods=['GET'])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('pubmed')
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            'status': 'success',
            'data': {
                'task_id': task.get_id(),
                'task_status': task.get_status(),
                'task_result': task.result,
            }
        }
    else:
        response_object = {'status': 'error'}
    return jsonify(response_object)
