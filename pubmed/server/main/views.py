import datetime
import dateutil.parser
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue

from pubmed.server.main.logger import get_logger
from pubmed.server.main.tasks import create_task_pubmed
from pubmed.server.main.medline_harvest import get_all_files, download_medline, parse_medline
from pubmed.server.main.utils_swift import conn, get_filenames_by_page

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

@main_blueprint.route('/medline', methods=['POST'])
def run_task_medline():
    """
    Harvest data from medline
    """
    args = request.get_json(force=True)
    all_files = get_all_files()
    for url in all_files:
        download_medline(url)

    container='medline'
    for page in range(1, 10000):
        logger.debug(f'Getting MEDLINE notices objects for page {page} from object storage ({container})')
        filenames = get_filenames_by_page(conn, container=container, page=page)
        logger.debug(f'{len(filenames)} files retrieved from object storage')
        if len(filenames) == 0:
            break
        for filename in filenames:
            with Connection(redis.from_url(current_app.config['REDIS_URL'])):
                q = Queue('pubmed', default_timeout=DEFAULT_TIMEOUT * 10)
                task = q.enqueue(parse_medline, filename)
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
    flat_list = [item for sublist in objects for item in sublist]
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
