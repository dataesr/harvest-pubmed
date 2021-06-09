import datetime
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue

from pubmed.server.main.tasks import create_task_pubmed

main_blueprint = Blueprint('main', __name__, )

DATE_FORMAT = "%Y/%m/%d"
DEFAULT_TIMEOUT = 21600


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
    start_string = args.get('start')
    end_string = args.get('end')
    del args['start']
    del args['end']
    start_date = datetime.datetime.strptime(start_string, DATE_FORMAT).date()
    end_date = datetime.datetime.strptime(end_string, DATE_FORMAT).date()
    delta = datetime.timedelta(days=1)
    while start_date <= end_date:
        args['date'] = start_date.strftime(DATE_FORMAT)
        start_date += delta
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
            },
        }
    else:
        response_object = {'status': 'error'}
    return jsonify(response_object)
