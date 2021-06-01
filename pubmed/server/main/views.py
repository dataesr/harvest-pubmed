import redis
from flask import render_template, Blueprint, jsonify, request, current_app
from rq import Queue, Connection

from pubmed.server.main.logger import get_logger
from pubmed.server.main.tasks import create_task_pubmed

main_blueprint = Blueprint('main', __name__, )
logger = get_logger()


@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('main/home.html')


@main_blueprint.route('/pubmed', methods=['POST'])
def run_task_harvest():
    args = request.get_json(force=True)
    logger.debug(args)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('pubmed', default_timeout=21600)
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
