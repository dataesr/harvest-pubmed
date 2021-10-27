import sys

from pubmed.server.main.logger import get_logger
from pubmed.server.main.pubmed_harvest import download_one_entrez_date
from pubmed.server.main.pubmed_parse import pubmed_to_json, pubmed_to_mongo

logger = get_logger(__name__)

def create_task_pubmed(args: dict) -> None:
    logger.debug(f'Create task pubmed with args {args}')
    task = args.get('task')
    date = args.get('date')
    if task == 'harvest' and date:
        download_one_entrez_date(date=date)
    elif task == 'parse' and date:
        pubmed_to_json(date=date)
    elif task == 'load':
        pubmed_to_mongo()
    elif task == 'all':
        try:
            download_one_entrez_date(date=date)
        except:
            error = sys.exc_info()[0]
            logger.error(f'Harvesting of pubmed for date {date} caused an error : {error}')
        pubmed_to_json(date=date)
    else:
        logger.error('Task error: your request should have a task between "harvest", "parse", "load" or "all".')
