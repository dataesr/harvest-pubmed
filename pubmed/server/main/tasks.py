from pubmed.server.main.logger import get_logger
from pubmed.server.main.pubmed_harvest import download_one_entrez_date
from pubmed.server.main.pubmed_parse import pubmed_to_json, pubmed_to_mongo

logger = get_logger()


def create_task_pubmed(args: dict) -> None:
    date = args.get('date')
    task_type = args.get('type')
    if task_type == 'harvest' and date:
        download_one_entrez_date(date=date)
    elif task_type == 'parse' and date:
        pubmed_to_json(date=date)
    elif task_type == 'load':
        pubmed_to_mongo()
    elif task_type == 'all':
        download_one_entrez_date(date=date)
        pubmed_to_json(date=date)
    else:
        logger.error('Type error: your request should have a type between "harvest", "parse", "load" or "all".')
