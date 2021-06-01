from pubmed.server.main.pubmed_harvest import download_one_entrez_date
from pubmed.server.main.pubmed_parse import pubmed_to_json, pubmed_to_mongo


def create_task_pubmed(args: dict) -> None:
    dt = args.get('dt')
    task_type = args.get('type')
    if task_type == 'harvest' and dt:
        download_one_entrez_date(dt)
    elif task_type == 'parse' and dt:
        pubmed_to_json(dt)
    elif task_type == 'load':
        pubmed_to_mongo()
