from pubmed.server.main.pubmed_harvest import download_one_entrez_date
from pubmed.server.main.pubmed_parse import pubmed_to_json, pubmed_to_mongo


def create_task_pubmed(args: dict) -> None:
    date = args.get('date')
    task_type = args.get('type')
    if task_type == 'harvest' and date:
        download_one_entrez_date(date=date)
    elif task_type == 'parse' and date:
        pubmed_to_json(date=date)
    elif task_type == 'load':
        pubmed_to_mongo()
