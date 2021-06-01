from pymongo import MongoClient

from pubmed.server.main.logger import get_logger

PV_MOUNT = '/upw_data/'
logger = get_logger()


def drop_collection(coll: str) -> None:
    logger.debug(f'Dropping {coll}')
    client = MongoClient('mongodb://mongo:27017/')
    db = client.unpaywall
    collection = db[coll]
    collection.drop()
