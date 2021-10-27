import gzip
import json
import os
import pandas as pd
import swiftclient
from retry import retry

from io import BytesIO, TextIOWrapper

from pubmed.server.main.logger import get_logger

logger = get_logger(__name__)
key = os.getenv('OS_PASSWORD')
project_id = os.getenv('OS_TENANT_ID')
project_name = os.getenv('OS_PROJECT_NAME')
tenant_name = os.getenv('OS_TENANT_NAME')
username = os.getenv('OS_USERNAME')
user = f'{tenant_name}:{username}'
region_name = 'GRA'

conn = swiftclient.Connection(
    authurl='https://auth.cloud.ovh.net/v3',
    user=user,
    key=key,
    os_options={
        'user_domain_name': 'Default',
        'project_domain_name': 'Default',
        'project_id': project_id,
        'project_name': project_name,
        'region_name': region_name
    },
    auth_version='3'
)

@retry(delay=2, tries=50)
def get_filenames_by_page(conn, container: str, page: int) -> list:
    logger.debug(f'Retrieving object from container {container} and page {page}')
    marker = None
    keep_going = True
    current_page = 0
    while keep_going:
        content = conn.get_container(container=container, marker=marker, limit=1000)[1]
        filenames = [file['name'] for file in content]
        if len(filenames) == 0:
            return []
        current_page += 1
        keep_going = (page > current_page)
        if len(content) > 0:
            marker = content[-1]['name']
    return filenames

@retry(delay=2, tries=50)
def set_inventory_json(conn: swiftclient.Connection, date: str, inventory_json: dict, container: str, path: str)\
        -> None:
    contents = json.dumps(inventory_json)
    conn.put_object(container, f'{path}/inventory_{date.replace("/", "")}', contents=contents)
    return


@retry(delay=2, tries=50)
def get_inventory_json(conn: swiftclient.Connection, date: str, container: str, path: str) -> dict:
    try:
        inventory = conn.get_object(container, f'{path}/inventory_{date.replace("/", "")}')[1]
        inventory = inventory.decode()
        return json.loads(inventory)
    except:
        return {}


def get_notice_filename(date: str, path: str) -> str:
    fulldate = date.replace('/', '')
    return f'{path}/{date}/{path}_{fulldate}.json.gz'


@retry(delay=2, tries=50)
def get_objects(conn: swiftclient.Connection, date: str, container: str, path: str) -> list:
    try:
        df = pd.read_json(BytesIO(conn.get_object(container, get_notice_filename(date, path))[1]), compression='gzip')
    except:
        df = pd.DataFrame([])
    return df.to_dict('records')


@retry(delay=2, tries=50)
def set_objects(conn: swiftclient.Connection, date: str, all_objects: list, container: str, path: str) -> None:
    logger.debug(f'Setting object {container} {path}')
    if isinstance(all_objects, list):
        all_objects = pd.DataFrame(all_objects)
    gz_buffer = BytesIO()
    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        all_objects.to_json(TextIOWrapper(gz_file, 'utf8'), orient='records')
    conn.put_object(container=container, obj=get_notice_filename(date, path), contents=gz_buffer.getvalue())
    logger.debug('done')
    return

@retry(delay=2, tries=50)
def get_objects_raw(conn: swiftclient.Connection, path: str, container: str) -> list:
    try:
        df = pd.read_json(BytesIO(conn.get_object(container, path)[1]), compression='gzip')
    except:
        df = pd.DataFrame([])
    return df.to_dict('records')

@retry(delay=2, tries=50)
def set_objects_raw(conn: swiftclient.Connection, path: str, all_objects: list, container: str) -> None:
    logger.debug(f'Setting raw object {container} {path}')
    if isinstance(all_objects, list):
        all_objects = pd.DataFrame(all_objects)
    gz_buffer = BytesIO()
    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        all_objects.to_json(TextIOWrapper(gz_file, 'utf8'), orient='records')
    conn.put_object(container=container, obj=path, contents=gz_buffer.getvalue())
    logger.debug('done')
    return


def remove_elt_with_field(x: list, id_to_remove: str, field: str) -> str:
    for ix, e in enumerate(x):
        if e.get(field) == id_to_remove:
            return x.pop(ix)[field]


def get_nb_elt_with_field(x: list, id_to_look_for: str, field: str) -> int:
    ans = 0
    for ix, e in enumerate(x):
        if e is None:
            continue
        if e.get(field) == id_to_look_for:
            ans += 1
    return ans


def remove_all_elt_with_field(x: list, id_to_remove: str, field: str) -> None:
    n = get_nb_elt_with_field(x, id_to_remove, field)
    for i in range(0, n):
        remove_elt_with_field(x, id_to_remove, field)
