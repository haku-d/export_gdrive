import logging
import sys
from os import environ as env
from urllib.parse import urlencode
import requests
import concurrent.futures


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


PAGE_SIZE = 1000
API_KEY = env.get('API_KEY')
DRIVE_FILE_MIME_TYPES = {
    'g_file': 'application/vnd.google-apps.file',
    'g_folder': 'application/vnd.google-apps.folder'
}


def is_valid_drive_id(drive_id):
    return drive_id and drive_id.strip() != ''


def is_drive_file_type(mime):
    return mime == DRIVE_FILE_MIME_TYPES['g_file']


def is_drive_folder_type(mime):
    return mime == DRIVE_FILE_MIME_TYPES['g_folder']


def get_files(drive_id, api_key, next_page_token=None):
    query = {
        'orderBy': 'folder desc',  # trying to list all files on first
        'pageSize': PAGE_SIZE,
        'key': api_key,
        'q': '"%s" in parents' % (drive_id)
    }

    if next_page_token is not None:
        query['pageToken'] = next_page_token

    api_url = 'https://www.googleapis.com/drive/v3/files?%s' % urlencode(
        query)
    r = requests.get(api_url,
                     headers={"Accept": "application/json"})

    if r.status_code != 200:
        return [], [], None

    content = r.json()
    files = list(
        filter(lambda file: not is_drive_folder_type(file['mimeType']),
               content.get('files')))
    folders = list(
        filter(lambda file: is_drive_folder_type(file['mimeType']),
               content.get('files')))
    next_page_token = content.get('nextPageToken', None)
    return files, folders, next_page_token


def extract_drive(drive_id):
    folder_ids = [{
        'drive_id': drive_id,
        'next_page_token': None
    }]

    while len(folder_ids) > 0:
        with concurrent.futures.ThreadPoolExecutor(max_workers=None) as worker:
            futures = {
                worker.submit(
                    get_files,
                    item['drive_id'],
                    API_KEY,
                    item['next_page_token']): item for item in
                folder_ids[:10]}
            for future in concurrent.futures.as_completed(futures):
                item = futures[future]
                (files, folders, page_token) = future.result()
                if len(files) > 0:
                    yield [file['id'] for file in files]
                if page_token:
                    item['next_page_token'] = page_token
                else:
                    folder_ids.remove(item)

                for folder in folders:
                    folder_ids.append({
                        'drive_id': folder['id'],
                        'next_page_token': None
                    })


if __name__ == '__main__':
    drive_id = sys.argv[1] if len(sys.argv) > 1 else None
    filename = '{}.csv'.format(drive_id)
    total_file = 0

    if not is_valid_drive_id(drive_id):
        logger.info('input drive is not valid')
        sys.exit()

    with open(filename, mode='w+') as f:
        for drives in extract_drive(drive_id):
            total_file += len(drives)
            logger.info('exported {} files'.format(total_file))
            f.write('\n'.join(drives) + '\n')
