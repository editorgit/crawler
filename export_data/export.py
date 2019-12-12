#!/usr/bin/env python
import os, sys
import logging
import datetime
import pprint

import requests
from fabric.connection import Connection

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import settings

from db_sync import get_cursor

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORT_DIR = 'export_data'
DIR_SCRIPT = os.path.join(BASE_DIR, EXPORT_DIR)

TABLES = ['domains', 'ip_addresses']

REMOTE_HOST = 'station@s3.amygoal.com'

REMOTE_PATH = '/home/station/code/import_spider_data'


def get_path(tld):
    return os.path.join(DIR_SCRIPT, tld)


def get_snapshots():
    for tld in settings.TLDS:

        dir_tld = get_path(tld)
        cursor = get_cursor(f'spiderbase_{tld}')

        if not os.path.exists(dir_tld):
            os.makedirs(dir_tld)

        for table in TABLES:

            select = f"SELECT * FROM {table}"

            # sql = f"COPY ({select}) TO STDOUT WITH CSV DELIMITER ';' HEADER;"
            sql = f"COPY ({select}) TO STDOUT WITH CSV DELIMITER ';';"

            with open(f"{dir_tld}/{table}.csv", "w") as file:
                cursor.copy_expert(sql, file)

        cursor.close()


def send_snapshots():
    for tld in settings.TLDS:

        dir_tld = get_path(tld)

        for table in TABLES:

            # make sure the directory is there!
            # run('mkdir -p /home/frodo/tmp')

            remote_dir = f"{REMOTE_PATH}/{tld}/"

            # print(remote_dir)
            # print(f"{dir_tld}/{table}.csv")

            # our local 'testdirectory' - it may contain files or subdirectories ...
            with Connection(REMOTE_HOST) as conn:
                conn.run('mkdir -p ' + remote_dir)
                # print("Create directory")

                conn.put(f"{dir_tld}/{table}.csv", remote_dir)

                time_tag = datetime.datetime.now().strftime('%d.%m.%Y %H:%M')
                print(f"Send file {time_tag}: {remote_dir}{table}.csv")
                logger.info(f"Send file {time_tag}: {remote_dir}{table}.csv")


if __name__ == '__main__':
    log_path = os.path.join(DIR_SCRIPT, 'export.log')
    print(f"log_path: {log_path}")
    logging.basicConfig(filename=log_path, level=logging.INFO)
    logger = logging.getLogger()
    time_tag = datetime.datetime.now().strftime('%d.%m.%Y %H:%M')
    logging.warning('')
    logging.warning(f'SEND FILES START: {time_tag}')

    get_snapshots()
    send_snapshots()

    time_tag = datetime.datetime.now().strftime('%d.%m.%Y %H:%M')
    logging.warning(f'SEND FILES FINISHED: {time_tag}')
