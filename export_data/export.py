import os, sys
import requests
import pprint
from fabric.connection import Connection

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import settings

from db_sync import get_cursor

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TABLES = ['domains', 'ip_addresses']

REMOTE_HOST = 'station@s3.amygoal.com'

REMOTE_PATH = '/home/station/code/import_spider_data'

# env.hosts = ['station@s3.amygoal.com']


def get_path(tld):
    dir_script = os.path.join(BASE_DIR, 'export_data')
    return os.path.join(dir_script, tld)


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
                print(f"Send file: {remote_dir}{table}.csv")


if __name__ == '__main__':

    get_snapshots()
    send_snapshots()
