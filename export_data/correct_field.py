import os, sys
import requests
import pprint
import psycopg2
from fabric.connection import Connection

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import settings

from db_sync import get_connection


def update_field(action):
    for tld in settings.TLDS:

        conn = get_connection(f'spiderbase_{tld}')
        cursor = conn.cursor()

        table = 'domains'
        field = 'domain'
        trash = '\n'

        sql = f"SELECT ids, {field} FROM {table} WHERE {field} LIKE '%{trash}%'"

        print(f"SQL: {sql}")


        cursor.execute(sql)

        results = list()
        for row in cursor:
            results.append(row)

        cursor.close()
        # print(cursor)

        cursor_update = conn.cursor()
        # cnt = 0
        for row in results:
            # cnt += 1
            print(row)
            field_value = row[1].replace(trash, ' ')
            # print(f"TITLE: {repr(title)}")

            if action == 'replace':
                sql2 = f"UPDATE {table} SET {field} ='{field_value}' WHERE ids = {row[0]}"

            elif action == 'delete':
                sql2 = f"DELETE FROM {table} WHERE ids = {row[0]}"

            cursor_update.execute(sql2)
            conn.commit()

        cursor_update.close()

        conn.close()


if __name__ == '__main__':
    update_field('replace')
    # update_field('delete')
