import psycopg2

import settings


def get_cursor(database):
    conn = psycopg2.connect(dbname=database, user=settings.DB_USER,
                            password=settings.DB_PASS, host='localhost')
    return conn.cursor()


def get_connection(database):
    conn = psycopg2.connect(dbname=database, user=settings.DB_USER,
                            password=settings.DB_PASS, host='localhost')
    return conn
