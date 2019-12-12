import asyncio
import asyncpg
from datetime import datetime, date, timedelta

import settings

class pg:
    def __init__(self, pg_pool):
        self.pg_pool = pg_pool

    async def execute(self, sql, *args, **kwargs):
        # print(f"sql_update: {sql}")
        async with self.pg_pool.acquire() as connection:
            return await connection.execute(sql, *args, **kwargs)

    async def fetchval(self, sql, *args, **kwargs):
        # print(f"SQL fetchval: {sql}")
        async with self.pg_pool.acquire() as connection:
            return await connection.fetchval(sql, *args, **kwargs)

    async def fetch_domains(self, *args, **kwargs):
        now = date.today()
        days_to_subtract = 30
        previous = now - timedelta(days=days_to_subtract)
        sql_select = (f"UPDATE domains SET in_job='{now}' "
                      "WHERE ids IN (SELECT ids FROM domains "
                      f"WHERE (ip_address IS NULL and last_visit_at IS NULL) or "
            f"(http_status_code = {settings.AIO_DNS_ERROR} and last_visit_at < '{previous}') "
                     "and use_level > 0 LIMIT 300) RETURNING domain")
        # print(f"sql_select: {sql_select}")
        async with self.pg_pool.acquire() as connection:
            return await connection.fetch(sql_select, *args, **kwargs)


async def init_pg(database, user, password):
    """
    Init Postgresql DB.
    """
    pg_pool = await asyncpg.create_pool(
        host=settings.DB_HOST,
        database=database,
        user=user,
        password=password,
        max_size=10,
    )
    return pg(pg_pool)


async def create_conn_dict():
    db_conn_dict = dict()
    for tld in settings.TLDS:
        db_conn_dict[tld] = await init_pg(f'spiderbase_{tld}',
                                          settings.DB_USER, settings.DB_PASS)

    return db_conn_dict
