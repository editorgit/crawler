import asyncio
import asyncpg
from datetime import datetime

import settings


class pg:
    def __init__(self, pg_pool):
        self.pg_pool = pg_pool

    async def fetch(self, sql, *args, **kwargs):
        async with self.pg_pool.acquire() as connection:
            return await connection.fetch(sql, *args, **kwargs)

    async def execute(self, sql, *args, **kwargs):
        async with self.pg_pool.acquire() as connection:
            return await connection.execute(sql, *args, **kwargs)

    async def fetch_domains4crawler(self, *args, **kwargs):
        now = datetime.now()
        sql_select = f"""UPDATE domains SET in_job='{now}' 
                         WHERE ids IN 
            (SELECT ids FROM domains WHERE use_level > 0 and in_job IS NULL and last_visit_at IS NULL LIMIT 10)
                         RETURNING ids AS domain_id, domain"""
        print(f"sql_select: {sql_select}")
        async with self.pg_pool.acquire() as connection:
            return await connection.fetch(sql_select, *args, **kwargs)


    async def fetch_pages4crawler(self, *args, **kwargs):
        now = datetime.now()
        sql_select = f"""UPDATE pages SET in_job='{now}' 
                               WHERE ids IN 
                    (SELECT ids FROM pages WHERE in_job IS NULL and last_visit_at IS NULL LIMIT 10)
                               RETURNING ids AS pages_id, domain_id, page_url, depth"""
        print(f"sql_select: {sql_select}")
        async with self.pg_pool.acquire() as connection:
            return await connection.fetch(sql_select, *args, **kwargs)


async def init_pg():
    """
    Init Postgresql DB.
    """
    pg_pool = await asyncpg.create_pool(
        database='spiderbase_ee',
        user='spidermen',
        max_size=100,
    )
    return pg(pg_pool)


db_dict = dict()


async def pg_ee():
    """
    Init Postgresql DB.
    """
    pg_pool = await asyncpg.create_pool(
        database='spiderbase_ee',
        user='spidermen',
        max_size=20,
    )
    return pg(pg_pool)

async def create_conn_list():
    db_conn_list = list()
    db_conn_list.append(await pg_ee())

    return db_conn_list

async def create_conn_dict():
    db_conn_dict = dict()
    db_conn_dict['ee'] = await pg_ee()

    return db_conn_dict

# loop = asyncio.get_event_loop()
# db_conn = loop.run_until_complete(init_pg())
