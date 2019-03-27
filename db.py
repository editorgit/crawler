import time
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
            # print(sql)
            return await connection.execute(sql, *args, **kwargs)

    async def fetch_domains4crawler(self, logger, *args, **kwargs):
        now = datetime.now()
        logger.info("DOMAINS request to DB")
        sql_select = f"""UPDATE domains SET in_job='{now}' 
                         WHERE ids IN 
            (SELECT DISTINCT ON (ip_address) ids FROM (SELECT * FROM domains WHERE use_level > 0 and ip_address IS NOT NULL and in_job IS NULL and last_visit_at IS NULL LIMIT 10000) AS domain_table LIMIT {settings.MAX_DOMAINS})
                         RETURNING ids AS domain_id, domain, max_depth, ip_address AS ip_id"""
        logger.info(f"sql_select: {sql_select}")
        async with self.pg_pool.acquire() as connection:
            return await connection.fetch(sql_select, *args, **kwargs)


    async def fetch_pages4crawler(self, logger, *args, **kwargs):
        # asyncio.sleep(0.5)
        # time.sleep(0.5)
        logger.info("PAGES request to DB")
        now = datetime.now()
        sql_select = f"""UPDATE pages SET in_job='{now}' 
                               WHERE ids IN 
                    (SELECT DISTINCT ON (ip_address) ids FROM (SELECT * FROM pages WHERE in_job IS NULL and last_visit_at IS NULL LIMIT 1000000) AS pages_table LIMIT {settings.MAX_PAGES})
                     RETURNING ids AS page_id, domain_id, page_url, depth, max_depth, ip_address AS ip_id"""
        logger.info(f"sql_select: {sql_select}")
        async with self.pg_pool.acquire() as connection:
            return await connection.fetch(sql_select, *args, **kwargs)


async def init_pg(database, user):
    """
    Init Postgresql DB.
    """
    pg_pool = await asyncpg.create_pool(
        database=database,
        user=user,
        max_size=10,
    )
    return pg(pg_pool)


async def create_conn_dict():
    db_conn_dict = dict()
    for tld in settings.TLDS:
        db_conn_dict[tld] = await init_pg(f'spiderbase_{tld}', 'spidermen')

    return db_conn_dict
