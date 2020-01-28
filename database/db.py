import time
import asyncio
import asyncpg
from datetime import datetime

import dateutil.relativedelta

import settings


class PgActions:

    now = datetime.now()
    repeated_period = now - dateutil.relativedelta.relativedelta(months=4)

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
        logger.info(f"DOMAINS request to {self.pg_pool._connect_kwargs['database']}")
        sql_select = f"""UPDATE domains SET in_job='{self.now}' 
                         WHERE ids IN 
            (SELECT DISTINCT ON (ip_address) ids FROM \
                (SELECT * FROM domains WHERE use_level > 0 and ip_address IS NOT NULL and in_job IS NULL and \
                    last_visit_at IS NULL OR last_visit_at < '{self.repeated_period.strftime("%Y-%m-%d")}' LIMIT 10000) 
                        AS domain_table LIMIT {settings.MAX_DOMAINS})
                         RETURNING ids AS domain_id, domain, max_depth, ip_address AS ip_id"""
        logger.debug(f"sql_select: {sql_select}")
        # print(f"sql_select: {sql_select}")
        async with self.pg_pool.acquire() as connection:
            return await connection.fetch(sql_select, *args, **kwargs)

    async def fetch_pages4crawler(self, logger, *args, **kwargs):
        # asyncio.sleep(0.5)
        # time.sleep(0.5)
        logger.info(f"PAGES request to {self.pg_pool._connect_kwargs['database']}")
        now = datetime.now()
        repeated_period = now - dateutil.relativedelta.relativedelta(months=6)
        sql_select = f"""UPDATE pages SET in_job='{now}' 
                               WHERE ids IN 
                    (SELECT DISTINCT ON (ip_address) ids FROM (SELECT * FROM pages WHERE in_job IS NULL and \
                        last_visit_at IS NULL and (last_visit_at IS NULL OR \
                        last_visit_at < '{repeated_period.strftime("%Y-%m-%d")}' AND depth = 1)
                        ORDER BY depth ASC
                        LIMIT 1000000) AS pages_table LIMIT {settings.MAX_PAGES})
                    RETURNING ids AS page_id, domain_id, page_url, depth, max_depth, ip_address AS ip_id"""
        logger.debug(f"sql_select: {sql_select}")
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
    return PgActions(pg_pool)


async def create_conn_dict():
    db_conn_dict = dict()
    for tld in settings.TLDS:
        db_conn_dict[tld] = await init_pg(
            f'spiderbase_{tld}',
            settings.DB_USER,
            settings.DB_PASS
        )
    return db_conn_dict
