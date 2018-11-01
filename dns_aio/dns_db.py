import asyncio
import asyncpg
from datetime import datetime


class pg:
    def __init__(self, pg_pool):
        self.pg_pool = pg_pool

    async def execute(self, sql, *args, **kwargs):
        print(f"sql_update: {sql}")
        async with self.pg_pool.acquire() as connection:
            return await connection.execute(sql, *args, **kwargs)

    async def fetchval(self, sql, *args, **kwargs):
        # print(f"SQL fetchval: {sql}")
        async with self.pg_pool.acquire() as connection:
            return await connection.fetchval(sql, *args, **kwargs)

    async def fetch_domains(self, *args, **kwargs):
        now = datetime.now()
        sql_select = f"""UPDATE domains SET in_job='{now}' 
                         WHERE ids IN 
            (SELECT ids FROM domains WHERE ip_address IS NULL and use_level > 0 LIMIT 300)
                         RETURNING domain"""
        # print(f"sql_select: {sql_select}")
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
    db_conn_dict['ee'] = await init_pg('spiderbase_ee', 'spidermen')

    return db_conn_dict
