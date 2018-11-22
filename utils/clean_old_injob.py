import pprint
import asyncio
import uvloop
import requests
from datetime import datetime, date, timedelta

import sys
import os.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
import settings

from db import create_conn_dict


class CleanInJob:

    def __init__(self, create_conn_dict):
        self.create_conn_dict = create_conn_dict
        self.db_conn_dict = None

    async def create_connections(self):
        """
        Create DB connections
        """
        self.db_conn_dict = await self.create_conn_dict()

    async def clean_old_in_job_status(self):
        await self.create_connections()

        now = date.today()

        sql_domains = f"UPDATE domains SET in_job=Null WHERE in_job < '{now}'"
        sql_pages = f"UPDATE pages SET in_job=Null WHERE in_job < '{now}'"

        # print(sql_domains)
        # print(sql_pages)

        for db in self.db_conn_dict:
            await self.db_conn_dict[db].execute(sql_domains)
            await self.db_conn_dict[db].execute(sql_pages)

        return 


if __name__ == '__main__':
    clean_job = CleanInJob(create_conn_dict)

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(clean_job.clean_old_in_job_status())
    finally:
        loop.close()
