import asyncio
from typing import Dict, List

from database.db import create_conn_dict

import settings


class GetUrls:

    log = None
    db_conn_dict: Dict = dict()

    async def create_connections(self, logger):
        """
        Create DB connections
        """
        self.log = logger
        self.db_conn_dict = await create_conn_dict()

    # async def _create_redis(self):
    #     self.publisher = await aioredis.create_redis(f'redis://{settings.DB_HOST}', db=1)
    #     self.subscriber = await aioredis.create_redis(f'redis://{settings.DB_HOST}', db=2)
        # self.subscriber = await subscriber.subscribe('chan:queue')

    async def get_urls4crawler(self, queue: asyncio.Queue) -> None:
        """
        Get URLS from DB and put them in Queue
        """
        db_list_request = list()
        for tld, db_conn in self.db_conn_dict.items():
            db_list_request.append(db_conn.fetch_domains4crawler(self.log))
            db_list_request.append(db_conn.fetch_pages4crawler(self.log))

        for db_request in asyncio.as_completed(db_list_request):
            await self._update_queue(queue, await db_request)

        self.log.warning(f"Queue start/update {queue.qsize()}")

    @staticmethod
    async def _update_queue(queue: asyncio.Queue, url_list: List) -> None:
        """
        :param url_list
        :update: queue
        """
        for url_data in url_list:
            url_data = dict(url_data)
            source_table = 'domains' if url_data.get('page_url', False) is False else 'pages'
            url_data.update({'table': source_table})
            await queue.put(dict(url_data))
