import time
import logging
import json
import csv
import re
import asyncio
import aiohttp
from collections import defaultdict

from datetime import datetime


class BQueue(asyncio.Queue):
    """ Bureaucratic queue """

    def __init__(self, maxsize=0, capacity=0, *, loop=None):
        """
        :param maxsize: a default maxsize from tornado.queues.Queue,
            means maximum queue members at the same time.
        :param capacity: means a quantity of income tries before will refuse
            accepting incoming data
        """
        super().__init__(maxsize, loop=None)
        if capacity is None:
            raise TypeError("capacity can't be None")

        if capacity < 0:
            raise ValueError("capacity can't be negative")

        self.capacity = capacity
        self.put_counter = 0
        self.is_reached = False

    def put_nowait(self, item):

        if not self.is_reached:
            super().put_nowait(item)
            self.put_counter += 1

            if 0 < self.capacity == self.put_counter:
                self.is_reached = True


class WebSpider:

    def __init__(self, db_conn, create_conn_dict, concurrency=1, timeout=300,
                 delay=0, headers=None, verbose=True, cookies=None,
                 max_parse=0, retries=2):


        self.headers = headers
        if not cookies:
            cookies = dict()
        self.client = aiohttp.ClientSession(headers=headers, cookies=cookies)

        self.concurrency = concurrency
        self.delay = delay
        self.retries = retries

        # self.q_crawl = BQueue(capacity=max_crawl)
        self.q_parse = BQueue(capacity=max_parse)

        self.counter = dict({'successful': 0, 'unsuccessful': 0})

        self.brief = defaultdict(set)
        self.data = dict()

        self.db_conn_dict = None
        self.create_conn_dict = create_conn_dict

        self.can_parse = True

        self.domains = list()
        self.pages = list()
        self.backlinks = list()

        logging.basicConfig(level='INFO')
        self.log = logging.getLogger()

        if not verbose:
            self.log.disabled = True

    async def create_connections(self):
        """
        Create DB connections
        """
        self.db_conn_dict = await self.create_conn_dict()

    async def get_urls4crawler(self):
        """
        Get URLS from DB and put them in Queue
        """
        db_list_request = list()
        for tld, db_conn in self.db_conn_dict.items():
            db_list_request.append(db_conn.fetch_domains4crawler())
            db_list_request.append(db_conn.fetch_pages4crawler())

        for db_request in asyncio.as_completed(db_list_request):
            await self.update_queue(await db_request)

    async def update_queue(self, url_list):
        """
        :param url_list
        :update: queue
        """
        for url_data in url_list:
            url_data = dict(url_data)
            source_table = 'domains' if url_data.get('page_url', False) == False else 'pages'
            url_data.update({'table': source_table})
            await self.q_parse.put(dict(url_data))

    async def update_data(self):
        # if domain or pages
        now = datetime.now()
        sql = f"""UPDATE {self.data['table']} 
                SET http_status_code={self.data["http_status"]}, title='{self.data["title"]}', in_job=Null, 
                    last_visit_at='{now}', len_content={self.data.get("len_content", 0)} 
                WHERE ids={self.data["ids"]}"""
        return await self.db_conn_dict[str(self.data['db'])].execute(sql)

    async def insert_domains(self):
        domains = tuple(self.domains) if len(self.domains) > 1 else list(self.domains)[0]
        sql = f"""INSERT INTO {self.data['table']} (domain)
                    VALUES {domains} ON CONFLICT DO NOTHING"""
        return await self.db_conn_dict[str(self.data['db'])].execute(sql)

    async def insert_pages(self):
        sql = f"""INSERT INTO pages (domain_id, depth, page_url)
                            VALUES {self.pages} ON CONFLICT DO NOTHING"""
        return await self.db_conn_dict[str(self.data['db'])].execute(sql)

    async def insert_backlinks(self):
        sql = f"""INSERT INTO backlinks (domain_id, depth, page_url)
                            VALUES {self.pages} ON CONFLICT DO NOTHING"""
        return await self.db_conn_dict[str(self.data['db'])].execute(sql)

    async def insert_redirects(self):
        sql = f"""INSERT INTO redirects (domain_id, depth, page_url)
                            VALUES {self.pages} ON CONFLICT DO NOTHING"""
        return await self.db_conn_dict[str(self.data['db'])].execute(sql)

    def get_parsed_content(self, url):
        """
        :param url: an url from which html will be parsed.
        :return: it has to return a dict with data.
        It must be a coroutine.
        """
        raise NotImplementedError

    async def get_html_from_url(self, url):
        print(f"url: {url}")
        answer = dict()
        try:
            async with self.client.get(url) as response:
            # async with aiohttp.request(method="GET", url=url, headers=self.headers) as response:
                print(f"response: {response}")
                self.counter['successful'] += 1
                answer['document'] = await response.text()
                # answer['content'] = response.content
                answer['http_status'] = response.status
                answer['real_url'] = f"{str(response._url).split(response.host)[0]}{response.host}"
                answer['host'] = response.host
                answer['redirects'] = response.history
                # answer['ip_address'] = response.connection._protocol.transport.get_extra_info('peername')
                return answer

        except aiohttp.client_exceptions.ClientConnectorError:
            self.counter['unsuccessful'] += 1
            answer['http_status'] = 51
            answer['title'] = "ConnectionError"
            return answer

    async def __wait(self, name):
        if self.delay > 0:
            self.log.info('{} waits for {} sec.'.format(name, self.delay))
            await asyncio.sleep(self.delay)

    async def parse_url(self):
        url_data = await self.q_parse.get()
        self.log.info('Parsing: {}'.format(url_data))

        try:
            content = await self.get_parsed_content(url_data)
        except Exception:
            self.log.error('An error has occurred during parsing',
                           exc_info=True)
        finally:
            self.q_parse.task_done()

    async def parser(self):
        retries = self.retries
        while True:
            if self.can_parse:
                await self.parse_url()
            elif retries > 0:
                await asyncio.sleep(0.5)
                retries -= 1
            else:
                break
            await self.__wait('Parser')
        return

    async def run(self):
        start = time.time()

        print('Start working')

        await self.create_connections()

        await self.get_urls4crawler()

        def task_completed(future):
            # This function should never be called in right case.
            # The only reason why it is invoking is uncaught exception.
            exc = future.exception()
            if exc:
                self.log.error('Worker has finished with error: {} '
                               .format(exc), exc_info=True)

        tasks = []

        for _ in range(self.concurrency):
            fut_parse = asyncio.ensure_future(self.parser())
            fut_parse.add_done_callback(task_completed)
            tasks.append(fut_parse)

        await self.q_parse.join()

        for task in tasks:
            task.cancel()

        end = time.time()
        print('Done in {} seconds'.format(end - start))

        # assert len(self.brief['parsing']) == len(self.data), \
        #     'Parsing length does not equal parsed length'

        self.log.info(f"Successful: {self.counter['successful']}")
        self.log.info(f"Unsuccessful: {self.counter['unsuccessful']}")
        self.log.info(f"Total parsed: {self.counter['unsuccessful'] + self.counter['successful']}")
        print('Task done!')
