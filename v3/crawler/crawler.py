import os
import time
import logging
import json
import csv
import re
import asyncio
import aioredis
from typing import Tuple

import aiohttp
from collections import defaultdict
from datetime import datetime

from tldextract import tldextract

import settings
from parser.parser import PageParser

logging.basicConfig(filename=settings.LOG_CRAWLER, level=settings.LOGGER_LEVEL, format=settings.FORMAT)


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

    html_page: str = ""
    parser = PageParser

    def __init__(self, create_conn_dict, concurrency=1, timeout=20,
                 delay=0, headers=None, verbose=True, cookies=None,
                 max_parse=0, retries=2, test=None, low_limit=10):

        self.headers = headers
        self.cookies = dict()
        self.client = ''
        self.timeout = timeout

        self.concurrency = concurrency
        self.delay = delay
        self.retries = retries
        self.lock_queue = False
        self.sleep = 0

        # self.q_crawl = BQueue(capacity=max_crawl)
        self.q_parse = BQueue(capacity=max_parse)
        self.publisher = None
        self.subscriber = None

        self.counter = dict({'successful': 0, 'unsuccessful': 0})
        self.test = test

        self.db_conn_dict = dict()
        self.create_conn_dict = create_conn_dict

        self.can_parse = True
        self.log = logging.getLogger()

        if not verbose:
            self.log.disabled = True

    async def _create_connections(self):
        """
        Create DB connections
        """
        self.db_conn_dict = await self.create_conn_dict()

    async def _create_redis(self):
        self.publisher = await aioredis.create_redis(f'redis://{settings.DB_HOST}', db=1)
        self.subscriber = await aioredis.create_redis(f'redis://{settings.DB_HOST}', db=2)
        # self.subscriber = await subscriber.subscribe('chan:queue')

    async def get_urls4crawler(self):
        """
        Get URLS from DB and put them in Queue
        """
        db_list_request = list()
        for tld, db_conn in self.db_conn_dict.items():
            db_list_request.append(db_conn.fetch_domains4crawler(self.log))
            db_list_request.append(db_conn.fetch_pages4crawler(self.log))

        for db_request in asyncio.as_completed(db_list_request):
            await self._update_queue(await db_request)

        self.log.warning(f"Queue start {self.q_parse.qsize()}")

    async def _update_queue(self, url_list):
        """
        :param url_list
        :update: queue
        """
        for url_data in url_list:
            url_data = dict(url_data)
            source_table = 'domains' if url_data.get('page_url', False) is False else 'pages'
            url_data.update({'table': source_table})
            await self.q_parse.put(dict(url_data))

    async def parse(self, url_data):
        url_to_parse = await self._get_correct_url(url_data)
        domain_url, page_tld = await self._get_url_data(url_to_parse)

        # exclude domain or page with incorrect TLD
        if page_tld not in settings.TLDS:
            return

        # get page
        answer = await self.request_url(url_to_parse)
        # parse page
        await self.parser().parse_content(self.db_conn_dict, url_data, answer, domain_url, page_tld)

    @staticmethod
    async def _get_url_data(url_to_parse) -> Tuple:
        domain_url = '.'.join(tldextract.extract(url_to_parse)[1:3])
        page_tld = tldextract.extract(url_to_parse)[2]
        return domain_url, page_tld

    async def request_url(self, url):
        answer = dict()
        try:
            # async with aiohttp.request(method="GET", url=url, headers=self.headers) as response:
            async with self.client.get(url, timeout=self.timeout) as response:
                # print(f"response: {response}")
                if response.content_type.startswith('image'):
                    title = f"Content type: IMAGE"
                    self.log.debug(title)
                    self.counter['unsuccessful'] += 1
                    return answer.update({'http_status': 71, 'title': title})

                try:
                    html = await response.text()
                except UnicodeDecodeError:
                    html = await response.text(encoding='Latin-1')
                answer['document'] = html

                # answer['content'] = await response.content.read()
                answer['http_status'] = response.status
                answer['real_url'] = f"{str(response._url).split(response.host)[0]}{response.host}"
                answer['host'] = response.host
                answer['redirects'] = response.history
                self.counter['successful'] += 1
                return answer

        except aiohttp.client_exceptions.ClientConnectorError:
            self.counter['unsuccessful'] += 1
            return {'http_status': 51, 'title': 'ClientConnectorError'}

        except aiohttp.client_exceptions.ServerDisconnectedError:
            self.counter['unsuccessful'] += 1
            return {'http_status': 52, 'title': 'ServerDisconnectedError'}

        except aiohttp.client_exceptions.ClientOSError:
            self.counter['unsuccessful'] += 1
            return {'http_status': 53, 'title': 'ClientOSError'}

        except aiohttp.client_exceptions.TooManyRedirects:
            self.counter['unsuccessful'] += 1
            return {'http_status': 54, 'title': 'TooManyRedirects'}

        except aiohttp.client_exceptions.ClientResponseError:
            self.counter['unsuccessful'] += 1
            return {'http_status': 55, 'title': 'ClientResponseError'}

        except aiohttp.client_exceptions.ClientPayloadError:
            self.counter['unsuccessful'] += 1
            return {'http_status': 56, 'title': 'ClientPayloadError'}

        except ValueError:
            self.counter['unsuccessful'] += 1
            return {'http_status': 57, 'title': 'URL should be absolute'}

        except asyncio.TimeoutError:
            self.counter['unsuccessful'] += 1
            return {'http_status': 58, 'title': 'asyncio.TimeoutError'}

    async def __wait(self, name):
        if self.delay > 0:
            self.log.debug('{} waits for {} sec.'.format(name, self.delay))
            await asyncio.sleep(self.delay)

    @staticmethod
    async def _get_correct_url(url_data):
        if url_data.get('table', None) == 'domains':
            return f"http://{url_data['domain']}"

        if url_data['page_url'].startswith('//'):
            return f"http:{url_data['page_url']}"

        if not url_data['page_url'].startswith('http'):
            return f"http://{url_data['page_url']}"

        return url_data['page_url']

    async def parse_url(self):
        url_data = await self.q_parse.get()
        # url_data = await self.subscriber.get_json()
        # url_data = self.subscriber.rpop('queue', encoding='utf-8')

        if self.q_parse.qsize() % 10 == 0:
            self.log.info(f"Queue size: {self.q_parse.qsize()}")

        # Pause for export data
        current_time = datetime.now()
        start_pause = current_time.replace(hour=2, minute=00)
        end_pause = current_time.replace(hour=2, minute=20)

        if end_pause > current_time > start_pause:
            time.sleep(60*20)

        # TODO: need replace with Redis
        if not self.lock_queue and self.q_parse.qsize() < settings.LOW_LIMIT and not self.test:
            # self.publisher.publish('chan:main', 'Update')

            self.lock_queue = True
            start = self.q_parse.qsize()
            start_time = datetime.now()
            self.log.warning(f"Queue update {start}: {start_time}")
            await self.get_urls4crawler()
            stop = self.q_parse.qsize()  # if stop == 0, after update queue, need pause
            stop_time = datetime.now()
            self.log.warning(f"Queue after update {stop - start}: {stop_time - start_time}\n")
            self.lock_queue = False

            if stop == 0:
                self.sleep += 5 * 60 # increase for 5 minute each time when stop == 0
                time.sleep(self.sleep)
                self.log.warning("Queue size is 0, sleep 5 minute")
            else:
                self.sleep = 0

            if self.sleep > 60 * 60:
                self.sleep = 60 * 60 # limit for sleep parameter: maximum 1 hour
                self.log.warning("self.sleep is too big, sleep 60 minute")

        self.log.debug('Parsing: {}'.format(url_data))

        try:
            content = await self.parse(url_data)
        except Exception:
            self.log.error(f'\n Error during parsing: {url_data}',
                           exc_info=True)
            # time.sleep(5)
        finally:
            self.q_parse.task_done()

    async def _requester(self):
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

    async def _create_session(self):
        self.client = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(verify_ssl=False),
            headers=self.headers,
            cookies=self.cookies,
            conn_timeout=self.timeout)

    async def run(self):
        start = time.time()

        self.log.warning(f'Start spider')
        await self._create_session()
        await self._create_connections()
        await self._create_redis()

        if self.test:
            await self.q_parse.put(self.test)
        else:
            await self.get_urls4crawler()
            # await self.publisher.publish('chan:main', 'Update')

        def task_completed(future):
            # This function should never be called in right case.
            # The only reason why it is invoking is uncaught exception.
            exc = future.exception()
            if exc:
                self.log.error('Worker has finished with error: {} '
                               .format(exc), exc_info=True)

        # fut_list = []

        self.log.warning(f'Concurrency: {self.concurrency}')

        # for _ in range(self.concurrency):
        #     fut_parse = asyncio.create_task(self._requester()).add_done_callback(task_completed)
        #     fut_list.append(fut_parse)

        tasks = await asyncio.gather(asyncio.create_task(self._requester()) \
                                     .add_done_callback(task_completed) for _ in range(self.concurrency))

        await self.q_parse.join()

        for task in tasks:
            task.cancel()

        # await aiohttp.ClientSession.close()

        end = time.time()
        self.log.info('Done in {} seconds'.format(end - start))
        self.log.info(f"Successful: {self.counter['successful']}")
        self.log.info(f"Unsuccessful: {self.counter['unsuccessful']}")
        self.log.info(f"Total parsed: {self.counter['unsuccessful'] + self.counter['successful']}")
        self.log.info('Task done!')
