import re
import time
import logging
import string
import json
import csv
import re
import asyncio
import aiohttp
from collections import defaultdict
from datetime import datetime

import settings


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

    def __init__(self, db_conn, create_conn_dict, concurrency=1, timeout=20,
                 delay=0, headers=None, verbose=True, cookies=None,
                 max_parse=0, retries=2, test=None, low_limit=10):


        self.headers = headers
        self.cookies = dict()
        self.client = ''

        self.concurrency = concurrency
        self.delay = delay
        self.retries = retries

        self.low_limit = low_limit
        self.lock_queue = False

        # self.q_crawl = BQueue(capacity=max_crawl)
        self.q_parse = BQueue(capacity=max_parse)

        self.counter = dict({'successful': 0, 'unsuccessful': 0})

        self.brief = defaultdict(set)
        self.data = dict()
        self.test = test

        self.db_conn_dict = None
        self.create_conn_dict = create_conn_dict

        self.can_parse = True
        self.db_list_request = list()

        self.domains = list()
        self.pages = list()
        self.backlinks = list()
        self.redirect_list = list()

        logging.basicConfig(level='WARNING')
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
        # if not self.db_list_request:
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
        if self.data['table'] == 'domains':
            title = self.data['title'].replace("'", "")
            sql = "UPDATE domains " \
                  "SET http_status_code=%s, title='%s', in_job=Null, last_visit_at='%s', len_content=%s " \
                  "WHERE ids=%s" % (self.data['http_status'], title, now,
                                    self.data.get('len_content', 0), self.data['ids'])
        else:
            sql = f"UPDATE {self.data['table']} " \
                  f"SET http_status_code={self.data['http_status']}, in_job=Null, last_visit_at='{now}' " \
                  f"WHERE ids={self.data['ids']}"
        return await self.db_conn_dict[str(self.data['db'])].execute(sql)

    async def insert_domains(self):
        # domains = str(tuple((domain,) for domain in self.domains))[1:-1]
        sql = f"""INSERT INTO domains (domain)
                    VALUES {self.domains} ON CONFLICT DO NOTHING"""
        return await self.db_conn_dict[str(self.data['db'])].execute(sql)

    async def insert_pages(self):
        sql = f"""INSERT INTO pages (domain_id, max_depth, ip_address, depth, page_url)
                            VALUES {self.pages} ON CONFLICT DO NOTHING"""
        return await self.db_conn_dict[str(self.data['db'])].execute(sql)

    async def insert_backlinks(self):
        sql = "INSERT INTO backlinks (donor_domain_id, donor_page_id, link_to, anchor, is_dofollow)" \
              "VALUES %s ON CONFLICT DO NOTHING" % self.backlinks
        return await self.db_conn_dict[str(self.data['db'])].execute(sql)

    async def insert_redirects(self, domain_id, redirects):
        domain_pk = domain_id if domain_id else self.data['ids']
        page_id = self.data['ids'] if domain_id else 0
        redirects = re.sub('['+string.punctuation+']', ' ', str(redirects)[:500])
        sql = "INSERT INTO redirects (domain_id, page_id, redirect_list, redirect_raw) " \
              "VALUES (%s, %s, '%s', '%s') " \
              "ON CONFLICT DO NOTHING" % (domain_pk, page_id, str(self.redirect_list), redirects)
        return await self.db_conn_dict[str(self.data['db'])].execute(sql)

    def get_parsed_content(self, url):
        """
        :param url: an url from which html will be parsed.
        :return: it has to return a dict with data.
        It must be a coroutine.
        """
        raise NotImplementedError

    async def get_html_from_url(self, url):
        # print(f"url: {url}")
        answer = dict()
        try:
            # async with aiohttp.request(method="GET", url=url, headers=self.headers) as response:
            async with self.client.get(url) as response:
                # print(f"response: {response}")
                if response.content_type.startswith('image'):
                    title = f"Content type: IMAGE"
                    self.log.info(title)
                    self.counter['unsuccessful'] += 1
                    answer['http_status'] = 71
                    answer['title'] = title
                    return answer

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
            answer['http_status'] = 51
            answer['title'] = "ClientConnectorError"
            return answer

        except aiohttp.client_exceptions.ServerDisconnectedError:
            self.counter['unsuccessful'] += 1
            answer['http_status'] = 52
            answer['title'] = "ServerDisconnectedError"
            return answer

        except aiohttp.client_exceptions.ClientOSError:
            self.counter['unsuccessful'] += 1
            answer['http_status'] = 53
            answer['title'] = "ClientOSError"
            return answer

        except aiohttp.client_exceptions.TooManyRedirects:
            self.counter['unsuccessful'] += 1
            answer['http_status'] = 54
            answer['title'] = "TooManyRedirects"
            return answer

        except aiohttp.client_exceptions.ClientResponseError:
            self.counter['unsuccessful'] += 1
            answer['http_status'] = 55
            answer['title'] = "ClientResponseError"
            return answer

        except aiohttp.client_exceptions.ClientPayloadError:
            self.counter['unsuccessful'] += 1
            answer['http_status'] = 56
            answer['title'] = "ClientPayloadError"
            return answer

        except ValueError:
            self.counter['unsuccessful'] += 1
            answer['http_status'] = 57
            answer['title'] = "URL should be absolute"
            return answer

    async def __wait(self, name):
        if self.delay > 0:
            self.log.info('{} waits for {} sec.'.format(name, self.delay))
            await asyncio.sleep(self.delay)

    async def parse_url(self):
        url_data = await self.q_parse.get()

        # self.log.info(f"Queue size: {self.q_parse.qsize()}")
        if not self.lock_queue and self.q_parse.qsize() < settings.LOW_LIMIT and not self.test:
            self.lock_queue = True
            self.log.warning(f"Queue lock")
            self.log.warning(f"Queue limit before {self.q_parse.qsize()}: {datetime.now()}")
            await self.get_urls4crawler()
            self.log.warning(f"Queue limit after {self.q_parse.qsize()}: {datetime.now()}")
            self.lock_queue = False
            self.log.warning(f"Queue unlock")

        self.log.info('Parsing: {}'.format(url_data))

        try:
            content = await self.get_parsed_content(url_data)
        except Exception:
            self.log.error(f'Error during parsing: {url_data}',
                           exc_info=True)
            # time.sleep(5)
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

    async def create_session(self):
        self.client = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(verify_ssl=False),
            headers=self.headers,
            cookies=self.cookies)

    async def run(self):
        start = time.time()

        self.log.warning(f'Start working: {datetime.now()}')
        await self.create_session()
        await self.create_connections()

        if self.test:
            await self.q_parse.put(self.test)
        else:
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

        # await aiohttp.ClientSession.close()

        end = time.time()
        print('Done in {} seconds'.format(end - start))

        # assert len(self.brief['parsing']) == len(self.data), \
        #     'Parsing length does not equal parsed length'

        self.log.info(f"Successful: {self.counter['successful']}")
        self.log.info(f"Unsuccessful: {self.counter['unsuccessful']}")
        self.log.info(f"Total parsed: {self.counter['unsuccessful'] + self.counter['successful']}")
        print('Task done!')
