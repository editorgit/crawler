import asyncio
import aiohttp
from typing import Tuple, Dict

from tldextract import tldextract

from parser.parser import PageParser

import settings


class Requester:

    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit 537.36 (KHTML, like Gecko) Chrome", "Accept": "text/html,application/xhtml+xml, application/xml;q=0.9,image/webp,*/*;q=0.8"}
    client = None
    timeout = 15
    cookies = ''

    def __init__(self, logger, db_connector):
        self.log = logger
        self.db_connector = db_connector
        self.client = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(verify_ssl=False),
            headers=self.headers,
            cookies=self.cookies,
            conn_timeout=self.timeout)

    async def url_handler(self, url_data: Dict) -> None:
        url_to_parse = await self.get_correct_url(url_data)
        domain_url, page_tld = await self.get_url_data(url_to_parse)

        # exclude domain or page with incorrect TLD
        if page_tld not in settings.TLDS:
            return

        # get page
        answer = await self.request_url(url_to_parse)
        # parse page
        await PageParser().parse_content(self.db_connector.db_conn_dict, url_data, answer, domain_url, page_tld)

    async def request_url(self, url: str) -> Dict:
        answer = dict()
        try:
            async with self.client.get(url, timeout=self.timeout) as response:
                # print(f"response: {response}")
                if response.content_type.startswith('image'):
                    title = f"Content type: IMAGE"
                    self.log.debug(title)
                    return {'http_status': 71, 'title': title}

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
                return answer

        except aiohttp.ClientConnectorError:
            return {'http_status': 51, 'title': 'ClientConnectorError'}

        except aiohttp.ServerDisconnectedError:
            return {'http_status': 52, 'title': 'ServerDisconnectedError'}

        except aiohttp.ClientOSError:
            return {'http_status': 53, 'title': 'ClientOSError'}

        except aiohttp.TooManyRedirects:
            return {'http_status': 54, 'title': 'TooManyRedirects'}

        except aiohttp.ClientResponseError:
            return {'http_status': 55, 'title': 'ClientResponseError'}

        except aiohttp.ClientPayloadError:
            return {'http_status': 56, 'title': 'ClientPayloadError'}

        except ValueError:
            return {'http_status': 57, 'title': 'URL should be absolute'}

        except asyncio.TimeoutError:
            return {'http_status': 58, 'title': 'asyncio.TimeoutError'}

    @staticmethod
    async def get_url_data(url_to_parse: str) -> Tuple:
        domain_url = '.'.join(tldextract.extract(url_to_parse)[1:3])
        page_tld = tldextract.extract(url_to_parse)[2]
        return domain_url, page_tld

    @staticmethod
    async def get_correct_url(url_data: Dict) -> str:

        if url_data.get('table', None) == 'domains':
            return f"http://{url_data['domain']}"

        if url_data.get('page_url', '').startswith('//'):
            return f"http:{url_data['page_url']}"

        if not url_data['page_url'].startswith('http'):
            return f"http://{url_data['page_url']}"

        return url_data['page_url']
