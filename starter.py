import asyncio
import aiohttp

import settings
from db import create_conn_dict
from parser import NetCrawler


if __name__ == '__main__':

    test = None
    # test = {'domain_id': 51870, 'domain': 'agf.ee', 'max_depth': 2, 'ip_id': 253, 'table': 'domains'}

    concurrency = settings.MAX_THREADS
    low_limit = settings.LOW_LIMIT
    max_parse = 0
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
               "Accept": "text/html,application/xhtml+xml, application/xml;q=0.9,image/webp,*/*;q=0.8"}
    web_crawler = NetCrawler(create_conn_dict, concurrency, timeout=15, verbose=True,
                             headers=headers, max_parse=max_parse, test=test, low_limit=low_limit)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(web_crawler.run())
    finally:
        loop.close()
