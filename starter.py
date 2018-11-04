import asyncio
import aiohttp

import settings
from db import pg_ee, create_conn_dict, create_conn_list
from parser import NetCrawler


if __name__ == '__main__':

    test = None
    test = {'page_id': 558789, 'domain_id': 2112, 'page_url': 'http://fcflora.ee/kontakt/kes-on-kes/', 'depth': 2, 'max_depth': 2, 'ip_id': 3, 'table': 'pages'}

    concurrency = settings.MAX_THREADS
    low_limit = settings.LOW_LIMIT
    max_parse = 0
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
               "Accept": "text/html,application/xhtml+xml, application/xml;q=0.9,image/webp,*/*;q=0.8"}
    web_crawler = NetCrawler(pg_ee, create_conn_dict, concurrency, timeout=15, verbose=True,
                             headers=headers, max_parse=max_parse, test=test, low_limit=low_limit)


    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(web_crawler.run())
    finally:
        loop.close()
