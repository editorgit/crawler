import asyncio

import settings
from db import pg_ee, create_conn_dict, create_conn_list
from parser import NetCrawler


if __name__ == '__main__':

    test = None

    # with open('/home/spidermen/spider/example/page.html', 'r') as f:
    #     test = f.read()

    concurrency = settings.MAX_THREADS
    max_parse = 10
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) "
                             "AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
               "Accept": "text/html,application/xhtml+xml,"
                         "application/xml;q=0.9,image/webp,*/*;q=0.8"}
    web_crawler = NetCrawler(pg_ee, create_conn_dict, concurrency, timeout=60, verbose=True,
                             headers=headers, max_parse=max_parse, test=test)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(web_crawler.run())
    finally:
        loop.close()
