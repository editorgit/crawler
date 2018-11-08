import uvloop
import asyncio
import sys
import os.path

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from dns_db import create_conn_dict
from update_ip import IpUpdate


if __name__ == '__main__':
    ip_update = IpUpdate(create_conn_dict, 'lt')

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(ip_update.update_domain_ip(loop))
    finally:
        loop.close()
