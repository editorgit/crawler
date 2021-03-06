import pprint
import asyncio
import uvloop
import requests
from datetime import datetime

import sys
import os.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
import settings

from dns_resolver import SwarmResolver
from dns_db import create_conn_dict


class IpUpdate:

    def __init__(self, create_conn_dict, tld):
        self.create_conn_dict = create_conn_dict
        self.tld = tld
        self.swarm = SwarmResolver(qtype="A", num_workers=10)
        self.db_conn_dict = None

    async def create_connections(self):
        """
        Create DB connections
        """
        self.db_conn_dict = await self.create_conn_dict()

    async def get_domains(self):
        return await self.db_conn_dict[self.tld].fetch_domains()

    async def get_ip(self, loop):
        domains = await self.get_domains()
        if not domains:
            return
        domains_list = list(domain['domain'] for domain in domains)
        # pprint.pprint(domains_list)
        return await self.swarm.resolve_list(loop, domains_list)

    async def update_domain_ip(self, loop):
        await self.create_connections()
        ip_domains = await self.get_ip(loop)

        if ip_domains:
            now = datetime.now()

            for domain, value in ip_domains.items():
                if value.startswith('DNS ERROR'):
                    sql = "UPDATE domains SET in_job=Null, http_status_code=%s, ip_address =0, title='%s', last_visit_at='%s' "\
                          "WHERE domain='%s'" % (settings.AIO_DNS_ERROR, value, now, domain)
                    await self.db_conn_dict[self.tld].execute(sql)

                else:
                    sql = """INSERT INTO ip_addresses (ip_v4) VALUES ('%s') """ \
                          """ ON CONFLICT ("ip_v4") DO UPDATE SET counter = ip_addresses.counter + 1 """ \
                          """ RETURNING ids """ % value
                    id_ip = await self.db_conn_dict[self.tld].fetchval(sql)

                    sql = "UPDATE domains SET in_job=Null, http_status_code=Null, ip_address ='%s', title=Null, last_visit_at=Null " \
                          "WHERE domain='%s'" % (id_ip, domain)
                    await self.db_conn_dict[self.tld].execute(sql)


if __name__ == '__main__':
    ip_update = IpUpdate(create_conn_dict, 'ee')

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(ip_update.update_domain_ip(loop))
    finally:
        loop.close()
