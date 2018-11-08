import asyncio
import aiodns


class SwarmResolver:
    """ A simple class which will resolve a list of domains asyncrionously """

    def __init__(self, num_workers=2, nameservers=['8.8.8.8', '8.8.4.4'], qtype="A"):
        self.num_workers = num_workers
        self.nameservers = nameservers
        self.qtype = qtype
        self.results = {}

    async def resolve_list(self, loop, domain_list):
        resolver = aiodns.DNSResolver(loop=loop, nameservers=self.nameservers, timeout=2, tries=1)

        for domain in domain_list:
            try:
                res = await resolver.query(domain, self.qtype)
                self.results[domain] = res[0][0]
            except aiodns.error.DNSError as e:
                error_code = e.args[0]
                if error_code == aiodns.error.ARES_ECONNREFUSED:
                    self.results[domain] = "DNS ERROR: CONNECTION_REFUSED"
                elif error_code == aiodns.error.ARES_ENODATA:
                    self.results[domain] = "DNS ERROR: NODATA"
                elif error_code == aiodns.error.ARES_ENOTFOUND:
                    self.results[domain] = "DNS ERROR: NOTFOUND"
                elif error_code == aiodns.error.ARES_EREFUSED:
                    self.results[domain] = "DNS ERROR: REFUSED"
                elif error_code == aiodns.error.ARES_ESERVFAIL:
                    self.results[domain] = "DNS ERROR: SERVFAIL"
                elif error_code == aiodns.error.ARES_ETIMEOUT:
                    self.results[domain] = "DNS ERROR: TIMEOUT"
                else:
                    self.results[domain] = "DNS ERROR: UNKNOWN_STATUS"

            except Exception as e:
                print(e)

        return(self.results)
