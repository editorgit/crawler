import asyncio
import aiodns

loop = asyncio.get_event_loop()
resolver = aiodns.DNSResolver(loop=loop)
f = resolver.query('veneteater.ee','AAAA')
# f = resolver.gethostbyname('veneteater.ee','A')
result = loop.run_until_complete(f)
print(result)