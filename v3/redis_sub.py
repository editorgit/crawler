import asyncio
import aioredis


async def handle_msg(msg):
    print('Got Message:', msg)
    i = int(msg['sleep_for'])
    print('Sleep for {}'.format(i))
    await asyncio.sleep(i)
    print('End sleep')


async def reader(ch):
    while (await ch.wait_message()):
        msg = await ch.get_json()
        asyncio.ensure_future(handle_msg(msg))


async def main():
    sub = await aioredis.create_redis(('localhost', 6379))
    res = await sub.subscribe('chan:1')
    ch1 = res[0]
    tsk = await reader(ch1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
