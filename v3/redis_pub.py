import asyncio
import aioredis


async def update_main_queue():
    publisher = await aioredis.create_redis(
        'redis://localhost')

    # res = await publisher.publish_json('main_queue', {"sleep_for": 5})
    res = await publisher.publish('main_queue', 'Update')
    assert res == 1

async def main():
    publisher = await aioredis.create_redis(
        'redis://localhost')

    # res = await publisher.publish_json('main_queue', {"sleep_for": 5})
    res = await publisher.publish('chan:main', 'Update')
    assert res == 1


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
