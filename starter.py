import asyncio
import itertools as it
import os
import random
import time
import logging

import settings
from database.get_urls import GetUrls

from requester import Requester

logging.basicConfig(filename=settings.LOGGER_PATH, level=settings.LOGGER_LEVEL, format=settings.LOGGER_FORMAT)
LOGGER = logging.getLogger()


def task_completed(future):
    # This function should never be called in right case.
    # The only reason why it is invoking is uncaught exception.
    exc = future.exception()
    if exc:
        LOGGER.error('Worker has finished with error: {} '
                       .format(exc), exc_info=True)


async def start_produce(requester: Requester, queue: asyncio.Queue) -> None:
    await update_queue(requester.db_connector, queue)


async def update_queue(db_connector: GetUrls, queue: asyncio.Queue) -> None:
    await db_connector.get_urls4crawler(queue)


async def consume(name: int, requester: Requester, queue: asyncio.Queue, lock: asyncio.Lock) -> None:

    while True:
        url_data = await queue.get()

        # Periodical log
        if queue.qsize() % 500 == 0:
            LOGGER.info(f"Queue size: {queue.qsize()}")
            # LOGGER.info(f"Consumer {name} got {url_data} from queue.")

        if queue.qsize() < settings.LOW_LIMIT:
            with await lock:
                await update_queue(requester.db_connector, queue)

        LOGGER.debug('Parsing: {}'.format(url_data))

        try:
            await requester.url_handler(url_data)
        except Exception:
            LOGGER.error(f'\n Error during parsing: {url_data}', exc_info=True)
        finally:
            queue.task_done()


async def main():
    # init queue
    queue = asyncio.Queue()
    lock = asyncio.Lock()

    # init DB connector
    db_connector = GetUrls()
    await db_connector.create_connections(LOGGER)

    # init URL handler
    requester = Requester(LOGGER, db_connector)

    # Producer
    await start_produce(requester, queue)

    # Consumers
    consumers = [asyncio.create_task(consume(n, requester, queue, lock))\
                 .add_done_callback(task_completed) for n in range(1, settings.MAX_THREADS)]
    await queue.join()  # Implicitly awaits consumers

    for cons in consumers:
        cons.cancel()

if __name__ == "__main__":
    start = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - start
    print(f"Spider completed in {elapsed:0.5f} seconds.")
