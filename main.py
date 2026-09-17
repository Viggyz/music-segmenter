import asyncio

import logging
import sys
import os
from multiprocessing import Process, Queue

from utils.stream_processing import StreamProcesser
from utils.file import make_folder
from utils.rupture_segmenter import RuptureSegmenter
from utils.radio_browser_url_fetcher import RadioBrowserUrlFetcher

LOG_DIR = "logs"


def setup_process_logging(process_identifier):
    """
    Configures logging handlers (Console + File) unique to the calling process.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    log_filename = os.path.join(LOG_DIR, f"{process_identifier}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s - [%(processName)s (PID:%(process)d)] - %(name)s - %(levelname)s - %(message)s'
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_filename, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Disable verbose Numba internal logging
    logging.getLogger("numba").setLevel(logging.WARNING)


async def fetch_urls(url_fetcher, urls):
    """ Generates list of urls given a url fetcher """
    async for stream_id, url in url_fetcher.fetch_urls():
        urls[stream_id] = url


async def async_producer(queue, urls, data_folder):
    producers = [asyncio.create_task(
        task) for task in StreamProcesser.from_stream(urls, data_folder, queue)]

    await asyncio.gather(*producers)

    # send poison pills to safely shutdown consumer processes.
    for _ in range(3):
        queue.put(None)


def producer_worker(queue, urls, data_folder):
    setup_process_logging("producer")
    logging.info("Producer process initialized")
    asyncio.run(async_producer(queue, urls, data_folder))


def consumer_worker(queue, data_folder, segment_folder, worker_id):
    setup_process_logging(f"consumer_{worker_id}")
    logging.info(f"Consumer worker {worker_id} initialized")
    RuptureSegmenter.create_and_return_segmenter(
        data_folder, segment_folder, queue)


if __name__ == "__main__":
    setup_process_logging("main")
    logging.info("Starting main process orchestration")

    # keep a buffer of 20
    queue = Queue(maxsize=20)

    DATA_FOLDER = "streams"
    SEGMENT_FOLDER = "segments"

    make_folder(DATA_FOLDER)
    make_folder(SEGMENT_FOLDER)

    url_fetcher = RadioBrowserUrlFetcher()
    urls = {}
    asyncio.run(fetch_urls(url_fetcher, urls))
    for key, _ in urls.items():
        make_folder(f"{DATA_FOLDER}/radio_{key}")
        make_folder(f"{SEGMENT_FOLDER}/radio_{key}")

    producer_process = Process(
        target=producer_worker, args=(queue, urls, DATA_FOLDER))
    producer_process.start()

    # consumer_processes = []
    # for i in range(3):
    #     p = Process(target=consumer_worker,
    #                 name=f"ConsumerProcess-{i}", args=(queue, DATA_FOLDER, SEGMENT_FOLDER, i))
    #     consumer_processes.append(p)
    #     p.start()

    producer_process.join()
    # for p in consumer_processes:
    #     p.join()

    logging.warning("All processes joined and closed.")
