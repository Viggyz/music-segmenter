import asyncio

import logging
import sys
import os
from multiprocessing import Process, Queue

from utils.stream_processing import StreamProcesser
from utils.file import make_folder, make_folder_sync
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

async def async_producer(queue, data_folder, segment_folder):
    url_fetcher = RadioBrowserUrlFetcher()
    urls = {stream_id: url async for stream_id, url in url_fetcher.fetch_urls()}

    for key, _ in urls.items():
        await make_folder(f"{data_folder}/radio_{key}")
        await make_folder(f"{segment_folder}/radio_{key}")
    producers = [asyncio.create_task(
            task) for task in StreamProcesser.from_stream(urls, data_folder, queue)]

    await asyncio.gather(*producers)

    # send poison pills to safely shutdown consumer processes.
    for _ in range(3):
        queue.put(None)

def producer_worker(queue, data_folder, segment_folder):
    setup_process_logging("producer")
    logging.info("Producer process initialized")
    asyncio.run(async_producer(queue, data_folder, segment_folder))

def consumer_worker(queue, data_folder, segment_folder, worker_id):
    setup_process_logging(f"consumer_{worker_id}")
    logging.info(f"Consumer worker {worker_id} initialized")
    RuptureSegmenter.create_and_return_segmenter(data_folder, segment_folder, queue)

if __name__ == "__main__":
    setup_process_logging("main")
    logging.info("Starting main process orchestration")

    # keep a buffer of 20
    queue = Queue(maxsize=20)

    DATA_FOLDER = "streams"
    SEGMENT_FOLDER = "segments"

    make_folder_sync(DATA_FOLDER)
    make_folder_sync(SEGMENT_FOLDER)

    producer_process = Process(target=producer_worker, args=(queue, DATA_FOLDER, SEGMENT_FOLDER))
    producer_process.start()

    consumer_processes = []
    for i in range(3):
        p = Process(target=consumer_worker, name=f"ConsumerProcess-{i}", args=(queue, DATA_FOLDER, SEGMENT_FOLDER, i))
        consumer_processes.append(p)
        p.start()

    producer_process.join()
    for p in consumer_processes:
        p.join()

    logging.warning("All processes joined and closed.")