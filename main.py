import argparse
import asyncio
import datetime
import logging
import os
import sys
from multiprocessing import Process, Queue

from tortoise import Tortoise

from utils.aac_stream_processor import AACStreamProcessor
from utils.file import make_folder
from utils.radio_browser_url_fetcher import RadioBrowserUrlFetcher
from utils.rupture_segmenter import RuptureSegmenter
from utils.stream_processing import StreamProcesser

LOG_DIR = "logs"

parser = argparse.ArgumentParser(description="A script that accepts a command-line flag.")
parser.add_argument("-rdb", "--reset_db", action="store_true", help="Drop and recreate db")
args = parser.parse_args()


def setup_process_logging(process_identifier):
    """
    Configures logging handlers (Console + File) unique to the calling process.
    """
    os.makedirs(os.path.join(LOG_DIR, process_identifier), exist_ok=True)
    # log_filename = os.path.join(LOG_DIR, f"{process_identifier}.log")
    log_filename = datetime.datetime.now().strftime(f"{LOG_DIR}/{process_identifier}/run_%Y%m%d_%H%M%S.log")

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
    async for stream_id, url, codec in url_fetcher.fetch_urls():
        urls[stream_id] = (url, codec)


async def async_producer(queue, urls, data_folder):
    await setup_db_connection()

    producers = []
    for stream_id, (url, codec) in urls.items():
        if codec == 'MP3':
            processor = await StreamProcesser.from_stream(url, stream_id, data_folder, queue)
            producers.append(asyncio.create_task(processor.record_mp3_stream()))
        elif codec == 'AAC':
            processor = await AACStreamProcessor.from_stream(url, stream_id, data_folder, queue)
            producers.append(asyncio.create_task(processor.record_aac_stream()))

    await asyncio.gather(*producers)

    # send poison pills to safely shutdown consumer processes.
    for _ in range(3):
        queue.put(None)
        
    await Tortoise.close_connection()


def producer_worker(queue, urls, data_folder):
    setup_process_logging("producer")
    logging.info("Producer process initialized")
    asyncio.run(async_producer(queue, urls, data_folder))


def consumer_worker(queue, data_folder, segment_folder, worker_id):
    setup_process_logging(f"consumer_{worker_id}")
    logging.info(f"Consumer worker {worker_id} initialized")
    RuptureSegmenter.create_and_return_segmenter(
        data_folder, segment_folder, queue)

async def setup_db_connection():
    db_config = {
        'connections': {'default': 'sqlite://db.sqlite3'},
        'apps': {
            'models': {
                'models': ['models'], # Replace with your models module path
                'default_connection': 'default',
            }
        }
    }

    
    # 1. Initialize to establish the connection
    await Tortoise.init(config=db_config)
    if args.reset_db:
        logging.warning("Resetting and recreating db.")

        # 2. Drop all tables managed by this configuration
        await Tortoise._drop_databases()
        
        # 3. RE-INITIALIZE to recreate the connection pool
        await Tortoise.init(config=db_config)

        # 4. Re-create all tables from scratch
        # safe=False forces it to create tables even if they exist (or after dropping)
        await Tortoise.generate_schemas(safe=False)
    else:
        await Tortoise.generate_schemas()



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
